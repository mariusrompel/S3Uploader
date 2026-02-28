using Dapper;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FileUploaderService.Repositories;

public class FileRepository : IFileRepository
{
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly ILogger<FileRepository> _logger;

    public FileRepository(IConfiguration configuration, ILogger<FileRepository> logger)
    {
        _connectionString = configuration["MssqlConnectionString"]
                            ?? throw new ArgumentNullException("MssqlConnectionString not found in configuration");
        _tableName = configuration["SourceTableName"]
                     ?? throw new ArgumentNullException("SourceTableName not found in configuration");
        _logger = logger;
    }

    public async Task InitializeDatabaseAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var checkTableSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName";
        var exists = await connection.ExecuteScalarAsync<int>(checkTableSql, new { TableName = _tableName }) > 0;

        if (!exists)
        {
             throw new InvalidOperationException($"Table '{_tableName}' does not exist. Please create it and import filenames.");
        }

        var ensureColumnsSql = $@"
            IF COL_LENGTH('{_tableName}', 'Status') IS NULL
                ALTER TABLE {_tableName} ADD Status NVARCHAR(50);

            IF COL_LENGTH('{_tableName}', 'UploadedAt') IS NULL
                ALTER TABLE {_tableName} ADD UploadedAt DATETIME2;

            IF COL_LENGTH('{_tableName}', 'Size') IS NULL
                ALTER TABLE {_tableName} ADD Size BIGINT;

            IF COL_LENGTH('{_tableName}', 'ErrorMessage') IS NULL
                ALTER TABLE {_tableName} ADD ErrorMessage NVARCHAR(MAX);

            -- Important indexes to prevent scans and deadlocks
            -- Creating indexes on large tables takes time. We will use commandTimeout: 0 (infinite).
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{_tableName}_Status' AND object_id = OBJECT_ID('{_tableName}'))
                CREATE INDEX IX_{_tableName}_Status ON {_tableName}(Status);

            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{_tableName}_FileName' AND object_id = OBJECT_ID('{_tableName}'))
                CREATE INDEX IX_{_tableName}_FileName ON {_tableName}(FileName);
        ";

        // Execute schema changes and indexing with no timeout (0), as it might take minutes on a table with millions of rows.
        await connection.ExecuteAsync(ensureColumnsSql, commandTimeout: 0);
    }

    public async Task<IEnumerable<FileItem>> GetPendingFilesAsync(int batchSize)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Use WITH (READPAST) to skip locked rows
        var sql = $"SELECT TOP (@BatchSize) FileName FROM {_tableName} WITH (READPAST) WHERE Status IS NULL OR Status NOT IN ('Processed', 'Error')";

        var fileNames = await connection.QueryAsync<string>(sql, new { BatchSize = batchSize });

        return fileNames.Select(f => new FileItem(f, 0, 0));
    }

    public async Task MarkFileProcessedAsync(string fileName, long size, string status)
    {
        var sql = $"UPDATE {_tableName} WITH (ROWLOCK) SET Status = @Status, UploadedAt = GETUTCDATE(), Size = @Size, ErrorMessage = NULL WHERE FileName = @FileName";
        await ExecuteWithRetryAsync(sql, new { Status = status, Size = size, FileName = fileName });
    }

    public async Task MarkFileFailedAsync(string fileName, string errorMessage)
    {
        var sql = $"UPDATE {_tableName} WITH (ROWLOCK) SET Status = 'Error', ErrorMessage = @Error, UploadedAt = GETUTCDATE() WHERE FileName = @FileName";
        await ExecuteWithRetryAsync(sql, new { Error = errorMessage, FileName = fileName });
    }

    private async Task ExecuteWithRetryAsync(string sql, object parameters)
    {
        int retries = 3;
        while (retries > 0)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();
                await connection.ExecuteAsync(sql, parameters);
                return; // Success
            }
            catch (SqlException ex) when (ex.Number == 1205) // Deadlock victim
            {
                retries--;
                if (retries == 0)
                {
                    _logger.LogError(ex, "Deadlock encountered and retries exhausted executing SQL: {Sql}", sql);
                    throw;
                }

                _logger.LogWarning("Deadlock encountered (Error 1205). Retrying operation... ({Retries} remaining)", retries);
                // Simple backoff
                await Task.Delay(TimeSpan.FromMilliseconds(new Random().Next(100, 500)));
            }
            catch (Exception)
            {
                throw; // Other errors escalate immediately
            }
        }
    }
}
