using Dapper;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;

namespace FileUploaderService.Repositories;

public class FileRepository : IFileRepository
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public FileRepository(IConfiguration configuration)
    {
        _connectionString = configuration["MssqlConnectionString"]
                            ?? throw new ArgumentNullException("MssqlConnectionString not found in configuration");
        _tableName = configuration["SourceTableName"]
                     ?? throw new ArgumentNullException("SourceTableName not found in configuration");
    }

    public async Task InitializeDatabaseAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Check if table exists. We assume it does since it's "imported".
        // We do a safe dynamic SQL call. Note: Table name from config is considered trusted.
        var checkTableSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName";
        var exists = await connection.ExecuteScalarAsync<int>(checkTableSql, new { TableName = _tableName }) > 0;

        if (!exists)
        {
             // If user doesn't have it, we throw.
             // "Import" implies existing data.
             throw new InvalidOperationException($"Table '{_tableName}' does not exist. Please create it and import filenames.");
        }

        // Add columns if missing
        // Using COL_LENGTH() function is standard for this check.
        var ensureColumnsSql = $@"
            IF COL_LENGTH('{_tableName}', 'Status') IS NULL
                ALTER TABLE {_tableName} ADD Status NVARCHAR(50);

            IF COL_LENGTH('{_tableName}', 'UploadedAt') IS NULL
                ALTER TABLE {_tableName} ADD UploadedAt DATETIME2;

            IF COL_LENGTH('{_tableName}', 'Size') IS NULL
                ALTER TABLE {_tableName} ADD Size BIGINT;

            IF COL_LENGTH('{_tableName}', 'ErrorMessage') IS NULL
                ALTER TABLE {_tableName} ADD ErrorMessage NVARCHAR(MAX);
        ";

        await connection.ExecuteAsync(ensureColumnsSql);
    }

    public async Task<IEnumerable<FileItem>> GetPendingFilesAsync(int batchSize)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Select pending files.
        // We select FileName. Assuming FileName is unique per row or we just pick rows.
        var sql = $"SELECT TOP (@BatchSize) FileName FROM {_tableName} WHERE Status IS NULL OR Status NOT IN ('Processed', 'Error')";

        var fileNames = await connection.QueryAsync<string>(sql, new { BatchSize = batchSize });

        // Return dummy FileItems. Path will be resolved by worker.
        return fileNames.Select(f => new FileItem(f, 0, 0));
    }

    public async Task MarkFileProcessedAsync(string fileName, long size, string status)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $"UPDATE {_tableName} SET Status = @Status, UploadedAt = GETUTCDATE(), Size = @Size, ErrorMessage = NULL WHERE FileName = @FileName";
        await connection.ExecuteAsync(sql, new { Status = status, Size = size, FileName = fileName });
    }

    public async Task MarkFileFailedAsync(string fileName, string errorMessage)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $"UPDATE {_tableName} SET Status = 'Error', ErrorMessage = @Error, UploadedAt = GETUTCDATE() WHERE FileName = @FileName";
        await connection.ExecuteAsync(sql, new { Error = errorMessage, FileName = fileName });
    }
}
