using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace S3ObjectListVerifier.Repositories;

public class S3ObjectItem
{
    public string ObjectKey { get; set; } = string.Empty;
    public long Size { get; set; }
}

public class VerifierRepository
{
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly ILogger<VerifierRepository> _logger;

    public VerifierRepository(IConfiguration configuration, ILogger<VerifierRepository> logger)
    {
        _connectionString = configuration["MssqlConnectionString"]
                            ?? throw new ArgumentNullException("MssqlConnectionString not found in configuration");
        _tableName = configuration["DestinationTableName"]
                     ?? "S3ObjectList";
        _logger = logger;
    }

    public async Task InitializeDatabaseAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $@"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{_tableName}' AND xtype='U')
            BEGIN
                CREATE TABLE {_tableName} (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    ObjectKey NVARCHAR(1000) NOT NULL,
                    Size BIGINT,
                    DiscoveredAt DATETIME2 DEFAULT GETUTCDATE()
                );

                -- Index on ObjectKey for fast lookups/comparisons later
                CREATE INDEX IX_{_tableName}_ObjectKey ON {_tableName}(ObjectKey);
            END";

        await connection.ExecuteAsync(sql);
        _logger.LogInformation("Database initialized. Destination table: {TableName}", _tableName);
    }

    public async Task SaveBatchAsync(IEnumerable<S3ObjectItem> items)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = $"INSERT INTO {_tableName} (ObjectKey, Size) VALUES (@ObjectKey, @Size)";

        // Dapper automatically handles inserting enumerables as batches/multiple statements
        await connection.ExecuteAsync(sql, items);
    }
}
