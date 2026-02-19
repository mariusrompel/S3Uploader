using Dapper;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;

namespace FileUploaderService.Repositories;

public class FileRepository : IFileRepository
{
    private readonly string _connectionString;

    public FileRepository(IConfiguration configuration)
    {
        _connectionString = configuration["MssqlConnectionString"]
                            ?? throw new ArgumentNullException("MssqlConnectionString not found in configuration");
    }

    public async Task InitializeDatabaseAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = @"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FileTracking' AND xtype='U')
            BEGIN
                CREATE TABLE FileTracking (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    FileName NVARCHAR(450) NOT NULL,
                    UploadedAt DATETIME2 DEFAULT GETUTCDATE(),
                    Size BIGINT,
                    Status NVARCHAR(50)
                );
                CREATE INDEX IX_FileTracking_FileName ON FileTracking(FileName);
            END";

        await connection.ExecuteAsync(sql);
    }

    public async Task<bool> IsFileProcessedAsync(string fileName)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var sql = "SELECT COUNT(1) FROM FileTracking WHERE FileName = @FileName AND Status = 'Processed'";
        var count = await connection.ExecuteScalarAsync<int>(sql, new { FileName = fileName });
        return count > 0;
    }

    public async Task<IEnumerable<string>> GetProcessedFileNamesAsync(IEnumerable<string> fileNames)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        // Dapper handles IEnumerable parameter for IN clause automatically.
        var sql = "SELECT FileName FROM FileTracking WHERE FileName IN @FileNames AND Status = 'Processed'";
        return await connection.QueryAsync<string>(sql, new { FileNames = fileNames });
    }

    public async Task MarkFileProcessedAsync(string fileName, long size, string status)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var sql = "INSERT INTO FileTracking (FileName, Size, Status, UploadedAt) VALUES (@FileName, @Size, @Status, GETUTCDATE())";
        await connection.ExecuteAsync(sql, new { FileName = fileName, Size = size, Status = status });
    }
}
