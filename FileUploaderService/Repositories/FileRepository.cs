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

        // Updated schema to include FileId derived from filename for better indexing/performance
        var sql = @"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FileTracking' AND xtype='U')
            BEGIN
                CREATE TABLE FileTracking (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    FileName NVARCHAR(450) NOT NULL,
                    FileId BIGINT, -- Parsed from filename (e.g. 100.wav -> 100)
                    UploadedAt DATETIME2 DEFAULT GETUTCDATE(),
                    Size BIGINT,
                    Status NVARCHAR(50)
                );
                CREATE INDEX IX_FileTracking_FileName ON FileTracking(FileName);
                CREATE INDEX IX_FileTracking_FileId ON FileTracking(FileId);
            END
            ELSE
            BEGIN
                -- Migration logic: Add FileId column if it doesn't exist
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'FileId' AND Object_ID = Object_ID(N'FileTracking'))
                BEGIN
                    ALTER TABLE FileTracking ADD FileId BIGINT;
                    CREATE INDEX IX_FileTracking_FileId ON FileTracking(FileId);
                END
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
        var sql = "SELECT FileName FROM FileTracking WHERE FileName IN @FileNames AND Status = 'Processed'";
        return await connection.QueryAsync<string>(sql, new { FileNames = fileNames });
    }

    public async Task MarkFileProcessedAsync(string fileName, long size, string status)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Parse FileId from filename
        long? fileId = null;
        var namePart = Path.GetFileNameWithoutExtension(fileName);
        if (long.TryParse(namePart, out var parsedId))
        {
            fileId = parsedId;
        }

        var sql = "INSERT INTO FileTracking (FileName, FileId, Size, Status, UploadedAt) VALUES (@FileName, @FileId, @Size, @Status, GETUTCDATE())";
        await connection.ExecuteAsync(sql, new { FileName = fileName, FileId = fileId, Size = size, Status = status });
    }

    public async Task<long> GetMaxProcessedIdAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        // Get the maximum processed FileId.
        // We assume strictly numeric filenames stored in FileId column.
        var sql = "SELECT ISNULL(MAX(FileId), 0) FROM FileTracking WHERE Status = 'Processed'";
        return await connection.ExecuteScalarAsync<long>(sql);
    }
}
