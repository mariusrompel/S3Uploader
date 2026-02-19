using FileUploaderService;

namespace FileUploaderService.Interfaces;

public interface IFileRepository
{
    Task InitializeDatabaseAsync();
    Task<IEnumerable<FileItem>> GetPendingFilesAsync(int batchSize);
    Task MarkFileProcessedAsync(string fileName, long size, string status);
    Task MarkFileFailedAsync(string fileName, string errorMessage);
}
