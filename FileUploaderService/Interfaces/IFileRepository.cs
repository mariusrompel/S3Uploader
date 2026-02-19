namespace FileUploaderService.Interfaces;

public interface IFileRepository
{
    Task InitializeDatabaseAsync();
    Task<bool> IsFileProcessedAsync(string fileName);
    Task<IEnumerable<string>> GetProcessedFileNamesAsync(IEnumerable<string> fileNames);
    Task MarkFileProcessedAsync(string fileName, long size, string status);

    // New method to get the maximum processed ID for sequential scanning
    Task<long> GetMaxProcessedIdAsync();
}
