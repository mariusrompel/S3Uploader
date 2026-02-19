namespace FileUploaderService.Interfaces;

public interface IS3UploaderService
{
    Task UploadFileAsync(string filePath);
}
