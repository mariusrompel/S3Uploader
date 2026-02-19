using Amazon.S3;
using Amazon.S3.Transfer;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace FileUploaderService.Services;

public class S3UploaderService : IS3UploaderService, IDisposable
{
    private readonly AmazonS3Client _s3Client;
    private readonly TransferUtility _transferUtility;
    private readonly ILogger<S3UploaderService> _logger;
    private readonly string _bucketName;

    public S3UploaderService(IAwsTokenService awsTokenService, IConfiguration configuration, ILogger<S3UploaderService> logger)
    {
        _logger = logger;
        _bucketName = configuration["Aws:BucketName"] ?? throw new ArgumentNullException("Aws:BucketName");
        var region = configuration["Aws:Region"] ?? "us-east-1";

        // Get the credentials provider object.
        // This object handles credential rotation internally.
        // We use .Result here because in our implementation it returns the provider object immediately (synchronously).
        // If the implementation were truly async, we would need to rethink injection or use async factory.
        var credentials = awsTokenService.GetCredentialsAsync().GetAwaiter().GetResult();

        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region)
        };

        _s3Client = new AmazonS3Client(credentials, config);
        _transferUtility = new TransferUtility(_s3Client);
    }

    public void Dispose()
    {
        _transferUtility?.Dispose();
        _s3Client?.Dispose();
    }

    public async Task UploadFileAsync(string filePath)
    {
        _logger.LogInformation("Uploading {FilePath} to bucket {BucketName}...", filePath, _bucketName);

        try
        {
            await _transferUtility.UploadAsync(filePath, _bucketName);
            _logger.LogInformation("Successfully uploaded {FilePath}.", filePath);
        }
        catch (AmazonS3Exception e)
        {
            _logger.LogError(e, "AWS S3 Error: {Message}", e.Message);
            throw;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error uploading file: {Message}", e.Message);
            throw;
        }
    }
}
