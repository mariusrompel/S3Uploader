using Amazon.S3;
using Amazon.S3.Transfer;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace FileUploaderService.Services;

public class S3UploaderService : IS3UploaderService, IDisposable
{
    private readonly AmazonS3Client _s3Client;
    private readonly TransferUtility _transferUtility;
    private readonly ILogger<S3UploaderService> _logger;
    private readonly string _bucketName;
    private readonly string _destinationPrefix;

    public S3UploaderService(IAwsTokenService awsTokenService, IConfiguration configuration, ILogger<S3UploaderService> logger)
    {
        _logger = logger;
        _bucketName = configuration["Aws:BucketName"] ?? throw new ArgumentNullException("Aws:BucketName");
        _destinationPrefix = configuration["Aws:DestinationPrefix"] ?? "";

        // Ensure prefix ends with '/' if not empty, and doesn't start with '/' for S3 key rules usually.
        // If user specified "/tenant1", make it "tenant1/"
        if (!string.IsNullOrEmpty(_destinationPrefix))
        {
            _destinationPrefix = _destinationPrefix.TrimStart('/');
            if (!_destinationPrefix.EndsWith("/"))
            {
                _destinationPrefix += "/";
            }
        }

        var region = configuration["Aws:Region"] ?? "us-east-1";

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
        var fileName = Path.GetFileName(filePath);
        var key = _destinationPrefix + fileName;

        _logger.LogInformation("Uploading {FilePath} to bucket {BucketName} with key {Key}...", filePath, _bucketName, key);

        try
        {
            // Use the key which includes the prefix.
            await _transferUtility.UploadAsync(filePath, _bucketName, key);
            _logger.LogInformation("Successfully uploaded {FilePath} as {Key}.", filePath, key);
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
