using Amazon.S3;
using Amazon.S3.Model;
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
    private readonly long _multipartThreshold = 5 * 1024 * 1024; // 5 MB threshold for simple PutObject

    public S3UploaderService(IAwsTokenService awsTokenService, IConfiguration configuration, ILogger<S3UploaderService> logger)
    {
        _logger = logger;
        _bucketName = configuration["Aws:BucketName"] ?? throw new ArgumentNullException("Aws:BucketName");
        _destinationPrefix = configuration["Aws:DestinationPrefix"] ?? "";

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
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region),
            MaxErrorRetry = 3,
            // Increase concurrent connections to handle high concurrency (e.g. 50 threads)
            // .NET Core usually handles this automatically, but explicit setting can help.
            // Timeout settings can also be tweaked if needed.
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
        var fileInfo = new FileInfo(filePath);

        _logger.LogInformation("Uploading {FilePath} to bucket {BucketName} with key {Key} (Size: {Size})...", filePath, _bucketName, key, fileInfo.Length);

        try
        {
            // Optimization: Use PutObjectAsync for small files to avoid TransferUtility overhead (multipart checks, etc.)
            // The user mentioned GSM files which are small.
            if (fileInfo.Length < _multipartThreshold)
            {
                var putRequest = new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = filePath,
                    DisablePayloadSigning = false // Keep default security
                };

                await _s3Client.PutObjectAsync(putRequest);
            }
            else
            {
                // Use TransferUtility for larger files (it handles multipart automatically)
                await _transferUtility.UploadAsync(filePath, _bucketName, key);
            }

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
