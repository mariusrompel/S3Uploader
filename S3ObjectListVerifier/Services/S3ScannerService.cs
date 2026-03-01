using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using S3ObjectListVerifier.Repositories;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace S3ObjectListVerifier.Services;

public class S3ScannerService
{
    private readonly CertificateAwsTokenService _awsTokenService;
    private readonly VerifierRepository _repository;
    private readonly IConfiguration _configuration;
    private readonly ILogger<S3ScannerService> _logger;
    private readonly string _bucketName;
    private readonly string _prefix;

    public S3ScannerService(CertificateAwsTokenService awsTokenService, VerifierRepository repository, IConfiguration configuration, ILogger<S3ScannerService> logger)
    {
        _awsTokenService = awsTokenService;
        _repository = repository;
        _configuration = configuration;
        _logger = logger;

        _bucketName = configuration["Aws:BucketName"] ?? throw new ArgumentNullException("Aws:BucketName");
        _prefix = configuration["Aws:DestinationPrefix"] ?? "";

        if (!string.IsNullOrEmpty(_prefix))
        {
            _prefix = _prefix.TrimStart('/');
            if (!_prefix.EndsWith("/"))
            {
                _prefix += "/";
            }
        }
    }

    public async Task ScanAndSaveAsync()
    {
        _logger.LogInformation("Starting S3 scan for bucket: {BucketName}, prefix: {Prefix}", _bucketName, _prefix);

        var region = _configuration["Aws:Region"] ?? "us-east-1";
        var credentials = await _awsTokenService.GetCredentialsAsync();

        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region)
        };

        using var s3Client = new AmazonS3Client(credentials, config);

        await _repository.InitializeDatabaseAsync();

        var request = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = string.IsNullOrEmpty(_prefix) ? null : _prefix,
            MaxKeys = 1000 // Max allowed per API call
        };

        long totalObjects = 0;
        int batchCount = 0;

        try
        {
            ListObjectsV2Response response;
            do
            {
                response = await s3Client.ListObjectsV2Async(request);

                var items = new List<S3ObjectItem>();
                foreach (var obj in response.S3Objects)
                {
                    items.Add(new S3ObjectItem
                    {
                        ObjectKey = obj.Key,
                        Size = obj.Size ?? 0 // Handle nullable long
                    });
                }

                if (items.Count > 0)
                {
                    await _repository.SaveBatchAsync(items);
                    totalObjects += items.Count;
                    batchCount++;

                    _logger.LogInformation("Processed batch {BatchCount}. Total objects saved so far: {TotalObjects}", batchCount, totalObjects);
                }

                request.ContinuationToken = response.NextContinuationToken;

            } while (response.IsTruncated == true); // Handle nullable bool

            _logger.LogInformation("Scan completed successfully. Total objects discovered and saved: {TotalObjects}", totalObjects);
        }
        catch (AmazonS3Exception e)
        {
            _logger.LogError(e, "Error encountered on server when listing objects. Message:'{Message}'", e.Message);
            throw;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Unknown error encountered when listing objects. Message:'{Message}'", e.Message);
            throw;
        }
    }
}
