using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using S3ObjectListVerifier.Repositories;
using System;
using System.Collections.Generic;
using System.Net;
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

        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region),
            MaxErrorRetry = 5 // Increase retries for long running process
        };

        // We retrieve the credential provider object. The AWS SDK uses this object to generate
        // fresh signatures for each request. ProcessAWSCredentials handles executing the helper
        // tool when the cached token expires.
        var credentials = await _awsTokenService.GetCredentialsAsync();
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
            ListObjectsV2Response response = null!;
            do
            {
                bool success = false;
                int retryCount = 0;

                // Explicit retry loop for the specific batch in case of token expiration race conditions
                // or transient network errors that the internal SDK retry doesn't catch.
                while (!success && retryCount < 3)
                {
                    try
                    {
                        response = await s3Client.ListObjectsV2Async(request);
                        success = true;
                    }
                    catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.Forbidden || ex.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        retryCount++;
                        _logger.LogWarning("Authentication error (Token Expired?) during batch {BatchCount}. Retrying {RetryCount}/3... Error: {Message}", batchCount + 1, retryCount, ex.Message);
                        // The next call will force the S3 client to ask `ProcessAWSCredentials` for credentials again,
                        // which should trigger a refresh if expired.
                        await Task.Delay(2000 * retryCount);
                        if (retryCount >= 3) throw;
                    }
                    catch (AmazonS3Exception ex) when ((int)ex.StatusCode >= 500)
                    {
                        retryCount++;
                        _logger.LogWarning("Transient server error during batch {BatchCount}. Retrying {RetryCount}/3... Error: {Message}", batchCount + 1, retryCount, ex.Message);
                        await Task.Delay(5000 * retryCount);
                        if (retryCount >= 3) throw;
                    }
                }

                var items = new List<S3ObjectItem>();
                foreach (var obj in response.S3Objects)
                {
                    items.Add(new S3ObjectItem
                    {
                        ObjectKey = obj.Key,
                        Size = obj.Size ?? 0
                    });
                }

                if (items.Count > 0)
                {
                    await _repository.SaveBatchAsync(items);
                    totalObjects += items.Count;
                    batchCount++;

                    if (batchCount % 10 == 0) // Log every 10,000 objects to avoid console spam for 30 million
                    {
                        _logger.LogInformation("Processed batch {BatchCount}. Total objects saved so far: {TotalObjects}", batchCount, totalObjects);
                    }
                }

                request.ContinuationToken = response.NextContinuationToken;

            } while (response.IsTruncated == true);

            _logger.LogInformation("Scan completed successfully. Total objects discovered and saved: {TotalObjects}", totalObjects);
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Fatal error encountered during S3 scan. Scan aborted after {TotalObjects} objects.", totalObjects);
            throw;
        }
    }
}
