using Amazon.Runtime;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace FileUploaderService.Services;

public class CertificateAwsTokenService : IAwsTokenService
{
    private readonly AWSCredentials _credentials;

    public CertificateAwsTokenService(IConfiguration configuration, ILogger<CertificateAwsTokenService> logger)
    {
        // We create a custom credentials object that handles the refresh logic internally.
        // This allows the S3 client to be long-lived and automatically get new credentials when needed.
        _credentials = new CustomCertificateCredentials(configuration, logger);
    }

    public Task<AWSCredentials> GetCredentialsAsync()
    {
        return Task.FromResult(_credentials);
    }
}

public class CustomCertificateCredentials : AWSCredentials
{
    private readonly IConfiguration _configuration;
    private readonly ILogger _logger;
    private ImmutableCredentials? _cached;
    private DateTime _expiration;
    private readonly object _lock = new object();

    public CustomCertificateCredentials(IConfiguration configuration, ILogger logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public override ImmutableCredentials GetCredentials()
    {
        lock (_lock)
        {
            // Add a buffer time for expiration (e.g. 5 minutes before actual expiry)
            if (_cached == null || DateTime.UtcNow > _expiration.AddMinutes(-5))
            {
                Refresh();
            }
            return _cached!;
        }
    }

    private void Refresh()
    {
        _logger.LogInformation("Refreshing AWS credentials...");

        var certPath = _configuration["Aws:CertificatePath"];
        var refreshMinutes = _configuration.GetValue<int>("Aws:TokenRefreshMinutes", 15);

        // Simulation logic
        if (!string.IsNullOrEmpty(certPath) && File.Exists(certPath))
        {
             _logger.LogInformation("Using certificate at {Path} to obtain credentials.", certPath);
             // TODO: Real implementation would invoke Roles Anywhere helper or sign request here.
             // If using external process helper:
             // var processHelper = new ProcessAWSCredentials(path, args);
             // return processHelper.GetCredentials();
        }
        else
        {
             _logger.LogWarning("Certificate not found at {Path}. Using mock credentials.", certPath);
        }

        // Generate dummy credentials
        // In a real app, these would come from the Roles Anywhere service response.
        _cached = new ImmutableCredentials("ASIA_MOCK_ACCESS_KEY", "MOCK_SECRET_KEY", "MOCK_SESSION_TOKEN");

        // Set expiration
        _expiration = DateTime.UtcNow.AddMinutes(refreshMinutes);

        _logger.LogInformation("Credentials refreshed. Next refresh at {Expiration}", _expiration);
    }
}
