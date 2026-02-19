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
    private readonly ILogger<CertificateAwsTokenService> _logger;

    public CertificateAwsTokenService(IConfiguration configuration, ILogger<CertificateAwsTokenService> logger)
    {
        _logger = logger;

        var certPath = configuration["Aws:CertificatePath"];
        var keyPath = configuration["Aws:PrivateKeyPath"];
        var trustAnchorArn = configuration["Aws:TrustAnchorArn"];
        var profileArn = configuration["Aws:ProfileArn"];
        var roleArn = configuration["Aws:RoleArn"];
        var region = configuration["Aws:Region"] ?? "us-east-1";
        var helperPath = configuration["Aws:SigningHelperPath"] ?? "aws_signing_helper";

        if (string.IsNullOrEmpty(certPath) || string.IsNullOrEmpty(keyPath) ||
            string.IsNullOrEmpty(trustAnchorArn) || string.IsNullOrEmpty(profileArn) ||
            string.IsNullOrEmpty(roleArn))
        {
            var missing = new List<string>();
            if (string.IsNullOrEmpty(certPath)) missing.Add("CertificatePath");
            if (string.IsNullOrEmpty(keyPath)) missing.Add("PrivateKeyPath");
            if (string.IsNullOrEmpty(trustAnchorArn)) missing.Add("TrustAnchorArn");
            if (string.IsNullOrEmpty(profileArn)) missing.Add("ProfileArn");
            if (string.IsNullOrEmpty(roleArn)) missing.Add("RoleArn");

            var msg = $"Missing required AWS Roles Anywhere configuration: {string.Join(", ", missing)}";
            _logger.LogError(msg);
            throw new ArgumentException(msg);
        }

        if (!File.Exists(certPath))
        {
             _logger.LogError("Certificate file not found at {Path}", certPath);
             throw new FileNotFoundException("Certificate file not found", certPath);
        }

        if (!File.Exists(keyPath))
        {
             _logger.LogError("Private key file not found at {Path}", keyPath);
             throw new FileNotFoundException("Private key file not found", keyPath);
        }

        _logger.LogInformation("Configuring AWS credentials using {Helper} with certificate: {CertPath}", helperPath, certPath);

        // ProcessAWSCredentials constructor takes a single string which is the full command line to execute.
        var command = $"{helperPath} credential-process --certificate \"{certPath}\" --private-key \"{keyPath}\" " +
                      $"--trust-anchor-arn {trustAnchorArn} --profile-arn {profileArn} --role-arn {roleArn}";

        _credentials = new ProcessAWSCredentials(command);
    }

    public Task<AWSCredentials> GetCredentialsAsync()
    {
        return Task.FromResult(_credentials);
    }
}
