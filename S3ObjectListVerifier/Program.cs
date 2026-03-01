using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using S3ObjectListVerifier.Repositories;
using S3ObjectListVerifier.Services;
using System;
using System.Threading.Tasks;

namespace S3ObjectListVerifier;

class Program
{
    static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        // Register Services
        builder.Services.AddSingleton<VerifierRepository>();
        builder.Services.AddSingleton<CertificateAwsTokenService>();
        builder.Services.AddSingleton<S3ScannerService>();

        var host = builder.Build();

        var logger = host.Services.GetRequiredService<ILogger<Program>>();
        var scannerService = host.Services.GetRequiredService<S3ScannerService>();

        logger.LogInformation("S3 Object List Verifier Tool Starting...");

        try
        {
            await scannerService.ScanAndSaveAsync();
            logger.LogInformation("S3 Object List Verifier Tool Finished.");
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "A critical error occurred while scanning S3.");
        }

        // Wait briefly for logs to flush before exiting
        await Task.Delay(500);
    }
}
