using FileUploaderService.Interfaces;
using FileUploaderService.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace FileUploaderService.Tests;

public class S3UploaderServiceTests
{
    [Fact]
    public async Task UploadFileAsync_Should_Call_GetCredentials()
    {
        // Arrange
        var tokenServiceMock = new Mock<IAwsTokenService>();
        tokenServiceMock.Setup(t => t.GetCredentialsAsync())
            .ReturnsAsync(new Amazon.Runtime.SessionAWSCredentials("AKIA_TEST", "Secret_Test", "Token_Test"));

        var loggerMock = new Mock<ILogger<S3UploaderService>>();

        var inMemorySettings = new Dictionary<string, string> {
            {"Aws:BucketName", "test-bucket"},
            {"Aws:Region", "us-east-1"},
        };
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        var service = new S3UploaderService(tokenServiceMock.Object, configuration, loggerMock.Object);

        var tempFile = Path.GetTempFileName();

        try
        {
            // Act & Assert
            // We expect an exception because the S3 client will fail to connect (network/auth)
            // or TransferUtility will check file/bucket accessibility.
            // But we verify that before failing, it asked for credentials.
            await Assert.ThrowsAnyAsync<Exception>(async () => await service.UploadFileAsync(tempFile));

            // Verify GetCredentials was called
            tokenServiceMock.Verify(t => t.GetCredentialsAsync(), Times.Once);
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }
}
