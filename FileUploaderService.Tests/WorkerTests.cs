using FileUploaderService;
using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace FileUploaderService.Tests;

public class WorkerTests : IDisposable
{
    private readonly string _testDir;

    public WorkerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    [Fact]
    public async Task Worker_Should_Process_New_Files_Sequentially()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<Worker>>();
        var repoMock = new Mock<IFileRepository>();
        var uploaderMock = new Mock<IS3UploaderService>();

        var inMemorySettings = new Dictionary<string, string> {
            {"SourceFolder", _testDir},
            {"FileExtension", ".txt"}, // using .txt for test, but sequential check expects ID
            {"ConcurrencyLimit", "2"},
        };

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        // Setup Repo
        // Starting at ID 0, so next check is 1.txt
        repoMock.Setup(r => r.GetMaxProcessedIdAsync())
            .ReturnsAsync(0);

        repoMock.Setup(r => r.InitializeDatabaseAsync())
            .Returns(Task.CompletedTask);

        repoMock.Setup(r => r.MarkFileProcessedAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        uploaderMock.Setup(u => u.UploadFileAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        // Create sequential file: 1.txt
        var testFilePath = Path.Combine(_testDir, "1.txt");
        await File.WriteAllTextAsync(testFilePath, "content");

        // Create 3.txt (Gap!)
        var testFilePath3 = Path.Combine(_testDir, "3.txt");
        await File.WriteAllTextAsync(testFilePath3, "content");

        using var worker = new Worker(loggerMock.Object, repoMock.Object, uploaderMock.Object, configuration);

        // Act
        var cts = new CancellationTokenSource();
        // Start the worker
        await worker.StartAsync(cts.Token);

        // Wait a bit for scan and process to complete
        await Task.Delay(3000);

        // Stop
        await worker.StopAsync(CancellationToken.None);

        // Assert
        // Verify Upload was called for 1.txt
        uploaderMock.Verify(u => u.UploadFileAsync(testFilePath), Times.AtLeastOnce);

        // Verify Upload was called for 3.txt (should find it after skipping 2)
        uploaderMock.Verify(u => u.UploadFileAsync(testFilePath3), Times.AtLeastOnce);

        // Verify DB update for the file name 1.txt
        repoMock.Verify(r => r.MarkFileProcessedAsync("1.txt", It.IsAny<long>(), "Processed"), Times.AtLeastOnce);

        // Verify DB update for 3.txt
        repoMock.Verify(r => r.MarkFileProcessedAsync("3.txt", It.IsAny<long>(), "Processed"), Times.AtLeastOnce);
    }
}
