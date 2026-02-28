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
    public async Task Worker_Should_Process_Files_From_Database_Using_Full_Path()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<Worker>>();
        var repoMock = new Mock<IFileRepository>();
        var uploaderMock = new Mock<IS3UploaderService>();

        var inMemorySettings = new Dictionary<string, string> {
            {"ConcurrencyLimit", "1"},
            {"ScanIntervalSeconds", "1"}
        };

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        // Create full paths for testing
        var file1Path = Path.Combine(_testDir, "1.txt");
        var file2Path = Path.Combine(_testDir, "2.txt");

        // We simulate a database queue returning full paths
        var pendingFiles = new Queue<FileItem>();
        pendingFiles.Enqueue(new FileItem(file1Path, 0, 0));
        pendingFiles.Enqueue(new FileItem(file2Path, 0, 0));

        repoMock.Setup(r => r.GetPendingFilesAsync(It.IsAny<int>()))
            .ReturnsAsync((int batch) => {
                var batchItems = new List<FileItem>();
                while(batchItems.Count < batch && pendingFiles.Count > 0)
                {
                    batchItems.Add(pendingFiles.Dequeue());
                }
                return batchItems;
            });

        repoMock.Setup(r => r.InitializeDatabaseAsync())
            .Returns(Task.CompletedTask);

        repoMock.Setup(r => r.MarkFileProcessedAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        var uploadedFiles = new List<string>();
        uploaderMock.Setup(u => u.UploadFileAsync(It.IsAny<string>()))
            .Callback<string>(path => uploadedFiles.Add(path))
            .Returns(Task.CompletedTask);

        // Create files on disk using the full paths
        await File.WriteAllTextAsync(file1Path, "content");
        await File.WriteAllTextAsync(file2Path, "content");

        using var worker = new Worker(loggerMock.Object, repoMock.Object, uploaderMock.Object, configuration);

        // Act
        var cts = new CancellationTokenSource();
        // Start the worker (don't await indefinitely, but let it start)
        var workerTask = worker.StartAsync(cts.Token);

        // Wait for processing
        await Task.Delay(3000);

        // Stop
        await worker.StopAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, uploadedFiles.Count);

        // Ensure the full path was passed to the S3 uploader
        Assert.Contains(file1Path, uploadedFiles);
        Assert.Contains(file2Path, uploadedFiles);

        // Verify DB updates use the full path
        repoMock.Verify(r => r.MarkFileProcessedAsync(file1Path, It.IsAny<long>(), "Processed"), Times.Once);
        repoMock.Verify(r => r.MarkFileProcessedAsync(file2Path, It.IsAny<long>(), "Processed"), Times.Once);
    }
}
