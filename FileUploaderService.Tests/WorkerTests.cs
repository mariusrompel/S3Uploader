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
    public async Task Worker_Should_Process_Files_From_Database_Queue()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<Worker>>();
        var repoMock = new Mock<IFileRepository>();
        var uploaderMock = new Mock<IS3UploaderService>();

        var inMemorySettings = new Dictionary<string, string> {
            {"SourceFolder", _testDir},
            {"FileExtension", ".txt"},
            {"ConcurrencyLimit", "1"},
            {"ScanIntervalSeconds", "1"}
        };

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        // Setup Repo
        // We simulate a database queue: 1.txt, 2.txt
        // 1st call returns [1.txt, 2.txt]
        // 2nd call returns empty (assuming processed)

        var pendingFiles = new Queue<FileItem>();
        pendingFiles.Enqueue(new FileItem("1.txt", 0, 0));
        pendingFiles.Enqueue(new FileItem("2.txt", 0, 0));

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
            .Callback<string>(path => uploadedFiles.Add(Path.GetFileName(path)))
            .Returns(Task.CompletedTask);

        // Create files on disk
        await File.WriteAllTextAsync(Path.Combine(_testDir, "1.txt"), "content");
        await File.WriteAllTextAsync(Path.Combine(_testDir, "2.txt"), "content");

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
        Assert.Contains("1.txt", uploadedFiles);
        Assert.Contains("2.txt", uploadedFiles);

        // Verify DB updates
        repoMock.Verify(r => r.MarkFileProcessedAsync("1.txt", It.IsAny<long>(), "Processed"), Times.Once);
        repoMock.Verify(r => r.MarkFileProcessedAsync("2.txt", It.IsAny<long>(), "Processed"), Times.Once);
    }
}
