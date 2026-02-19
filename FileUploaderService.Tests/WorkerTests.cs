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
    public async Task Worker_Should_Process_Files_In_Numeric_Order_Handling_Gaps()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<Worker>>();
        var repoMock = new Mock<IFileRepository>();
        var uploaderMock = new Mock<IS3UploaderService>();

        var inMemorySettings = new Dictionary<string, string> {
            {"SourceFolder", _testDir},
            {"FileExtension", ".txt"},
            {"ConcurrencyLimit", "1"}, // Use 1 to verify order strictly
            {"ScanIntervalSeconds", "1"}
        };

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        // Setup Repo
        long maxId = 0; // State variable for mock

        repoMock.Setup(r => r.GetMaxProcessedIdAsync())
            .ReturnsAsync(() => maxId); // Use delegate to return current state

        repoMock.Setup(r => r.InitializeDatabaseAsync())
            .Returns(Task.CompletedTask);

        // When MarkFileProcessedAsync is called, update maxId if the file ID is higher.
        // But we need to parse it. The mock can just assume.
        repoMock.Setup(r => r.MarkFileProcessedAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<string>()))
            .Callback<string, long, string>((fileName, size, status) => {
                var namePart = Path.GetFileNameWithoutExtension(fileName);
                if (long.TryParse(namePart, out var id))
                {
                    if (id > maxId) maxId = id;
                }
            })
            .Returns(Task.CompletedTask);

        var uploadedFiles = new List<string>();
        uploaderMock.Setup(u => u.UploadFileAsync(It.IsAny<string>()))
            .Callback<string>(path => uploadedFiles.Add(Path.GetFileName(path)))
            .Returns(Task.CompletedTask);

        // Create files OUT OF ORDER on disk
        // IDs: 5, 2, 10
        await File.WriteAllTextAsync(Path.Combine(_testDir, "5.txt"), "content");
        await File.WriteAllTextAsync(Path.Combine(_testDir, "2.txt"), "content");
        await File.WriteAllTextAsync(Path.Combine(_testDir, "10.txt"), "content");

        using var worker = new Worker(loggerMock.Object, repoMock.Object, uploaderMock.Object, configuration);

        // Act
        var cts = new CancellationTokenSource();
        var workerTask = worker.StartAsync(cts.Token);

        // Wait for scan and process.
        // 1st scan: should find 2, 5, 10. Process 2. Update maxId=2.
        // 2nd scan: finds 5, 10. Process 5. Update maxId=5.
        // 3rd scan: finds 10. Process 10. Update maxId=10.
        // Wait sufficient time for 3 scans (interval is 1s, plus processing).
        await Task.Delay(4000);

        // Stop
        await worker.StopAsync(CancellationToken.None);

        // Assert
        // We expect at least 3 files. It shouldn't process them AGAIN because maxId updates.
        // Duplicate processing would only happen if maxId wasn't updated or file check failed.

        // Check order of FIRST 3 uploads.
        Assert.True(uploadedFiles.Count >= 3, $"Expected at least 3 uploads, got {uploadedFiles.Count}");
        Assert.Equal("2.txt", uploadedFiles[0]);
        Assert.Equal("5.txt", uploadedFiles[1]);
        Assert.Equal("10.txt", uploadedFiles[2]);

        // Ensure no duplicates were processed (count should be exactly 3)
        Assert.Equal(3, uploadedFiles.Count);
    }
}
