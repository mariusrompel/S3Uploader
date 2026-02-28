using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;
using System.IO;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FileUploaderService;

// 'FileName' here represents the full path as stored in the database.
public record FileItem(string FileName, long FileId, long Size);

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IFileRepository _fileRepository;
    private readonly IS3UploaderService _s3UploaderService;
    private readonly IConfiguration _configuration;
    private readonly int _concurrencyLimit;
    private readonly int _scanIntervalSeconds;

    public Worker(ILogger<Worker> logger, IFileRepository fileRepository, IS3UploaderService s3UploaderService, IConfiguration configuration)
    {
        _logger = logger;
        _fileRepository = fileRepository;
        _s3UploaderService = s3UploaderService;
        _configuration = configuration;

        _concurrencyLimit = _configuration.GetValue<int>("ConcurrencyLimit", 5);
        _scanIntervalSeconds = _configuration.GetValue<int>("ScanIntervalSeconds", 60);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started at: {time}", DateTimeOffset.Now);

        try
        {
            await _fileRepository.InitializeDatabaseAsync();
            _logger.LogInformation("Database initialized and verified.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize database (table missing?). Worker cannot proceed.");
            return;
        }

        var channel = Channel.CreateBounded<FileItem>(new BoundedChannelOptions(_concurrencyLimit * 2)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        var consumerTasks = new List<Task>();
        for (int i = 0; i < _concurrencyLimit; i++)
        {
            consumerTasks.Add(ConsumeFilesAsync(channel.Reader, stoppingToken));
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Fetching pending files from database...");

                try
                {
                    // Get a batch of pending files. Size = concurrency * 2 to keep pipeline full.
                    var pendingFiles = await _fileRepository.GetPendingFilesAsync(_concurrencyLimit * 2);

                    int queuedCount = 0;
                    foreach (var item in pendingFiles)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        // The 'FileName' from the database is expected to be the full file path.
                        var fullFilePath = item.FileName;

                        if (File.Exists(fullFilePath))
                        {
                            var fileInfo = new FileInfo(fullFilePath);
                            // Push the full path to the channel so the consumer can upload it.
                            await channel.Writer.WriteAsync(new FileItem(fullFilePath, 0, fileInfo.Length), stoppingToken);
                            queuedCount++;
                        }
                        else
                        {
                            _logger.LogWarning("File listed in DB but not found on disk: {Path}", fullFilePath);
                            await _fileRepository.MarkFileFailedAsync(fullFilePath, "File not found on disk");
                        }
                    }

                    if (queuedCount == 0)
                    {
                        _logger.LogInformation("No pending files found. Waiting...");
                        await Task.Delay(TimeSpan.FromSeconds(_scanIntervalSeconds), stoppingToken);
                    }
                    else
                    {
                         _logger.LogInformation("Queued {Count} files.", queuedCount);
                         // Yield slightly if needed, but bounded channel handles backpressure.
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error fetching pending files.");
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in producer loop.");
        }
        finally
        {
            channel.Writer.Complete();
            await Task.WhenAll(consumerTasks);
        }
    }

    private async Task ConsumeFilesAsync(ChannelReader<FileItem> reader, CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var item in reader.ReadAllAsync(stoppingToken))
            {
                var fullFilePath = item.FileName;

                try
                {
                    _logger.LogInformation("Processing file: {FilePath}", fullFilePath);

                    // Uploads the file to S3.
                    // Note: S3UploaderService extracts Path.GetFileName(fullFilePath) to construct the S3 Object Key.
                    await _s3UploaderService.UploadFileAsync(fullFilePath);

                    // Updates the database using the full path as the identifier.
                    await _fileRepository.MarkFileProcessedAsync(fullFilePath, item.Size, "Processed");

                    _logger.LogInformation("File processed and tracked: {FilePath}", fullFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process file: {FilePath}.", fullFilePath);
                    await _fileRepository.MarkFileFailedAsync(fullFilePath, ex.Message);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer task failed.");
        }
    }
}
