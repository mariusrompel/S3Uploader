using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace FileUploaderService;

public record FileItem(string FileName, long FileId, long Size);

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IFileRepository _fileRepository;
    private readonly IS3UploaderService _s3UploaderService;
    private readonly IConfiguration _configuration;
    private readonly string _sourceFolder;
    private readonly string _fileExtension; // e.g., ".wav"
    private readonly int _concurrencyLimit;
    private readonly int _scanIntervalSeconds;

    public Worker(ILogger<Worker> logger, IFileRepository fileRepository, IS3UploaderService s3UploaderService, IConfiguration configuration)
    {
        _logger = logger;
        _fileRepository = fileRepository;
        _s3UploaderService = s3UploaderService;
        _configuration = configuration;

        _sourceFolder = _configuration["SourceFolder"] ?? throw new ArgumentNullException("SourceFolder");
        _fileExtension = _configuration["FileExtension"] ?? ".wav";
        _concurrencyLimit = _configuration.GetValue<int>("ConcurrencyLimit", 5);
        _scanIntervalSeconds = _configuration.GetValue<int>("ScanIntervalSeconds", 60);

        // Ensure extension has dot
        if (!_fileExtension.StartsWith(".")) _fileExtension = "." + _fileExtension;
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
            // We stop the service if we can't find the table.
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
                if (!Directory.Exists(_sourceFolder))
                {
                    _logger.LogWarning("Source folder not found: {Folder}", _sourceFolder);
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                    continue;
                }

                _logger.LogInformation("Fetching pending files from database...");

                try
                {
                    // Get a batch of pending files. Size = concurrency * 2 to keep pipeline full.
                    var pendingFiles = await _fileRepository.GetPendingFilesAsync(_concurrencyLimit * 2);

                    int queuedCount = 0;
                    foreach (var item in pendingFiles)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        var fileName = item.FileName;
                        var filePath = Path.Combine(_sourceFolder, fileName);

                        if (File.Exists(filePath))
                        {
                            var fileInfo = new FileInfo(filePath);
                            // We push to channel.
                            // Note: FileItem record structure changed slightly in previous steps but logic holds.
                            // We reconstruct it with full info.
                            await channel.Writer.WriteAsync(new FileItem(fileName, 0, fileInfo.Length), stoppingToken);
                            queuedCount++;
                        }
                        else
                        {
                            _logger.LogWarning("File listed in DB but not found on disk: {Path}", filePath);
                            await _fileRepository.MarkFileFailedAsync(fileName, "File not found on disk");
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
                         // If we found files, we loop immediately (or short delay) to keep processing unless queue is full.
                         // The channel write blocks if full, so we naturally throttle.
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
                var fileName = item.FileName;
                var filePath = Path.Combine(_sourceFolder, fileName);

                try
                {
                    _logger.LogInformation("Processing file: {FileName}", fileName);

                    await _s3UploaderService.UploadFileAsync(filePath);

                    await _fileRepository.MarkFileProcessedAsync(fileName, item.Size, "Processed");

                    _logger.LogInformation("File processed and tracked: {FileName}", fileName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process file: {FileName}.", fileName);
                    await _fileRepository.MarkFileFailedAsync(fileName, ex.Message);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer task failed.");
        }
    }
}
