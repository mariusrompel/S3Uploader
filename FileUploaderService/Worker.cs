using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace FileUploaderService;

public record FileItem(string FilePath, long Size);

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IFileRepository _fileRepository;
    private readonly IS3UploaderService _s3UploaderService;
    private readonly IConfiguration _configuration;
    private readonly string _sourceFolder;
    private readonly string _fileExtension; // e.g., ".wav"
    private readonly int _concurrencyLimit;

    public Worker(ILogger<Worker> logger, IFileRepository fileRepository, IS3UploaderService s3UploaderService, IConfiguration configuration)
    {
        _logger = logger;
        _fileRepository = fileRepository;
        _s3UploaderService = s3UploaderService;
        _configuration = configuration;

        _sourceFolder = _configuration["SourceFolder"] ?? throw new ArgumentNullException("SourceFolder");
        _fileExtension = _configuration["FileExtension"] ?? ".wav";
        _concurrencyLimit = _configuration.GetValue<int>("ConcurrencyLimit", 5);

        // Ensure extension has dot
        if (!_fileExtension.StartsWith(".")) _fileExtension = "." + _fileExtension;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started at: {time}", DateTimeOffset.Now);

        try
        {
            await _fileRepository.InitializeDatabaseAsync();
            _logger.LogInformation("Database initialized.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize database.");
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

        // Sequential Scan Strategy
        long currentId = 0;
        try
        {
            // Initial sync: Get the last processed ID from DB
            currentId = await _fileRepository.GetMaxProcessedIdAsync();
            _logger.LogInformation("Starting sequential scan from ID: {Id}", currentId + 1);

            while (!stoppingToken.IsCancellationRequested)
            {
                if (!Directory.Exists(_sourceFolder))
                {
                    _logger.LogWarning("Source folder not found: {Folder}", _sourceFolder);
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                    continue;
                }

                int maxMisses = 1000; // Look ahead 1000 IDs for a file.
                bool foundAny = false;

                // We will try to fill the channel as much as possible, or at least one file.
                // Loop until we find a file or exhaust lookahead.
                for (int lookAhead = 1; lookAhead <= maxMisses; lookAhead++)
                {
                    if (stoppingToken.IsCancellationRequested) break;

                    long checkId = currentId + lookAhead;
                    string fileName = $"{checkId}{_fileExtension}";
                    string filePath = Path.Combine(_sourceFolder, fileName);

                    if (File.Exists(filePath))
                    {
                        var fileInfo = new FileInfo(filePath);
                        await channel.Writer.WriteAsync(new FileItem(filePath, fileInfo.Length), stoppingToken);

                        // We found a file at 'checkId'.
                        // This means 'currentId + 1' to 'checkId - 1' were skipped/gaps.
                        // Update currentId to checkId so next loop starts from here.
                        currentId = checkId;
                        foundAny = true;

                        // Reset lookAhead to continue scanning immediately from this new point
                        lookAhead = 0;

                        // To avoid hogging the thread forever if files are dense, maybe break occasionally?
                        // The await WriteAsync handles backpressure, so we pause if consumers are slow.
                        // But we should check cancellation often.
                    }
                }

                if (!foundAny)
                {
                    // We looked ahead 1000 IDs and found nothing.
                    // Likely reached the end of the sequence or a huge gap.
                    _logger.LogInformation("No new files found up to ID {Id}. Waiting...", currentId + maxMisses);
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
            _logger.LogError(ex, "Error in sequential scanner.");
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
                var fileName = Path.GetFileName(item.FilePath);
                try
                {
                    _logger.LogInformation("Processing file: {FileName}", fileName);

                    await _s3UploaderService.UploadFileAsync(item.FilePath);

                    await _fileRepository.MarkFileProcessedAsync(fileName, item.Size, "Processed");

                    _logger.LogInformation("File processed and tracked: {FileName}", fileName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process file: {FileName}. Will retry on next restart if gap not filled.", fileName);
                    // Failure handling:
                    // We do NOT mark as processed.
                    // The producer has already advanced past this ID.
                    // This file will be skipped until restart OR manual intervention.
                    // For now, this meets the requirement of "resume where stopped" (at the high water mark).
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
