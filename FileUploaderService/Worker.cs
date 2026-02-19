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
    private readonly string _fileExtension;
    private readonly int _concurrencyLimit;

    public Worker(ILogger<Worker> logger, IFileRepository fileRepository, IS3UploaderService s3UploaderService, IConfiguration configuration)
    {
        _logger = logger;
        _fileRepository = fileRepository;
        _s3UploaderService = s3UploaderService;
        _configuration = configuration;

        _sourceFolder = _configuration["SourceFolder"] ?? throw new ArgumentNullException("SourceFolder");
        _fileExtension = _configuration["FileExtension"] ?? "*";
        _concurrencyLimit = _configuration.GetValue<int>("ConcurrencyLimit", 5);
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

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Scanning source folder: {Folder}", _sourceFolder);

                if (Directory.Exists(_sourceFolder))
                {
                    var searchPattern = _fileExtension.StartsWith("*") ? _fileExtension : "*" + _fileExtension;
                    var files = Directory.EnumerateFiles(_sourceFolder, searchPattern);

                    var batch = new List<string>();
                    int batchSize = 100;
                    int queuedCount = 0;

                    foreach (var filePath in files)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        batch.Add(filePath);

                        if (batch.Count >= batchSize)
                        {
                            queuedCount += await ProcessBatchAsync(batch, channel.Writer, stoppingToken);
                            batch.Clear();
                        }
                    }

                    if (batch.Count > 0)
                    {
                        queuedCount += await ProcessBatchAsync(batch, channel.Writer, stoppingToken);
                    }

                    _logger.LogInformation("Queued {Count} files for upload.", queuedCount);
                }
                else
                {
                    _logger.LogWarning("Source folder not found: {Folder}", _sourceFolder);
                }

                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
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

    private async Task<int> ProcessBatchAsync(List<string> filePaths, ChannelWriter<FileItem> writer, CancellationToken stoppingToken)
    {
        int count = 0;
        try
        {
            var fileNames = filePaths.Select(Path.GetFileName).ToList();
            var processedNames = await _fileRepository.GetProcessedFileNamesAsync(fileNames!);
            var processedSet = new HashSet<string>(processedNames);

            foreach (var filePath in filePaths)
            {
                var fileName = Path.GetFileName(filePath);
                if (!processedSet.Contains(fileName))
                {
                    var fileInfo = new FileInfo(filePath);
                    await writer.WriteAsync(new FileItem(filePath, fileInfo.Length), stoppingToken);
                    count++;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing batch.");
        }
        return count;
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
                    _logger.LogError(ex, "Failed to process file: {FileName}. Will retry on next scan.", fileName);
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
