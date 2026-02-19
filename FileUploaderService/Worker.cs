using FileUploaderService.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace FileUploaderService;

public record FileItem(string FilePath, long FileId, long Size);

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
        _scanIntervalSeconds = _configuration.GetValue<int>("ScanIntervalSeconds", 60); // Default 60s

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

                _logger.LogInformation("Scanning source folder: {Folder}", _sourceFolder);

                // Get the last processed ID to act as a high-water mark.
                // We only care about files with ID > lastId.
                long lastId = await _fileRepository.GetMaxProcessedIdAsync();

                // Collect new files
                var newFiles = new List<(long Id, string Path)>();

                try
                {
                    // Enumerate all files matching the extension
                    // This might take time for millions of files, but it's necessary to find the "lowest > lastId".
                    var files = Directory.EnumerateFiles(_sourceFolder, "*" + _fileExtension);

                    foreach (var filePath in files)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        var fileName = Path.GetFileName(filePath);
                        // Parse ID from filename (assuming strict numeric format like "123.wav")
                        var namePart = Path.GetFileNameWithoutExtension(fileName);
                        if (long.TryParse(namePart, out var fileId))
                        {
                            if (fileId > lastId)
                            {
                                newFiles.Add((fileId, filePath));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error enumerating files.");
                }

                if (newFiles.Count > 0)
                {
                    // Sort by ID ascending to process "smallest number first"
                    _logger.LogInformation("Found {Count} new files. Sorting...", newFiles.Count);
                    newFiles.Sort((a, b) => a.Id.CompareTo(b.Id));

                    // Process them in sorted order
                    foreach (var (id, path) in newFiles)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        try
                        {
                            var fileInfo = new FileInfo(path);
                            await channel.Writer.WriteAsync(new FileItem(path, id, fileInfo.Length), stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error checking file info for {Path}", path);
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("No new files found.");
                }

                // Wait before next scan. Ideally longer interval for large folders.
                await Task.Delay(TimeSpan.FromSeconds(_scanIntervalSeconds), stoppingToken);
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
                var fileName = Path.GetFileName(item.FilePath);
                try
                {
                    _logger.LogInformation("Processing file: {FileName} (ID: {Id})", fileName, item.FileId);

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
