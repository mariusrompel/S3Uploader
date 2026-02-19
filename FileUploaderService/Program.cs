using FileUploaderService;
using FileUploaderService.Interfaces;
using FileUploaderService.Repositories;
using FileUploaderService.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Register Services
builder.Services.AddSingleton<IFileRepository, FileRepository>();
builder.Services.AddSingleton<IAwsTokenService, CertificateAwsTokenService>();
builder.Services.AddSingleton<IS3UploaderService, S3UploaderService>();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
