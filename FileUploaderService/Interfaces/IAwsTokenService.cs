using Amazon.Runtime;

namespace FileUploaderService.Interfaces;

public interface IAwsTokenService
{
    Task<AWSCredentials> GetCredentialsAsync();
}
