using Amazon.S3;
using Azure.Identity;
using Azure.Storage.Blobs;
using SolTechnology.DataLake.Aws;
using SolTechnology.DataLake.Azure;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Cli;

/// <summary>Storage for a location, with credentials from the environment, the way the cloud CLIs find them.</summary>
internal static class StorageResolver
{
    private static readonly string[] AzureSchemes = ["abfss", "abfs", "wasbs", "wasb", "az"];

    public static ILakeStorage For(string location, string? s3Endpoint, string? azureConnectionString)
    {
        var scheme = location.Contains("://", StringComparison.Ordinal) ? location[..location.IndexOf("://", StringComparison.Ordinal)].ToLowerInvariant() : "";

        if (scheme is "s3" or "s3a")
        {
            var config = new AmazonS3Config();
            if (s3Endpoint is not null)
            {
                config.ServiceURL = s3Endpoint;
                config.ForcePathStyle = true;
            }
            return new S3Storage(new AmazonS3Client(config));
        }

        if (AzureSchemes.Contains(scheme) || scheme == "https")
        {
            if (azureConnectionString is not null) return new AzureBlobStorage(new BlobServiceClient(azureConnectionString));
            var account = AccountUrl(location)
                ?? throw new ArgumentException($"'{location}' does not name an account; pass --azure-connection-string or set AZURE_STORAGE_CONNECTION_STRING.");
            return new AzureBlobStorage(new BlobServiceClient(account, new DefaultAzureCredential()));
        }

        return LocalStorage.Instance;
    }

    // abfss://c@shop.dfs.core.windows.net/x -> https://shop.blob.core.windows.net; OneLake -> https://onelake.blob.fabric.microsoft.com
    private static Uri? AccountUrl(string location)
    {
        var rest = location[(location.IndexOf("://", StringComparison.Ordinal) + 3)..];
        var authority = rest.Split('/')[0];
        var host = authority.Contains('@') ? authority[(authority.IndexOf('@') + 1)..] : authority;
        if (!host.Contains('.')) return null;
        return new Uri("https://" + host.Replace(".dfs.", ".blob.", StringComparison.OrdinalIgnoreCase));
    }
}
