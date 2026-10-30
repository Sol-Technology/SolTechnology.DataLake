using Amazon.Runtime;
using Amazon.S3;

namespace IcebergSpike;

// Where table files live. Iceberg locations are URIs (file://, s3://, abfss://...); the catalog may hand out credentials per table.
public interface IFileIO
{
    Task<Stream> OpenAsync(string location, CancellationToken cancellationToken = default);
}

public sealed class LocalFileIO(Func<string, string> resolve) : IFileIO
{
    public Task<Stream> OpenAsync(string location, CancellationToken cancellationToken = default) =>
        Task.FromResult<Stream>(File.OpenRead(resolve(location)));
}

// S3 with the properties Iceberg uses (s3.endpoint, s3.access-key-id, s3.session-token, ...), e.g. vended by a REST catalog.
public sealed class S3FileIO : IFileIO, IDisposable
{
    private readonly AmazonS3Client _client;

    public S3FileIO(IReadOnlyDictionary<string, string> properties, string? endpointOverride = null)
    {
        var accessKey = properties["s3.access-key-id"];
        var secretKey = properties["s3.secret-access-key"];
        AWSCredentials credentials = properties.TryGetValue("s3.session-token", out var token)
            ? new SessionAWSCredentials(accessKey, secretKey, token)
            : new BasicAWSCredentials(accessKey, secretKey);

        var config = new AmazonS3Config
        {
            ForcePathStyle = properties.GetValueOrDefault("s3.path-style-access") == "true",
            AuthenticationRegion = properties.GetValueOrDefault("s3.region") ?? properties.GetValueOrDefault("client.region"),
        };
        var endpoint = endpointOverride ?? properties.GetValueOrDefault("s3.endpoint");
        if (endpoint is not null) config.ServiceURL = endpoint;
        _client = new AmazonS3Client(credentials, config);
    }

    public async Task<Stream> OpenAsync(string location, CancellationToken cancellationToken = default)
    {
        var uri = new Uri(location);
        if (uri.Scheme is not ("s3" or "s3a")) throw new NotSupportedException(location);

        // Whole object into memory: fine for a spike. Parquet needs seeking; the real reader will use ranged GETs.
        using var response = await _client.GetObjectAsync(uri.Host, uri.AbsolutePath.TrimStart('/'), cancellationToken);
        var buffer = new MemoryStream();
        await response.ResponseStream.CopyToAsync(buffer, cancellationToken);
        buffer.Position = 0;
        return buffer;
    }

    public void Dispose() => _client.Dispose();
}
