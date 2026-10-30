using Amazon.Runtime;
using Amazon.S3;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// S3 and STS in one container: Silo, a maintained MinIO fork. Lakekeeper vends short-lived credentials from its STS.
/// Containers reach it as <see cref="InternalEndpoint"/>, tests on the host as <see cref="Endpoint"/>.
/// </summary>
public sealed class SiloFixture(TestContainersContext context) : IAsyncDisposable
{
    private const string Image = "pgsty/silo:RELEASE.2026-09-03T13-18-01Z";
    public const string User = "silo-root-user";
    public const string Password = "silo-root-password";
    public const string Bucket = "lake";
    public const string Region = "local-01";
    public const string InternalEndpoint = "http://silo:9000";

    private IContainer? _container;

    private IContainer Container => _container ?? throw new InvalidOperationException("Call InitializeAsync first.");

    public string Endpoint => $"http://{Container.Hostname}:{Container.GetMappedPublicPort(9000)}";

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var builder = new ContainerBuilder(Image)
            .WithNetwork(context.Network)
            .WithNetworkAliases("silo")
            .WithCommand("server", "/data")
            .WithEnvironment("MINIO_ROOT_USER", User)
            .WithEnvironment("MINIO_ROOT_PASSWORD", Password)
            .WithPortBinding(9000, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(9000).ForPath("/minio/health/live")));
        if (TestContainersContext.ReuseContainers) builder = builder.WithName("soltech-datalake-silo").WithReuse(true);

        _container = builder.Build();
        await _container.StartAsync(ct).ConfigureAwait(false);

        using var s3 = new AmazonS3Client(new BasicAWSCredentials(User, Password),
            new AmazonS3Config { ServiceURL = Endpoint, ForcePathStyle = true, AuthenticationRegion = Region });
        if (!(await s3.ListBucketsAsync(ct).ConfigureAwait(false)).Buckets?.Any(b => b.BucketName == Bucket) ?? true)
            await s3.PutBucketAsync(Bucket, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null && !TestContainersContext.ReuseContainers) await _container.DisposeAsync().ConfigureAwait(false);
    }
}
