using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// Trino with an Iceberg catalog <c>lake</c> on Lakekeeper (vended credentials). Used to write what PyIceberg
/// cannot, e.g. position delete files from <c>DELETE</c> / <c>UPDATE</c> on a format v2 table.
/// </summary>
public sealed class TrinoFixture(TestContainersContext context) : IAsyncDisposable
{
    private const string Image = "trinodb/trino:476";

    private const string LakeCatalog = $"""
        connector.name=iceberg
        iceberg.catalog.type=rest
        iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog
        iceberg.rest-catalog.warehouse={LakekeeperFixture.Warehouse}
        iceberg.rest-catalog.security=NONE
        iceberg.rest-catalog.vended-credentials-enabled=true
        fs.native-s3.enabled=true
        s3.region={SiloFixture.Region}
        s3.path-style-access=true
        s3.endpoint={SiloFixture.InternalEndpoint}
        """;

    private IContainer? _container;

    private IContainer Container => _container ?? throw new InvalidOperationException("Call InitializeAsync first.");

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var builder = new ContainerBuilder(Image)
            .WithNetwork(context.Network)
            .WithResourceMapping(Encoding.UTF8.GetBytes(LakeCatalog), "/etc/trino/catalog/lake.properties")
            .WithPortBinding(8080, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8080).ForPath("/v1/info")));
        if (TestContainersContext.ReuseContainers) builder = builder.WithName("soltech-datalake-trino").WithReuse(true);

        _container = builder.Build();
        await _container.StartAsync(ct).ConfigureAwait(false);

        // /v1/info answers while the server is still initializing; a query is the real readiness check.
        var deadline = DateTime.UtcNow.AddMinutes(3);
        while (true)
        {
            var result = await Container.ExecAsync(["trino", "--execute", "SELECT 1"], ct).ConfigureAwait(false);
            if (result.ExitCode == 0) return;
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Trino did not start: " + result.Stderr);
            await Task.Delay(2000, ct).ConfigureAwait(false);
        }
    }

    /// <summary>Runs one statement in <c>lake.&lt;schema&gt;</c>; throws with Trino's error when it fails.</summary>
    public async Task<string> ExecuteAsync(string sql, string schema = "shop", CancellationToken ct = default)
    {
        var result = await Container.ExecAsync(["trino", "--catalog", "lake", "--schema", schema, "--execute", sql], ct).ConfigureAwait(false);
        return result.ExitCode == 0 ? result.Stdout : throw new InvalidOperationException($"Trino: {sql}\n{result.Stderr}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null && !TestContainersContext.ReuseContainers) await _container.DisposeAsync().ConfigureAwait(false);
    }
}
