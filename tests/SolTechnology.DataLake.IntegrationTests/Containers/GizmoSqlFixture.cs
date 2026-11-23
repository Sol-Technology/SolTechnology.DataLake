using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// GizmoSQL: an Arrow Flight SQL server on DuckDB, the stand-in for Dremio, InfluxDB 3 or any other Flight SQL server.
/// Plain gRPC (no TLS), user and password auth.
/// </summary>
public sealed class GizmoSqlFixture : IAsyncDisposable
{
    private const string Image = "gizmodata/gizmosql:v1.42.3-slim";
    public const string User = "lake";
    public const string Password = "lake-password";

    private IContainer? _container;

    private IContainer Container => _container ?? throw new InvalidOperationException("Call InitializeAsync first.");

    /// <summary>The URI the ADBC Flight SQL driver takes.</summary>
    public string Uri => $"grpc+tcp://{Container.Hostname}:{Container.GetMappedPublicPort(31337)}";

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var builder = new ContainerBuilder(Image)
            .WithEnvironment("TLS_ENABLED", "0")
            .WithEnvironment("GIZMOSQL_USERNAME", User)
            .WithEnvironment("GIZMOSQL_PASSWORD", Password)
            .WithPortBinding(31337, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("GizmoSQL server - started"));
        if (TestContainersContext.ReuseContainers) builder = builder.WithName("soltech-datalake-gizmosql").WithReuse(true);

        _container = builder.Build();
        await _container.StartAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null && !TestContainersContext.ReuseContainers) await _container.DisposeAsync().ConfigureAwait(false);
    }
}
