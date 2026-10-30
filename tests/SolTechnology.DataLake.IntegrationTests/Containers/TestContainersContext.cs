using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Networks;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// Shared Testcontainers context for the test assembly: the docker network all fixtures attach to and the
/// <c>TESTCONTAINERS_REUSE</c> policy (same model as SolTechnology.Core.Testing).
/// </summary>
/// <remarks>
/// With reuse enabled, containers and the network keep running between test runs to skip the boot time; they get
/// stable names and <see cref="DisposeAsync"/> leaves them alone. Ryuk is disabled so the suite also runs under
/// Docker Desktop Enhanced Container Isolation.
/// </remarks>
public sealed class TestContainersContext : IAsyncDisposable
{
    public static bool ReuseContainers { get; } =
        string.Equals(Environment.GetEnvironmentVariable("TESTCONTAINERS_REUSE"), "true", StringComparison.OrdinalIgnoreCase);

    static TestContainersContext()
    {
        TestcontainersSettings.ResourceReaperEnabled = false;
        Console.WriteLine($"[Testcontainers] Reuse {(ReuseContainers ? "enabled" : "disabled")}");
    }

    public INetwork Network { get; } = ReuseContainers
        ? new NetworkBuilder().WithName("soltech-datalake-integration-tests").WithReuse(true).WithLabel("testcontainers.reuse", "true").Build()
        : new NetworkBuilder().Build();

    public Task InitializeAsync(CancellationToken ct = default) => Network.CreateAsync(ct);

    public async ValueTask DisposeAsync()
    {
        if (!ReuseContainers) await Network.DisposeAsync().ConfigureAwait(false);
    }
}
