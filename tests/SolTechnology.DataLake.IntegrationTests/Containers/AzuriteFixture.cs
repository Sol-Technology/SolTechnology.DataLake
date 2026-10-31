using Azure.Storage.Blobs;
using Testcontainers.Azurite;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>Azurite, the Azure Storage emulator, for the Blob API (what ADLS Gen2 and OneLake also serve).</summary>
public sealed class AzuriteFixture(TestContainersContext context) : IAsyncDisposable
{
    private const string Image = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    private AzuriteContainer? _container;

    private AzuriteContainer Container => _container ?? throw new InvalidOperationException("Call InitializeAsync first.");

    public BlobServiceClient CreateClient() => new(Container.GetConnectionString());

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var builder = new AzuriteBuilder(Image)
            .WithNetwork(context.Network)
            .WithCommand("--skipApiVersionCheck");
        if (TestContainersContext.ReuseContainers) builder = builder.WithName("soltech-datalake-azurite").WithReuse(true);

        _container = builder.Build();
        await _container.StartAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null && !TestContainersContext.ReuseContainers) await _container.DisposeAsync().ConfigureAwait(false);
    }
}
