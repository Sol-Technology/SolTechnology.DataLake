using System.Net;
using System.Net.Http.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.PostgreSql;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// Lakekeeper, an Iceberg REST catalog, without auth (allowall), on Postgres, with one warehouse in Silo.
/// Containers reach it as <c>http://lakekeeper:8181/catalog</c>, tests on the host as <see cref="CatalogUri"/>.
/// </summary>
public sealed class LakekeeperFixture(TestContainersContext context) : IAsyncDisposable
{
    private const string Image = "quay.io/lakekeeper/catalog:v0.14.2";
    public const string Warehouse = "spike";

    private PostgreSqlContainer? _database;
    private IContainer? _catalog;

    private IContainer Catalog => _catalog ?? throw new InvalidOperationException("Call InitializeAsync first.");

    public Uri CatalogUri => new($"http://{Catalog.Hostname}:{Catalog.GetMappedPublicPort(8181)}/catalog/");

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var database = new PostgreSqlBuilder("postgres:17")
            .WithNetwork(context.Network)
            .WithNetworkAliases("lakekeeper-db")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .WithDatabase("postgres");
        if (TestContainersContext.ReuseContainers) database = database.WithName("soltech-datalake-lakekeeper-db").WithReuse(true);
        _database = database.Build();
        await _database.StartAsync(ct).ConfigureAwait(false);

        var environment = new Dictionary<string, string>
        {
            ["LAKEKEEPER__PG_ENCRYPTION_KEY"] = "integration-tests-only",
            ["LAKEKEEPER__PG_DATABASE_URL_READ"] = "postgresql://postgres:postgres@lakekeeper-db:5432/postgres",
            ["LAKEKEEPER__PG_DATABASE_URL_WRITE"] = "postgresql://postgres:postgres@lakekeeper-db:5432/postgres",
            ["LAKEKEEPER__AUTHZ_BACKEND"] = "allowall",
        };

        await using (var migrate = new ContainerBuilder(Image).WithNetwork(context.Network).WithEnvironment(environment).WithCommand("migrate").Build())
            await OneShot.RunAsync(migrate, ct).ConfigureAwait(false);

        var catalog = new ContainerBuilder(Image)
            .WithNetwork(context.Network)
            .WithNetworkAliases("lakekeeper")
            .WithEnvironment(environment)
            .WithCommand("serve")
            .WithPortBinding(8181, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8181).ForPath("/health")));
        if (TestContainersContext.ReuseContainers) catalog = catalog.WithName("soltech-datalake-lakekeeper").WithReuse(true);
        _catalog = catalog.Build();
        await _catalog.StartAsync(ct).ConfigureAwait(false);

        await CreateWarehouseAsync(ct).ConfigureAwait(false);
    }

    // Bootstrap and warehouse are created once; with reused containers they already exist.
    private async Task CreateWarehouseAsync(CancellationToken ct)
    {
        using var http = new HttpClient { BaseAddress = new Uri(CatalogUri, "/") };
        var configUri = $"catalog/v1/config?warehouse={Warehouse}";
        using (var existing = await http.GetAsync(configUri, ct).ConfigureAwait(false))
            if (existing.IsSuccessStatusCode) return;

        using (var bootstrap = await http.PostAsJsonAsync("management/v1/bootstrap", new Dictionary<string, object> { ["accept-terms-of-use"] = true }, ct).ConfigureAwait(false))
            if (!bootstrap.IsSuccessStatusCode && bootstrap.StatusCode is not (HttpStatusCode.BadRequest or HttpStatusCode.Conflict))
                throw new InvalidOperationException($"Lakekeeper bootstrap: {bootstrap.StatusCode} {await bootstrap.Content.ReadAsStringAsync(ct)}");

        var warehouse = new Dictionary<string, object>
        {
            ["warehouse-name"] = Warehouse,
            ["project-id"] = "00000000-0000-0000-0000-000000000000",
            ["storage-profile"] = new Dictionary<string, object>
            {
                ["type"] = "s3",
                ["bucket"] = SiloFixture.Bucket,
                ["key-prefix"] = Warehouse,
                ["endpoint"] = SiloFixture.InternalEndpoint,
                ["sts-endpoint"] = SiloFixture.InternalEndpoint,
                ["region"] = SiloFixture.Region,
                ["path-style-access"] = true,
                ["flavor"] = "s3-compat",
                ["sts-enabled"] = true,
            },
            ["storage-credential"] = new Dictionary<string, object>
            {
                ["type"] = "s3",
                ["credential-type"] = "access-key",
                ["access-key-id"] = SiloFixture.User,
                ["secret-access-key"] = SiloFixture.Password,
            },
        };
        using (var created = await http.PostAsJsonAsync("management/v1/warehouse", warehouse, ct).ConfigureAwait(false))
            if (!created.IsSuccessStatusCode)
                throw new InvalidOperationException($"Lakekeeper warehouse: {created.StatusCode} {await created.Content.ReadAsStringAsync(ct)}");

        (await http.GetAsync(configUri, ct).ConfigureAwait(false)).EnsureSuccessStatusCode();
    }

    public async ValueTask DisposeAsync()
    {
        if (TestContainersContext.ReuseContainers) return;
        if (_catalog is not null) await _catalog.DisposeAsync().ConfigureAwait(false);
        if (_database is not null) await _database.DisposeAsync().ConfigureAwait(false);
    }
}
