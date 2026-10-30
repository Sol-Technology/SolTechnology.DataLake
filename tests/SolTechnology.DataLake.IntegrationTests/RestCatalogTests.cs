using IcebergSpike;
using SolTechnology.DataLake.IntegrationTests.Containers;
using static SolTechnology.DataLake.IntegrationTests.IntegrationTestsFixture;

namespace SolTechnology.DataLake.IntegrationTests;

// Iceberg REST catalog (Lakekeeper) with S3 credentials vended per table; files in Silo.
public class RestCatalogTests
{
    private HttpClient _http = null!;

    [SetUp]
    public void CreateClient() => _http = new HttpClient();

    [TearDown]
    public void DisposeClient() => _http.Dispose();

    [Test]
    public async Task Loads_table_from_REST_catalog_and_reads_it_with_vended_credentials()
    {
        var loaded = await new RestCatalog(_http, Lakekeeper.CatalogUri, LakekeeperFixture.Warehouse).LoadTableAsync("shop", "orders");
        var properties = loaded.PropertiesFor(loaded.Metadata.Location);

        properties.Should().ContainKey("s3.session-token", "Lakekeeper vends STS credentials");
        loaded.MetadataLocation.Should().Be(Orders.GetProperty("metadata_location").GetString());

        var rows = await ScanAsync(loaded);

        RowAssert.Same(rows, Orders);
    }

    [Test]
    public async Task Applies_position_deletes_written_by_Trino()
    {
        var loaded = await new RestCatalog(_http, Lakekeeper.CatalogUri, LakekeeperFixture.Warehouse).LoadTableAsync("shop", "orders_mor");
        var metrics = new ScanMetrics();

        var rows = await ScanAsync(loaded, metrics: metrics);
        TestContext.Out.WriteLine(metrics);

        metrics.DeleteFiles.Should().BePositive();
        metrics.RowsDeleted.Should().Be(3, "Trino deleted ids 1 and 12 and rewrote id 4");
        RowAssert.Same(rows, OrdersWithDeletes);
    }

    [Test]
    public async Task Filter_skips_files_on_S3()
    {
        var loaded = await new RestCatalog(_http, Lakekeeper.CatalogUri, LakekeeperFixture.Warehouse).LoadTableAsync("shop", "orders");
        var metrics = new ScanMetrics();
        var oct2 = new DateTimeOffset(2026, 10, 2, 0, 0, 0, TimeSpan.Zero);

        var rows = await ScanAsync(loaded, [Predicate.Eq("country", "PL"), new("created_at", oct2, oct2.AddDays(1).AddTicks(-10))], metrics);
        TestContext.Out.WriteLine(metrics);

        metrics.FilesRead.Should().Be(1);
        rows.Should().HaveCount(3).And.OnlyContain(r => (string)r["country"]! == "PL" && ((DateTimeOffset)r["created_at"]!).Date == oct2.Date);
    }

    // Lakekeeper hands out the S3 endpoint containers use (http://silo:9000); from the host it is Silo's mapped port.
    private static async Task<List<Dictionary<string, object?>>> ScanAsync(LoadedTable loaded, IReadOnlyList<Predicate>? filter = null, ScanMetrics? metrics = null)
    {
        using var io = new S3FileIO(loaded.PropertiesFor(loaded.Metadata.Location), endpointOverride: Silo.Endpoint);
        var rows = await new IcebergTable(loaded.Metadata, io).Scan(filter: filter, metrics: metrics);
        return rows.OrderBy(r => (long)r["id"]!).ToList();
    }
}
