using System.Text.Json;

namespace IcebergSpike;

// Needs spikes/rest-catalog running (docker compose up -d). Skipped otherwise.
[Category("Docker")]
public class RestCatalogTests
{
    private static readonly Uri CatalogUri = new("http://localhost:8181/catalog/");
    private const string S3FromHost = "http://localhost:9000";
    private static string ExpectedFile => Path.Combine(Fixtures.Root, "..", "rest-catalog", "expected", "orders.json");

    private HttpClient _http = null!;

    [SetUp]
    public async Task CatalogMustBeUp()
    {
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        try { (await _http.GetAsync(new Uri(CatalogUri, "v1/config?warehouse=spike"))).EnsureSuccessStatusCode(); }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException) { Skip("Lakekeeper is not running: " + e.Message); }
        if (!File.Exists(ExpectedFile)) Skip("Run the seed container first.");
    }

    // CI sets REQUIRE_DOCKER=1: there a missing catalog or fixture is a failure, not a skip.
    private static void Skip(string reason)
    {
        if (Environment.GetEnvironmentVariable("REQUIRE_DOCKER") == "1") Assert.Fail(reason);
        Assert.Ignore(reason);
    }

    [TearDown]
    public void Dispose() => _http.Dispose();

    [Test]
    public async Task Loads_table_from_REST_catalog_and_reads_it_with_vended_credentials()
    {
        var expected = JsonDocument.Parse(File.ReadAllText(ExpectedFile)).RootElement;
        var catalog = new RestCatalog(_http, CatalogUri, "spike");

        var loaded = await catalog.LoadTableAsync("shop", "orders");
        var properties = loaded.PropertiesFor(loaded.Metadata.Location);
        TestContext.Out.WriteLine($"{loaded.MetadataLocation}\nvended: {string.Join(", ", properties.Keys.Order())}");

        properties.Should().ContainKey("s3.session-token", "Lakekeeper vends STS credentials");
        loaded.MetadataLocation.Should().Be(expected.GetProperty("metadata_location").GetString());

        using var io = new S3FileIO(properties, endpointOverride: S3FromHost);
        var table = new IcebergTable(loaded.Metadata, io);
        var rows = (await table.Scan()).OrderBy(r => (long)r["id"]!).ToList();

        Oracle.AssertSame(rows, expected.GetProperty("rows").EnumerateArray().ToList());
    }

    [Test]
    public async Task Applies_position_deletes_written_by_Trino()
    {
        var expectedFile = Path.Combine(Path.GetDirectoryName(ExpectedFile)!, "orders_mor.json");
        if (!File.Exists(expectedFile)) Skip("Run spikes/rest-catalog/deletes.sh first.");
        var expected = JsonDocument.Parse(File.ReadAllText(expectedFile)).RootElement;

        var catalog = new RestCatalog(_http, CatalogUri, "spike");
        var loaded = await catalog.LoadTableAsync("shop", "orders_mor");
        using var io = new S3FileIO(loaded.PropertiesFor(loaded.Metadata.Location), endpointOverride: S3FromHost);
        var metrics = new ScanMetrics();

        var rows = (await new IcebergTable(loaded.Metadata, io).Scan(metrics: metrics)).OrderBy(r => (long)r["id"]!).ToList();
        TestContext.Out.WriteLine(metrics);

        metrics.DeleteFiles.Should().BePositive();
        metrics.RowsDeleted.Should().Be(3, "Trino deleted ids 1 and 12 and rewrote id 4");
        Oracle.AssertSame(rows, expected.GetProperty("rows").EnumerateArray().ToList());
    }

    [Test]
    public async Task Filter_skips_files_on_S3()
    {
        var catalog = new RestCatalog(_http, CatalogUri, "spike");
        var loaded = await catalog.LoadTableAsync("shop", "orders");
        using var io = new S3FileIO(loaded.PropertiesFor(loaded.Metadata.Location), endpointOverride: S3FromHost);
        var table = new IcebergTable(loaded.Metadata, io);
        var metrics = new ScanMetrics();
        var oct2 = new DateTimeOffset(2026, 10, 2, 0, 0, 0, TimeSpan.Zero);

        var rows = await table.Scan(filter: [Predicate.Eq("country", "PL"), new("created_at", oct2, oct2.AddDays(1).AddTicks(-10))], metrics: metrics);
        TestContext.Out.WriteLine(metrics);

        metrics.FilesRead.Should().Be(1);
        rows.Should().OnlyContain(r => (string)r["country"]! == "PL"
                                       && ((DateTimeOffset)r["created_at"]!).Date == oct2.Date);
        rows.Should().HaveCount(3);
    }
}
