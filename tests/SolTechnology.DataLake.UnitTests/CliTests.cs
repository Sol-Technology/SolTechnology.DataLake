using System.Text.Json.Nodes;
using SolTechnology.Avro;
using SolTechnology.DataLake.Cli;

namespace SolTechnology.DataLake.UnitTests;

public class CliTests
{
    private TempDataset _lake = null!;

    [OneTimeSetUp]
    public void DeclareAvroConvertLicence() => AvroConvert.License = AvroLicense.NonCommercial;

    [SetUp]
    public void CreateLake()
    {
        _lake = new TempDataset();
        var fixtures = Path.Combine(AppContext.BaseDirectory, "Fixtures");
        Directory.CreateDirectory(_lake.PathOf("region=eu"));
        File.Copy(Path.Combine(fixtures, "nested.parquet"), _lake.PathOf("region=eu/nested.parquet"));
        File.Copy(Path.Combine(fixtures, "orders.avro"), _lake.PathOf("region=eu/orders.avro"));
        _lake.Touch("_SUCCESS");
    }

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    private static async Task<(int Code, string Output, string Error)> Run(params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var code = await Program.RunAsync(args, output, error);
        return (code, output.ToString(), error.ToString());
    }

    [Test]
    public async Task Ls_lists_data_files_with_sizes()
    {
        var (code, output, _) = await Run("ls", _lake.Root);

        code.Should().Be(0);
        output.Should().Contain("parquet  region=eu/nested.parquet").And.Contain("avro     region=eu/orders.avro").And.Contain("2 files, ");
        output.Should().NotContain("_SUCCESS");
    }

    [Test]
    public async Task Schema_shows_nested_columns_and_partitions()
    {
        var (code, output, _) = await Run("schema", _lake.Root);

        code.Should().Be(0);
        output.Should().Contain("id: int64 not null").And.Contain("lines: list\n  element: struct\n    sku: string").And.Contain("price: decimal(10, 2)")
            .And.Contain("at: time(micros)").And.Contain("uid: uuid").And.Contain("# partitions from the path: region");
    }

    [Test]
    public async Task Head_prints_rows_as_JSON_lines_across_files()
    {
        var (code, output, _) = await Run("head", _lake.Root, "-n", "5");

        code.Should().Be(0);
        var rows = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonNode.Parse(l)!).ToList();
        rows.Should().HaveCount(5);
        rows[0]["address"]!["city"]!.GetValue<string>().Should().Be("Wroclaw");
        rows[0]["region"]!.GetValue<string>().Should().Be("eu");
        rows[4]["customer"]!.GetValue<string>().Should().Be("Ann", "the fifth row is the first record of orders.avro");
    }

    [Test]
    public async Task Cat_prints_every_row_as_JSON_lines()
    {
        var (code, output, _) = await Run("cat", _lake.Root);

        code.Should().Be(0);
        var rows = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonNode.Parse(l)!).ToList();
        rows.Should().HaveCountGreaterThan(5);
        rows.Should().OnlyContain(r => r["region"]!.GetValue<string>() == "eu");
    }

    [Test]
    public async Task Cat_prints_CSV_with_a_header_and_a_limit()
    {
        var sales = LakeContext.Create(o => o.UseParquet(_lake.Root), m => m.Dataset<Sale>("sales").PartitionBy(s => s.Country)).Dataset<Sale>("sales");
        await sales.AppendAsync([new Sale { Id = 1, Name = "a, \"b\"", Tags = ["x"] }, new Sale { Id = 2 }, new Sale { Id = 3 }]);

        var (code, output, _) = await Run("cat", _lake.PathOf("sales"), "--format", "csv", "--limit", "2");

        code.Should().Be(0);
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Should().Equal(
            "Id,Name,Amount,Tags,Country",
            "1,\"a, \"\"b\"\"\",1.5,\"[\"\"x\"\"]\",PL",
            "2,,1.5,,PL");
    }

    [Test]
    public async Task Stats_sums_files_rows_partitions_and_column_ranges()
    {
        var sales = LakeContext.Create(o => o.UseParquet(_lake.Root, configure: p => p.RowGroupSize(10)), m => m.Dataset<Sale>("sales").PartitionBy(s => s.Country))
            .Dataset<Sale>("sales");
        await sales.AppendAsync(Enumerable.Range(0, 30).Select(i => new Sale { Id = i, Name = i % 3 == 0 ? null : $"n{i:00}", Country = i % 2 == 0 ? "PL" : "DE" }));

        var (code, output, _) = await Run("stats", _lake.PathOf("sales"));

        code.Should().Be(0);
        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines[0].Should().StartWith("2 files (2 parquet), ").And.EndWith(", 30 rows, 4 row groups");
        lines[1].Should().Be("partition Country: 2 values (DE, PL)");
        lines.Should().Contain(l => System.Text.RegularExpressions.Regex.IsMatch(l, @"^Id\s+int64\s+0\s+0\s+29$"));
        lines.Should().Contain(l => System.Text.RegularExpressions.Regex.IsMatch(l, @"^Name\s+string\s+10\s+n01\s+n29$"));
        lines.Should().Contain(l => l.StartsWith("Tags[] "));
    }

    public sealed class Sale
    {
        public long Id { get; set; }
        public string? Name { get; set; }
        public decimal Amount { get; set; } = 1.5m;
        public List<string>? Tags { get; set; }
        public string Country { get; set; } = "PL";
    }

    [TestCase("https://attacker.example/c/x.parquet")]
    [TestCase("abfss://c@attacker.example/x")]
    public async Task Azure_credentials_go_only_to_Azure_hosts(string location)
    {
        var (code, _, error) = await Run("head", location);

        code.Should().Be(2);
        error.Should().Contain("not an Azure Storage host");
    }

    [TestCase("abfss://lake@shop.dfs.core.windows.net/orders/")]
    [TestCase("abfss://lake@shop.z12.dfs.storage.azure.net/orders/")]
    [TestCase("https://shop.z12.blob.storage.azure.net/lake/orders/")]
    public void Azure_hosts_get_Azure_storage(string location) =>
        Program.StorageFor(location, null, null).Should().BeOfType<SolTechnology.DataLake.Azure.AzureBlobStorage>().Which.CanHandle(location).Should().BeTrue();

    [Test]
    public async Task Missing_location_is_exit_code_2()
    {
        var (code, _, error) = await Run("ls", _lake.PathOf("nope"));

        code.Should().Be(2);
        error.Should().Contain("DirectoryNotFoundException");
    }
}
