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
    public async Task Missing_location_is_exit_code_2()
    {
        var (code, _, error) = await Run("ls", _lake.PathOf("nope"));

        code.Should().Be(2);
        error.Should().Contain("DirectoryNotFoundException");
    }
}
