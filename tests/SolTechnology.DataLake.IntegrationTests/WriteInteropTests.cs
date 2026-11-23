using System.Text.Json;
using SolTechnology.DataLake.Aws;
using SolTechnology.DataLake.IntegrationTests.Containers;
using SolTechnology.Time;
using static SolTechnology.DataLake.IntegrationTests.IntegrationTestsFixture;

namespace SolTechnology.DataLake.IntegrationTests;

// DataLake writes a partitioned dataset to S3 (Silo); pyarrow and DuckDB read it there (Docker/pyiceberg/interop.py).
public class WriteInteropTests
{
    public enum Status { New, Paid }

    public sealed class Line
    {
        public string Sku { get; set; } = "";
        public int Quantity { get; set; }
    }

    public sealed class Order
    {
        public long Id { get; set; }
        public int? Quantity { get; set; }
        [DecimalPrecision(10, 2)] public decimal Amount { get; set; }
        public string? Note { get; set; }
        public bool Paid { get; set; }
        public Guid Key { get; set; }
        public Status State { get; set; }
        public TimeOnly At { get; set; }
        public DateTimeZone CreatedAt { get; set; }
        public UnzonedDateTime LocalTime { get; set; }
        public List<Line> Lines { get; set; } = [];
        public Dictionary<string, int>? Attributes { get; set; }
        public DateOnly Date { get; set; }
        public string? Country { get; set; }
    }

    private static readonly DateTime Start = new(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc);

    private static Order Row(int i) => new()
    {
        Id = i,
        Quantity = i % 3 == 0 ? null : i,
        Amount = i * 1.25m,
        Note = i % 5 == 0 ? null : $"note {i}",
        Paid = i % 2 == 0,
        Key = new Guid($"00000000-0000-0000-0000-{i:000000000000}"),
        State = (Status)(i % 2),
        At = new TimeOnly(12, 0).Add(TimeSpan.FromTicks(i * 10)),
        CreatedAt = DateTimeZone.FromUtc(Start.AddTicks(i * 10_000_010L), TimeZoneInfo.Utc),
        LocalTime = new UnzonedDateTime(new DateOnly(2026, 10, 1), new TimeOnly(9, 0).Add(TimeSpan.FromMinutes(i))),
        Lines = [new Line { Sku = $"sku-{i}", Quantity = i }],
        Attributes = i % 2 == 0 ? null : new() { ["n"] = i },
        Date = new DateOnly(2026, 10, 1 + i % 2),
        Country = (i % 3) switch { 0 => "PL", 1 => "a/b c", _ => null },
    };

    private static JsonElement _result;
    private static List<Order> _rows = null!;

    [OneTimeSetUp]
    public static async Task WriteAndReadElsewhere()
    {
        _rows = Enumerable.Range(0, 300).Select(Row).ToList();
        var root = $"s3://{SiloFixture.Bucket}/interop/{Guid.NewGuid():N}";
        var location = $"{root}/orders";
        using var s3 = Silo.CreateClient();

        var lake = LakeContext.Create(
            o => o.UseFiles(root, new S3Storage(s3), f => f.Parquet(p => p.RowGroupSize(10))),
            m => m.Dataset<Order>("orders").PartitionBy(r => r.Date, r => r.Country));
        await lake.Dataset<Order>("orders").AppendAsync(_rows);

        var from = _rows[250].CreatedAt.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ");
        var json = await PyIceberg.RunAsync("interop.json", ["python", "/scripts/interop.py", "interop", location, "CreatedAt", from]);
        _result = JsonDocument.Parse(json).RootElement.Clone();
        TestContext.Out.WriteLine(JsonSerializer.Serialize(_result.GetProperty("duckdb").GetProperty("schema")));
        TestContext.Out.WriteLine(JsonSerializer.Serialize(_result.GetProperty("pyarrow").GetProperty("schema")));
    }

    [TestCase("pyarrow")]
    [TestCase("duckdb")]
    public void Reads_every_row(string engine)
    {
        var read = _result.GetProperty(engine);

        read.GetProperty("count").GetInt32().Should().Be(300);
        read.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("Id").GetInt64()).Should().Equal(Enumerable.Range(0, 300).Select(i => (long)i));
    }

    [TestCase("pyarrow")]
    [TestCase("duckdb")]
    public void A_filter_on_the_timestamp_finds_every_matching_row(string engine) =>
        // With timestamp statistics in the wrong unit, engines skip row groups that match.
        _result.GetProperty(engine).GetProperty("filtered").GetInt32().Should().Be(_rows.Count(r => r.CreatedAt.UtcDateTime >= _rows[250].CreatedAt.UtcDateTime));

    [Test]
    public void Types_are_what_pyarrow_expects()
    {
        var schema = _result.GetProperty("pyarrow").GetProperty("schema");

        string Type(string column) => schema.GetProperty(column).GetString()!;
        Type("Id").Should().Be("int64");
        Type("Quantity").Should().Be("int32");
        Type("Amount").Should().Be("decimal128(10, 2)");
        Type("Note").Should().BeOneOf("string", "large_string");
        Type("Key").Should().BeOneOf("extension<arrow.uuid>", "fixed_size_binary[16]", "uuid");
        Type("State").Should().BeOneOf("string", "large_string");
        Type("At").Should().Be("time64[us]");
        Type("CreatedAt").Should().Be("timestamp[us, tz=UTC]");
        Type("LocalTime").Should().Be("timestamp[us]");
        Type("Lines").Should().StartWith("list<element: struct<Sku: string");
        Type("Attributes").Should().StartWith("map<string, int32");
        // pyarrow infers only integers and strings from hive folder names.
        Type("Date").Should().Be("string");
    }

    [Test]
    public void Types_are_what_DuckDB_expects()
    {
        var schema = _result.GetProperty("duckdb").GetProperty("schema");

        string Type(string column) => schema.GetProperty(column).GetString()!;
        Type("Amount").Should().Be("DECIMAL(10,2)");
        Type("Key").Should().Be("UUID");
        Type("At").Should().Be("TIME");
        Type("CreatedAt").Should().Be("TIMESTAMP WITH TIME ZONE");
        Type("LocalTime").Should().Be("TIMESTAMP");
        Type("Lines").Should().Be("STRUCT(Sku VARCHAR, Quantity INTEGER)[]");
        Type("Attributes").Should().Be("MAP(VARCHAR, INTEGER)");
        Type("Date").Should().Be("DATE");
    }

    [TestCase("pyarrow")]
    [TestCase("duckdb")]
    public void Values_are_the_same(string engine)
    {
        var row = _result.GetProperty(engine).GetProperty("rows")[7];

        row.GetProperty("Quantity").GetInt32().Should().Be(7);
        row.GetProperty("Amount").GetString().Should().Be("8.75");
        row.GetProperty("Note").GetString().Should().Be("note 7");
        row.GetProperty("Paid").GetBoolean().Should().BeFalse();
        // pyarrow without the uuid extension returns the 16 bytes.
        row.GetProperty("Key").GetString()!.Replace("-", "").Should().Be("00000000000000000000000000000007");
        row.GetProperty("State").GetString().Should().Be("Paid");
        row.GetProperty("At").GetString().Should().Be("12:00:00.000007");
        row.GetProperty("CreatedAt").GetString().Should().Be("2026-10-01T08:00:07.000007+00:00");
        row.GetProperty("LocalTime").GetString().Should().Be("2026-10-01T09:07:00");
        row.GetProperty("Lines")[0].GetProperty("Sku").GetString().Should().Be("sku-7");
        row.GetProperty("Attributes").GetProperty("n").GetInt32().Should().Be(7);
        row.GetProperty("Date").GetString().Should().Be("2026-10-02");
        row.GetProperty("Country").GetString().Should().Be("a/b c", "partition values are escaped in the path and decoded on read");
        _result.GetProperty(engine).GetProperty("rows")[3].GetProperty("Quantity").ValueKind.Should().Be(JsonValueKind.Null);
        _result.GetProperty(engine).GetProperty("rows")[2].GetProperty("Country").ValueKind.Should().Be(JsonValueKind.Null, "__HIVE_DEFAULT_PARTITION__ is null");
    }

    [Test]
    public void Timestamp_statistics_are_in_microseconds()
    {
        var stats = _result.GetProperty("stats").EnumerateArray().ToList();

        stats.Should().HaveCount(30);
        stats.Should().OnlyContain(s => s.GetProperty("created_by").GetString()!.StartsWith("SolTechnology.DataLake version "));
        // pyarrow decodes min/max in the column's unit; in milliseconds they would land in 1970.
        stats.Should().OnlyContain(s => s.GetProperty("min").GetString()!.StartsWith("2026-10-01T08:0"));
        stats.Should().OnlyContain(s => s.GetProperty("max").GetString()!.StartsWith("2026-10-01T08:0"));
    }
}
