using System.Text.Json;
using SolTechnology.Avro;
using SolTechnology.Time;

namespace SolTechnology.DataLake.UnitTests;

// Fixtures/orders.avro is written by fastavro (Fixtures/generate_avro.py), an independent Avro writer.
public class AvroTests
{
    private static string Fixture => Path.Combine(AppContext.BaseDirectory, "Fixtures", "orders.avro");

    private TempDataset _lake = null!;

    [OneTimeSetUp]
    public void DeclareAvroConvertLicence() => AvroConvert.License = AvroLicense.NonCommercial;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    public enum Status { New, Paid }

    public sealed class Address
    {
        public string? City { get; set; }
        public string Zip { get; set; } = "";
    }

    public sealed class Order
    {
        public long Id { get; set; }
        public string? Customer { get; set; }
        public decimal? Amount { get; set; }
        public DateTimeZone CreatedAt { get; set; }
        public UnzonedDateTime LocalAt { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly? At { get; set; }
        public Guid? Uid { get; set; }
        public Status Status { get; set; }
        public Address? Address { get; set; }
        public List<string> Tags { get; set; } = [];
        public Dictionary<string, long?> Attributes { get; set; } = [];
        public string Region { get; set; } = "";
    }

    private IDatasetQuery<Order> Open() => Dataset.Open<Order>(_lake.Root, new DataLakeOptions { ColumnNaming = JsonNamingPolicy.SnakeCaseLower });

    [Test]
    public async Task Reads_records_like_Parquet_rows()
    {
        Copy("region=eu/orders.avro");

        var rows = await Open().ReadAsync().ToListAsync();

        rows.Should().HaveCount(2);
        var first = rows[0];
        first.Id.Should().Be(1);
        first.Customer.Should().Be("Ann");
        first.Amount.Should().Be(9.99m);
        first.CreatedAt.ToDateTimeOffset().Should().Be(new DateTimeOffset(2026, 10, 29, 12, 30, 15, TimeSpan.Zero).AddTicks(1234560));
        first.LocalAt.Should().Be(new UnzonedDateTime(new DateOnly(2026, 10, 29), new TimeOnly(14, 30, 15, 123)));
        first.Day.Should().Be(new DateOnly(2026, 10, 29));
        first.At.Should().Be(new TimeOnly(12, 30, 15).Add(TimeSpan.FromTicks(1234560)));
        first.Uid.Should().Be(Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7"));
        first.Status.Should().Be(Status.Paid);
        first.Address.Should().BeEquivalentTo(new Address { City = "Wroclaw", Zip = "50-001" });
        first.Tags.Should().Equal("a", "b");
        first.Attributes.Should().BeEquivalentTo(new Dictionary<string, long?> { ["color"] = 1, ["size"] = null });
        first.Region.Should().Be("eu");

        rows[1].Should().BeEquivalentTo(new { Id = 2L, Customer = (string?)null, Amount = (decimal?)null, At = (TimeOnly?)null, Address = (Address?)null, Status = Status.New });
        rows[1].Tags.Should().BeEmpty();
    }

    [Test]
    public async Task Partitions_skip_Avro_files_and_the_filter_runs_per_record()
    {
        Copy("region=eu/orders.avro");
        Copy("region=us/orders.avro");
        var metrics = new ScanMetrics();

        var rows = await Open().Where(o => o.Region == "us" && o.Status == Status.Paid).ReadAsync(metrics).ToListAsync();

        metrics.Files.Should().Be(2);
        metrics.FilesSkippedByPartition.Should().Be(1);
        metrics.RowsRead.Should().Be(2);
        rows.Should().ContainSingle().Which.Region.Should().Be("us");
    }

    public sealed class Instant
    {
        [System.Runtime.Serialization.DataMember(Name = "local_at")]
        public DateTimeOffset LocalAt { get; set; }
    }

    public sealed class MissingColumn
    {
        public long Version { get; set; }
    }

    [Test]
    public async Task Same_errors_as_for_Parquet()
    {
        Copy("orders.avro");

        var localIntoOffset = () => Dataset.Open<Instant>(_lake.Root).ReadAsync().ToListAsync().AsTask();
        var missing = () => Dataset.Open<MissingColumn>(_lake.Root).ReadAsync().ToListAsync().AsTask();

        (await localIntoOffset.Should().ThrowAsync<NotSupportedException>()).WithMessage("*'local_at' is a timestamp without a time zone*UnzonedDateTime*");
        (await missing.Should().ThrowAsync<InvalidDataException>()).WithMessage("*orders.avro has no column 'Version'*");
    }

    private void Copy(string relative)
    {
        var target = _lake.PathOf(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        File.Copy(Fixture, target);
    }
}
