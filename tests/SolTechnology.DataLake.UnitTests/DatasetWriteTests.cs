using System.Runtime.Serialization;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Storage;
using SolTechnology.Time;

namespace SolTechnology.DataLake.UnitTests;

public class DatasetWriteTests
{
    private TempDataset _lake = null!;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    public enum Status { New, Paid }

    public sealed class Everything
    {
        public long Id { get; set; }
        public int? Quantity { get; set; }
        public short Small { get; set; }
        public byte Tiny { get; set; }
        public ulong Big { get; set; }
        public double Ratio { get; set; }
        public float? Weight { get; set; }
        public bool Active { get; set; }
        [DecimalPrecision(10, 2)] public decimal Amount { get; set; }
        public decimal? Precise { get; set; }
        public string Name { get; set; } = "";
        public string? Note { get; set; }
        public byte[]? Payload { get; set; }
        public Guid Key { get; set; }
        public Status State { get; set; }
        public Status? MaybeState { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly At { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTimeOffset? Seen { get; set; }
        public DateTimeZone Zoned { get; set; }
        public UnzonedDateTime Local { get; set; }
        [DataMember(Name = "renamed_column")] public int Renamed { get; set; }
        [IgnoreDataMember] public string Ignored { get; set; } = "";
    }

    private static Everything Sample(int i) => new()
    {
        Id = i,
        Quantity = i % 3 == 0 ? null : i,
        Small = (short)-i,
        Tiny = (byte)i,
        Big = ulong.MaxValue - (ulong)i,
        Ratio = i / 3.0,
        Weight = i % 2 == 0 ? null : i * 1.5f,
        Active = i % 2 == 0,
        Amount = i * 1.25m,
        Precise = i % 4 == 0 ? null : 1234567890.123456789m + i,
        Name = $"name-{i}",
        Note = i % 5 == 0 ? null : $"note ą ż {i}",
        Payload = i % 2 == 0 ? null : [(byte)i, 2, 3],
        Key = new Guid(i, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        State = (Status)(i % 2),
        MaybeState = i % 3 == 0 ? null : Status.Paid,
        Day = new DateOnly(2026, 10, 1).AddDays(i),
        At = new TimeOnly(12, 30, 15).Add(TimeSpan.FromTicks(i * 10)),
        CreatedUtc = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc).AddTicks(i * 12_340),
        Seen = i % 2 == 0 ? null : new DateTimeOffset(2026, 10, 1, 10, 0, 0, TimeSpan.FromHours(2)).AddMinutes(i),
        Zoned = DateTimeZone.FromUtc(new DateTime(2026, 10, 1, 6, 0, 0, DateTimeKind.Utc).AddSeconds(i), TimeZoneInfo.Utc),
        Local = new UnzonedDateTime(new DateOnly(2026, 10, 1), new TimeOnly(9, 0).Add(TimeSpan.FromMinutes(i))),
        Renamed = i * 7,
        Ignored = "not written",
    };

    [Test]
    public async Task Every_type_reads_back_the_same()
    {
        var rows = Enumerable.Range(0, 100).Select(Sample).ToList();

        var result = await DataLake.Open<Everything>(_lake.Root).WriteAsync(rows);
        var read = await DataLake.Open<Everything>(_lake.Root).ReadAsync().ToListAsync();

        result.Rows.Should().Be(100);
        result.Files.Should().ContainSingle().Which.RelativePath.Should().MatchRegex(@"^part-00001-[0-9a-f]{12}\.snappy\.parquet$");
        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering()
            .Excluding(r => r.Ignored)
            .Using<DateTimeOffset>(c => c.Subject.Should().Be(c.Expectation)).WhenTypeIs<DateTimeOffset>());
        read.Should().OnlyContain(r => r.Ignored == "");
        read[1].CreatedUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Test]
    public async Task Columns_have_the_types_other_engines_expect()
    {
        await DataLake.Open<Everything>(_lake.Root).WriteAsync([Sample(1)]);

        await using var stream = File.OpenRead(Directory.GetFiles(_lake.Root)[0]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        var fields = reader.Schema.DataFields.ToDictionary(f => f.Name);

        fields.Keys.Should().Contain("renamed_column").And.NotContain("Ignored").And.NotContain("Renamed");
        fields["Id"].IsNullable.Should().BeFalse();
        fields["Quantity"].IsNullable.Should().BeTrue();
        fields["Name"].IsNullable.Should().BeTrue("strings are reference types");
        fields["Amount"].Should().BeOfType<DecimalDataField>().Which.Precision.Should().Be(10);
        fields["Precise"].Should().BeOfType<DecimalDataField>().Which.Scale.Should().Be(18);
        fields["State"].SchemaElement!.LogicalType!.STRING.Should().NotBeNull("enums are written by name");
        fields["Key"].SchemaElement!.LogicalType!.UUID.Should().NotBeNull();
        fields["At"].SchemaElement!.LogicalType!.TIME!.Unit.MICROS.Should().NotBeNull();
        fields["At"].SchemaElement!.LogicalType!.TIME!.IsAdjustedToUTC.Should().BeFalse("a time of day is not an instant");
        foreach (var (name, utc) in new[] { ("CreatedUtc", true), ("Seen", true), ("Zoned", true), ("Local", false) })
        {
            var timestamp = fields[name].SchemaElement!.LogicalType!.TIMESTAMP!;
            timestamp.Unit.MICROS.Should().NotBeNull(name);
            timestamp.IsAdjustedToUTC.Should().Be(utc, name);
        }
    }

    [Test]
    public async Task Timestamp_statistics_are_in_microseconds()
    {
        var rows = Enumerable.Range(0, 10).Select(Sample).ToList();
        await DataLake.Open<Everything>(_lake.Root).WriteAsync(rows);

        await using var stream = File.OpenRead(Directory.GetFiles(_lake.Root)[0]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var statistics = group.GetStatistics(reader.Schema.DataFields.Single(f => f.Name == "CreatedUtc"))!;

        statistics.MinValue.Should().Be(Micros(rows[0].CreatedUtc));
        statistics.MaxValue.Should().Be(Micros(rows[9].CreatedUtc));
        statistics.NullCount.Should().Be(0);
    }

    [Test]
    public async Task Filters_skip_row_groups_by_the_written_timestamp_statistics()
    {
        var rows = Enumerable.Range(0, 1000).Select(Sample).ToList();
        await DataLake.Open<Everything>(_lake.Root).WriteAsync(rows, new DatasetWriteOptions { RowGroupSize = 100 });
        var from = rows[950].CreatedUtc;
        var metrics = new ScanMetrics();

        var read = await DataLake.Open<Everything>(_lake.Root).Where(r => r.CreatedUtc >= from).ReadAsync(metrics).ToListAsync();

        read.Select(r => r.Id).Should().Equal(Enumerable.Range(950, 50).Select(i => (long)i));
        metrics.RowGroups.Should().Be(10);
        metrics.RowGroupsSkippedByStatistics.Should().Be(9);
    }

    public sealed class Line
    {
        public string Sku { get; set; } = "";
        public int? Quantity { get; set; }
        public List<string?>? Tags { get; set; }
    }

    public sealed class Address
    {
        public string? City { get; set; }
        public Street? Street { get; set; }
    }

    public sealed class Street
    {
        public string Name { get; set; } = "";
        public int? Number { get; set; }
    }

    public sealed class Order
    {
        public long Id { get; set; }
        public List<Line>? Lines { get; set; }
        public int[] Scores { get; set; } = [];
        public List<int?>? Optional { get; set; }
        public Address? Address { get; set; }
        public Dictionary<string, int?>? Attributes { get; set; }
        public Dictionary<int, Line>? ByNumber { get; set; }
        public List<List<int>>? Matrix { get; set; }
        public List<DateTime>? Times { get; set; }
    }

    [Test]
    public async Task Nested_columns_read_back_the_same()
    {
        var t = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc);
        Order[] rows =
        [
            new()
            {
                Id = 1,
                Lines = [new Line { Sku = "a", Quantity = 2, Tags = ["x", null] }, new Line { Sku = "b", Tags = [] }],
                Scores = [1, 2, 3],
                Optional = [1, null, 3],
                Address = new Address { City = "Kraków", Street = new Street { Name = "Długa", Number = 5 } },
                Attributes = new() { ["color"] = 1, ["size"] = null },
                ByNumber = new() { [7] = new Line { Sku = "c" } },
                Matrix = [[1, 2], [], [3]],
                Times = [t, t.AddTicks(10)],
            },
            new() { Id = 2, Lines = [], Scores = [], Optional = [], Address = new Address(), Attributes = [], ByNumber = [], Matrix = [], Times = [] },
            new() { Id = 3 },
        ];

        await DataLake.Open<Order>(_lake.Root).WriteAsync(rows);
        var read = await DataLake.Open<Order>(_lake.Root).ReadAsync().ToListAsync();

        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    [Test]
    public async Task Nested_timestamp_statistics_are_in_microseconds()
    {
        var t = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc);
        await DataLake.Open<Order>(_lake.Root).WriteAsync([new Order { Id = 1, Times = [t.AddTicks(10), t] }, new Order { Id = 2 }]);

        await using var stream = File.OpenRead(Directory.GetFiles(_lake.Root)[0]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var statistics = group.GetStatistics(reader.Schema.DataFields.Single(f => f.Path.ToString().StartsWith("Times")))!;

        statistics.MinValue.Should().Be(Micros(t));
        statistics.MaxValue.Should().Be(Micros(t) + 1);
    }

    public sealed class Node
    {
        public long Id { get; set; }
        public Node? Parent { get; set; }
    }

    [Test]
    public async Task A_recursive_class_is_an_error()
    {
        var write = () => DataLake.Open<Node>(_lake.Root).WriteAsync([new Node()]);

        (await write.Should().ThrowAsync<NotSupportedException>()).WithMessage("*Parent.Parent*Node contains itself*");
    }

    public sealed class Sale
    {
        public long Id { get; set; }
        public DateOnly Date { get; set; }
        public string? Country { get; set; }
        public decimal Amount { get; set; }
    }

    private static List<Sale> Sales(int count, int offset = 0) => Enumerable.Range(offset, count).Select(i => new Sale
    {
        Id = i,
        Date = new DateOnly(2026, 10, 1 + i % 3),
        Country = (i % 4) switch { 0 => "PL", 1 => "DE", 2 => "a/b c%", _ => null },
        Amount = i,
    }).ToList();

    [Test]
    public async Task PartitionBy_writes_hive_folders_and_reads_back()
    {
        var rows = Sales(120);

        var result = await DataLake.Open<Sale>(_lake.Root).WriteAsync(rows, new DatasetWriteOptions { PartitionBy = [nameof(Sale.Date), nameof(Sale.Country)] });
        var read = await DataLake.Open<Sale>(_lake.Root).ReadAsync().ToListAsync();

        result.Files.Should().HaveCount(12);
        Directory.GetDirectories(_lake.Root).Select(Path.GetFileName).Should().BeEquivalentTo("Date=2026-10-01", "Date=2026-10-02", "Date=2026-10-03");
        Directory.GetDirectories(_lake.PathOf("Date=2026-10-01")).Select(Path.GetFileName)
            .Should().BeEquivalentTo("Country=PL", "Country=DE", "Country=a%2Fb c%25", "Country=__HIVE_DEFAULT_PARTITION__");
        read.OrderBy(r => r.Id).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());

        await using var stream = File.OpenRead(result.Files[0].Location);
        await using var reader = await ParquetReader.CreateAsync(stream);
        reader.Schema.DataFields.Select(f => f.Name).Should().Equal("Id", "Amount");
    }

    [Test]
    public async Task Partition_filters_skip_written_folders()
    {
        await DataLake.Open<Sale>(_lake.Root).WriteAsync(Sales(120), new DatasetWriteOptions { PartitionBy = [nameof(Sale.Country)] });
        var metrics = new ScanMetrics();

        var read = await DataLake.Open<Sale>(_lake.Root).Where(s => s.Country == "a/b c%").ReadAsync(metrics).ToListAsync();

        read.Should().HaveCount(30).And.OnlyContain(s => s.Country == "a/b c%");
        metrics.FilesSkippedByPartition.Should().Be(3);
    }

    [Test]
    public async Task ErrorIfExists_refuses_a_location_with_data()
    {
        var dataset = DataLake.Open<Sale>(_lake.Root);
        await dataset.WriteAsync(Sales(10));

        var again = () => dataset.WriteAsync(Sales(10));

        (await again.Should().ThrowAsync<IOException>()).WithMessage("*already has 1 data file*Append or OverwritePartitions*");
        Directory.GetFiles(_lake.Root).Should().ContainSingle();
    }

    [Test]
    public async Task Append_adds_files()
    {
        var dataset = DataLake.Open<Sale>(_lake.Root);
        await dataset.WriteAsync(Sales(10));

        await dataset.WriteAsync(Sales(5, 10), new DatasetWriteOptions { Mode = DatasetWriteMode.Append });

        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().BeEquivalentTo(Enumerable.Range(0, 15).Select(i => (long)i));
    }

    [Test]
    public async Task OverwritePartitions_replaces_only_the_partitions_written()
    {
        var dataset = DataLake.Open<Sale>(_lake.Root);
        var byDate = new DatasetWriteOptions { PartitionBy = [nameof(Sale.Date)] };
        await dataset.WriteAsync(Sales(30), byDate);

        var newDay2 = new List<Sale> { new() { Id = 1000, Date = new DateOnly(2026, 10, 2), Country = "PL", Amount = 1 } };
        var result = await dataset.WriteAsync(newDay2, new DatasetWriteOptions { PartitionBy = [nameof(Sale.Date)], Mode = DatasetWriteMode.OverwritePartitions });

        var read = await dataset.ReadAsync().ToListAsync();
        result.Deleted.Should().ContainSingle().Which.RelativePath.Should().StartWith("Date=2026-10-02/");
        read.Where(s => s.Date == new DateOnly(2026, 10, 2)).Select(s => s.Id).Should().Equal(1000);
        read.Count(s => s.Date != new DateOnly(2026, 10, 2)).Should().Be(20);
    }

    [Test]
    public async Task OverwritePartitions_without_partitions_replaces_the_dataset()
    {
        var dataset = DataLake.Open<Sale>(_lake.Root);
        await dataset.WriteAsync(Sales(30));
        _lake.Touch("_SUCCESS");

        await dataset.WriteAsync(Sales(2, 100), new DatasetWriteOptions { Mode = DatasetWriteMode.OverwritePartitions });

        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().Equal(100, 101);
        File.Exists(_lake.PathOf("_SUCCESS")).Should().BeTrue("only data files are replaced");
    }

    [Test]
    public async Task Large_writes_roll_over_to_new_files_and_row_groups()
    {
        var rows = Enumerable.Range(0, 20_000).Select(i => new Sale { Id = i, Date = new DateOnly(2026, 1, 1), Country = $"country-{i}", Amount = i }).ToList();

        var result = await DataLake.Open<Sale>(_lake.Root).WriteAsync(rows,
            new DatasetWriteOptions { RowGroupSize = 1000, TargetFileSize = 50_000, Compression = ParquetCompression.Zstd });

        result.Files.Should().HaveCountGreaterThan(2).And.OnlyContain(f => f.RelativePath.EndsWith(".zstd.parquet"));
        var metrics = new ScanMetrics();
        (await DataLake.Open<Sale>(_lake.Root).ReadAsync(metrics).CountAsync()).Should().Be(20_000);
        metrics.RowGroups.Should().Be(20);
    }

    [TestCase(ParquetCompression.Snappy)]
    [TestCase(ParquetCompression.Zstd)]
    [TestCase(ParquetCompression.Gzip)]
    [TestCase(ParquetCompression.None)]
    public async Task Every_compression_reads_back(ParquetCompression compression)
    {
        await DataLake.Open<Sale>(_lake.Root).WriteAsync(Sales(50), new DatasetWriteOptions { Compression = compression });

        (await DataLake.Open<Sale>(_lake.Root).ReadAsync().CountAsync()).Should().Be(50);
    }

    [Test]
    public async Task Values_that_would_be_changed_are_errors()
    {
        var unspecified = () => DataLake.Open<Everything>(_lake.PathOf("a")).WriteAsync([new Everything { CreatedUtc = new DateTime(2026, 1, 1) }]);
        var tooPrecise = () => DataLake.Open<Everything>(_lake.PathOf("b")).WriteAsync([new Everything { Amount = 1.234m }]);
        var tooLarge = () => DataLake.Open<Everything>(_lake.PathOf("c")).WriteAsync([new Everything { Amount = 123_456_789m }]);

        (await unspecified.Should().ThrowAsync<InvalidDataException>()).WithMessage("*CreatedUtc*Kind.Unspecified*UnzonedDateTime*");
        (await tooPrecise.Should().ThrowAsync<InvalidDataException>()).WithMessage("*Amount*1.234*more than 2 digits*");
        (await tooLarge.Should().ThrowAsync<InvalidDataException>()).WithMessage("*Amount*does not fit decimal(10, 2)*");
        Directory.EnumerateFiles(_lake.Root, "*", SearchOption.AllDirectories).Should().BeEmpty("a failed write leaves nothing behind");
    }

    [Test]
    public async Task Bad_options_are_errors()
    {
        var dataset = DataLake.Open<Sale>(_lake.Root);

        await FluentActions.Awaiting(() => dataset.WriteAsync(Sales(1), new DatasetWriteOptions { PartitionBy = ["Nope"] }))
            .Should().ThrowAsync<ArgumentException>().WithMessage("*no property 'Nope'*");
        await FluentActions.Awaiting(() => dataset.WriteAsync(Sales(1), new DatasetWriteOptions { PartitionBy = [nameof(Sale.Amount)] }))
            .Should().ThrowAsync<NotSupportedException>().WithMessage("*Amount is Decimal*");
        await FluentActions.Awaiting(() => dataset.Where(s => s.Id > 1).WriteAsync(Sales(1)))
            .Should().ThrowAsync<InvalidOperationException>();
    }

    [Test]
    public async Task A_read_only_storage_fails_before_writing()
    {
        var dataset = DataLake.Open<Sale>("mem://lake", new DataLakeOptions { Storage = new EmptyReadOnlyStorage() });

        await FluentActions.Awaiting(() => dataset.WriteAsync(Sales(1))).Should().ThrowAsync<NotSupportedException>().WithMessage("*read-only*");
    }

    private sealed class EmptyReadOnlyStorage : ILakeStorage
    {
        public bool CanHandle(string location) => location.StartsWith("mem://");
        public IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken cancellationToken = default) => AsyncEnumerable.Empty<LakeFile>();
        public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default) => ValueTask.FromResult(0);
    }

    private static long Micros(DateTime utc) => (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10;
}
