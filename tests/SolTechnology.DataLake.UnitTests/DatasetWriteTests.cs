using System.Linq.Expressions;
using System.Runtime.Serialization;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Storage;
using SolTechnology.Time;

namespace SolTechnology.DataLake.UnitTests;

// Every write test runs on local disk and on UseInMemory(): the same provider on two storages, so the same results.
[TestFixture("local")]
[TestFixture("memory")]
public class DatasetWriteTests(string provider)
{
    private TempDataset _temp = null!;
    private MemoryStorage _memory = null!;

    [SetUp]
    public void CreateLake()
    {
        _temp = new TempDataset();
        _memory = new MemoryStorage();
    }

    [TearDown]
    public void DeleteLake() => _temp.Dispose();

    private ILakeStorage Storage => provider == "local" ? LocalStorage.Instance : _memory;

    private string Location(string name) => provider == "local" ? $"{_temp.Root}/{name}" : $"memory://lake/{name}";

    private LakeContext Lake(Action<LakeModelBuilder>? model = null, Action<ParquetProviderOptions>? options = null) =>
        LakeContext.Create(o =>
        {
            if (provider == "local") o.UseParquet(_temp.Root, configure: options);
            else o.UseInMemory(_memory, options);
        }, model);

    private IDataset<T> Writable<T>(params Expression<Func<T, object?>>[] partitionBy) where T : class =>
        Lake(m => m.Dataset<T>("data").PartitionBy(partitionBy)).Dataset<T>("data");

    private IDataset<T> Writable<T>(Action<ParquetProviderOptions> options, params Expression<Func<T, object?>>[] partitionBy) where T : class =>
        Lake(m => m.Dataset<T>("data").PartitionBy(partitionBy), options).Dataset<T>("data");

    private async Task<List<string>> FilesOf(string name = "data")
    {
        try
        {
            return await Storage.ListAsync(Location(name)).Select(f => f.RelativePath).OrderBy(p => p, StringComparer.Ordinal).ToListAsync();
        }
        catch (DirectoryNotFoundException)
        {
            return [];
        }
    }

    private async Task<ParquetReader> OpenParquet(LakeFile file)
    {
        var bytes = new byte[file.Length];
        await Storage.ReadAsync(file, 0, bytes);
        return await ParquetReader.CreateAsync(new MemoryStream(bytes));
    }

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
        var dataset = Writable<Everything>();

        var result = await dataset.AppendAsync(rows);
        var read = await dataset.ReadAsync().ToListAsync();

        result.Rows.Should().Be(100);
        result.Written.Should().ContainSingle().Which.RelativePath.Should().MatchRegex(@"^part-00001-[0-9a-f]{12}\.snappy\.parquet$");
        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering()
            .Excluding(r => r.Ignored)
            .Using<DateTimeOffset>(c => c.Subject.Should().Be(c.Expectation)).WhenTypeIs<DateTimeOffset>());
        read.Should().OnlyContain(r => r.Ignored == "");
        read[1].CreatedUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Test]
    public async Task Columns_have_the_types_other_engines_expect()
    {
        var result = await Writable<Everything>().AppendAsync([Sample(1)]);

        await using var reader = await OpenParquet(result.Written[0]);
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
        var result = await Writable<Everything>().AppendAsync(rows);

        await using var reader = await OpenParquet(result.Written[0]);
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
        var dataset = Writable<Everything>(o => o.RowGroupSize(100));
        await dataset.AppendAsync(rows);
        var from = rows[950].CreatedUtc;
        var metrics = new ScanMetrics();

        var read = await dataset.Where(r => r.CreatedUtc >= from).ReadAsync(metrics).ToListAsync();

        read.Select(r => r.Id).Should().Equal(Enumerable.Range(950, 50).Select(i => (long)i));
        metrics.RowGroups.Should().Be(10);
        metrics.RowGroupsSkippedByStatistics.Should().Be(9);
    }

    [Test]
    public async Task Filters_skip_row_groups_by_text_statistics()
    {
        var dataset = Writable<Sale>(o => o.RowGroupSize(10));
        await dataset.AppendAsync(Enumerable.Range(0, 100).Select(i => new Sale { Id = i, Country = $"c{i / 10:00}" }));
        var metrics = new ScanMetrics();

        var read = await dataset.Where(s => s.Country == "c07").ReadAsync(metrics).ToListAsync();

        read.Select(r => r.Id).Should().Equal(Enumerable.Range(70, 10).Select(i => (long)i));
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
        var dataset = Writable<Order>();

        await dataset.AppendAsync(rows);
        var read = await dataset.ReadAsync().ToListAsync();

        read.Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());
    }

    public sealed class NarrowLine
    {
        public int? Quantity { get; set; }
    }

    public sealed class NarrowAddress
    {
        public NarrowStreet? Street { get; set; }
    }

    public sealed class NarrowStreet
    {
        public int? Number { get; set; }
    }

    public sealed class NarrowOrder
    {
        public long Id { get; set; }
        public NarrowLine[]? Lines { get; set; }
        public NarrowAddress? Address { get; set; }
        public IReadOnlyDictionary<int, NarrowLine>? ByNumber { get; set; }
        public IReadOnlyList<IReadOnlyList<long>>? Matrix { get; set; }
    }

    [Test]
    public async Task Nested_columns_read_into_narrower_classes_skip_the_rest()
    {
        Order[] rows =
        [
            new()
            {
                Id = 1,
                Lines = [new Line { Sku = "a", Quantity = 2, Tags = ["x", "y"] }, new Line { Sku = "b", Tags = [] }],
                Address = new Address { City = "Kraków", Street = new Street { Name = "Długa", Number = 5 } },
                ByNumber = new() { [7] = new Line { Sku = "c", Quantity = 9, Tags = ["z"] } },
                Matrix = [[1, 2], [], [3]],
            },
            new() { Id = 2, Lines = [], Address = new Address(), ByNumber = [] },
            new() { Id = 3 },
        ];
        await Writable<Order>().AppendAsync(rows);

        // Another context on the same storage, without a model: reading needs no declaration.
        var read = await Lake().Dataset<NarrowOrder>("data").ReadAsync().ToListAsync();

        read[0].Lines!.Select(l => l.Quantity).Should().Equal(2, null);
        read[0].Address!.Street!.Number.Should().Be(5);
        read[0].ByNumber![7].Quantity.Should().Be(9);
        read[0].Matrix!.Select(m => m.ToArray()).Should().BeEquivalentTo(new[] { new long[] { 1, 2 }, [], [3] }, o => o.WithStrictOrdering());
        read[1].Lines.Should().BeEmpty();
        read[1].Address!.Street.Should().BeNull();
        read[1].ByNumber.Should().BeEmpty();
        read[2].Lines.Should().BeNull();
        read[2].Address.Should().BeNull();
        read[2].Matrix.Should().BeNull();
    }

    [Test]
    public async Task Nested_timestamp_statistics_are_in_microseconds()
    {
        var t = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc);
        var result = await Writable<Order>().AppendAsync([new Order { Id = 1, Times = [t.AddTicks(10), t] }, new Order { Id = 2 }]);

        await using var reader = await OpenParquet(result.Written[0]);
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
        var write = () => Writable<Node>().AppendAsync([new Node()]);

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

    private static readonly DateOnly Day1 = new(2026, 10, 1);
    private static readonly DateOnly Day2 = new(2026, 10, 2);

    [Test]
    public async Task Partitions_are_hive_folders_and_read_back()
    {
        var rows = Sales(120);
        var dataset = Writable<Sale>(s => s.Date, s => s.Country);

        var result = await dataset.AppendAsync(rows);
        var read = await dataset.ReadAsync().ToListAsync();

        result.Written.Should().HaveCount(12);
        (await FilesOf()).Select(p => p[..p.LastIndexOf('/')]).Where(p => p.StartsWith("Date=2026-10-01/")).Should().BeEquivalentTo(
            "Date=2026-10-01/Country=PL", "Date=2026-10-01/Country=DE", "Date=2026-10-01/Country=a%2Fb c%25", "Date=2026-10-01/Country=__HIVE_DEFAULT_PARTITION__");
        read.OrderBy(r => r.Id).Should().BeEquivalentTo(rows, o => o.WithStrictOrdering());

        await using var reader = await OpenParquet(result.Written[0]);
        reader.Schema.DataFields.Select(f => f.Name).Should().Equal("Id", "Amount");
    }

    [Test]
    public async Task Partition_filters_skip_written_folders()
    {
        var dataset = Writable<Sale>(s => s.Country);
        await dataset.AppendAsync(Sales(120));
        var metrics = new ScanMetrics();

        var read = await dataset.Where(s => s.Country == "a/b c%").ReadAsync(metrics).ToListAsync();

        read.Should().HaveCount(30).And.OnlyContain(s => s.Country == "a/b c%");
        metrics.FilesSkippedByPartition.Should().Be(3);
    }

    [Test]
    public async Task Append_adds_files()
    {
        var dataset = Writable<Sale>();
        await dataset.AppendAsync(Sales(10));

        await dataset.AppendAsync(Sales(5, 10));

        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().BeEquivalentTo(Enumerable.Range(0, 15).Select(i => (long)i));
        (await FilesOf()).Should().HaveCount(2);
    }

    [Test]
    public async Task Overwrite_replaces_the_whole_dataset_with_every_partition()
    {
        var dataset = Writable<Sale>(s => s.Date);
        await dataset.AppendAsync(Sales(30));

        var result = await dataset.OverwriteAsync([new Sale { Id = 100, Date = Day1 }]);

        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().Equal(100);
        result.Removed.Should().HaveCount(3);
        (await FilesOf()).Should().ContainSingle().Which.Should().StartWith("Date=2026-10-01/");
    }

    [Test]
    public async Task Overwrite_with_no_rows_empties_the_dataset()
    {
        var dataset = Writable<Sale>();
        await dataset.AppendAsync(Sales(30));

        await dataset.OverwriteAsync(Array.Empty<Sale>());

        (await dataset.CountAsync()).Should().Be(0);
    }

    [Test]
    public async Task Overwrite_keeps_hidden_files()
    {
        var dataset = Writable<Sale>();
        await dataset.AppendAsync(Sales(30));
        await Storage.WriteAsync($"{Location("data")}/_SUCCESS", new MemoryStream());

        await dataset.OverwriteAsync(Sales(2, 100));

        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().Equal(100, 101);
        (await FilesOf()).Should().Contain("_SUCCESS", "only data files are replaced");
    }

    [Test]
    public async Task ReplacePartitions_replaces_only_the_partitions_written()
    {
        var dataset = Writable<Sale>(s => s.Date);
        await dataset.AppendAsync(Sales(30));

        var result = await dataset.ReplacePartitionsAsync([new Sale { Id = 1000, Date = Day2, Country = "PL", Amount = 1 }]);

        var read = await dataset.ReadAsync().ToListAsync();
        result.Removed.Should().ContainSingle().Which.RelativePath.Should().StartWith("Date=2026-10-02/");
        read.Where(s => s.Date == Day2).Select(s => s.Id).Should().Equal(1000);
        read.Count(s => s.Date != Day2).Should().Be(20);
    }

    [Test]
    public async Task ReplacePartitions_with_no_rows_changes_nothing()
    {
        var dataset = Writable<Sale>(s => s.Date);
        await dataset.AppendAsync(Sales(30));

        await dataset.ReplacePartitionsAsync(Array.Empty<Sale>());

        (await dataset.CountAsync()).Should().Be(30);
    }

    [Test]
    public async Task ReplacePartitions_needs_partitions()
    {
        var replace = () => Writable<Sale>().ReplacePartitionsAsync(Sales(1));

        (await replace.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*'data' is not partitioned*OverwriteAsync*");
    }

    [Test]
    public async Task Overwrite_with_a_filter_replaces_what_it_matches()
    {
        var dataset = Writable<Sale>(s => s.Date, s => s.Country);
        await dataset.AppendAsync(Sales(120));

        var result = await dataset.OverwriteAsync([new Sale { Id = 1000, Date = Day1, Country = "PL" }], where: s => s.Date == Day1 && s.Country == "PL");

        var read = await dataset.ReadAsync().ToListAsync();
        result.Removed.Should().ContainSingle();
        read.Where(s => s.Date == Day1 && s.Country == "PL").Select(s => s.Id).Should().Equal(1000);
        read.Should().HaveCount(120 - 10 + 1);
    }

    [Test]
    public async Task Overwrite_with_a_filter_refuses_rows_outside_it_and_leaves_nothing_behind()
    {
        var dataset = Writable<Sale>(o => o.TargetFileSize(1), s => s.Date);
        await dataset.AppendAsync(Sales(30));
        var before = await FilesOf();

        var overwrite = () => dataset.OverwriteAsync([new Sale { Id = 1, Date = Day1 }, new Sale { Id = 2, Date = Day1 }, new Sale { Id = 3, Date = Day2 }],
            where: s => s.Date == Day1);

        (await overwrite.Should().ThrowAsync<InvalidDataException>()).WithMessage("*does not match the filter*");
        (await FilesOf()).Should().Equal(before);
    }

    [TestCase("row")]
    [TestCase("opaque")]
    public async Task Overwrite_with_a_filter_on_rows_is_not_supported_on_files(string kind)
    {
        var dataset = Writable<Sale>(s => s.Date);
        await dataset.AppendAsync(Sales(30));
        Expression<Func<Sale, bool>> where = kind == "row" ? s => s.Date == Day1 && s.Amount > 5 : s => s.Country!.StartsWith("P");

        var delete = () => dataset.DeleteAsync(where);

        (await delete.Should().ThrowAsync<NotSupportedException>()).WithMessage("*deletes whole files*partition columns (Date)*");
        (await dataset.CountAsync()).Should().Be(30);
    }

    [Test]
    public async Task Delete_removes_the_partitions_a_filter_matches()
    {
        var dataset = Writable<Sale>(s => s.Date);
        await dataset.AppendAsync(Sales(30));

        var result = await dataset.DeleteAsync(s => s.Date != Day2);

        result.Rows.Should().Be(0);
        result.Removed.Should().HaveCount(2);
        (await dataset.ReadAsync().ToListAsync()).Should().OnlyContain(s => s.Date == Day2).And.HaveCount(10);
    }

    [Test]
    public async Task Folders_written_by_Spark_in_another_case_are_the_same_partitions()
    {
        var spark = Writable<Sale>(s => s.Date);
        var file = (await spark.AppendAsync([new Sale { Id = 1, Date = Day1 }])).Written[0];
        var bytes = new byte[file.Length];
        await Storage.ReadAsync(file, 0, bytes);
        await Storage.WriteAsync($"{Location("lower")}/date=2026-10-01/part-00000.parquet", new MemoryStream(bytes));
        var dataset = Lake(m => m.Dataset<Sale>("lower").PartitionBy(s => s.Date)).Dataset<Sale>("lower");

        var result = await dataset.ReplacePartitionsAsync([new Sale { Id = 2, Date = Day1 }]);

        result.Removed.Should().ContainSingle().Which.RelativePath.Should().Be("date=2026-10-01/part-00000.parquet");
        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().Equal(2);
    }

    [Test]
    public async Task Files_partitioned_differently_than_declared_stop_the_write()
    {
        await Writable<Sale>(s => s.Date).AppendAsync(Sales(3));
        var unpartitioned = Lake(m => m.Dataset<Sale>("data")).Dataset<Sale>("data");

        var overwrite = () => unpartitioned.OverwriteAsync(Sales(1));

        (await overwrite.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*partitioned by (Date)*declared with ()*");
        (await unpartitioned.CountAsync()).Should().Be(3);
    }

    [Test]
    public async Task Datasets_not_in_the_model_are_read_only()
    {
        var lake = Lake(m => m.Dataset<Sale>("sales"));

        var write = () => lake.Dataset<Sale>("salse").AppendAsync(Sales(1));

        (await write.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*'salse' is not declared*model.Dataset<Sale>(\"salse\")*");
        (await FilesOf("salse")).Should().BeEmpty();
    }

    [Test]
    public void A_declared_dataset_has_one_row_type()
    {
        var lake = Lake(m => m.Dataset<Sale>("sales"));

        FluentActions.Invoking(() => lake.Dataset<Order>("sales")).Should().Throw<InvalidOperationException>().WithMessage("*declared*for Sale, not Order*");
    }

    [Test]
    public async Task A_dataset_that_does_not_exist_reads_as_empty()
    {
        var dataset = Lake().Dataset<Sale>("nothing-here");

        (await dataset.ReadAsync().ToListAsync()).Should().BeEmpty();
        (await dataset.CountAsync()).Should().Be(0);
    }

    [Test]
    public async Task Count_comes_from_footers_or_from_a_filtered_read()
    {
        var dataset = Writable<Sale>(o => o.RowGroupSize(7), s => s.Date);
        await dataset.AppendAsync(Sales(100));

        (await dataset.CountAsync()).Should().Be(100);
        (await dataset.Where(s => s.Date == Day1 && s.Amount >= 50).CountAsync()).Should().Be(17);
    }

    [Test]
    public async Task Large_writes_roll_over_to_new_files_and_row_groups()
    {
        var rows = Enumerable.Range(0, 20_000).Select(i => new Sale { Id = i, Date = Day1, Country = $"country-{i}", Amount = i }).ToList();
        var dataset = Writable<Sale>(o => o.RowGroupSize(1000).TargetFileSize(50_000).Compression(ParquetCompression.Zstd));

        var result = await dataset.AppendAsync(rows);

        result.Written.Should().HaveCountGreaterThan(2).And.OnlyContain(f => f.RelativePath.EndsWith(".zstd.parquet"));
        var metrics = new ScanMetrics();
        (await dataset.ReadAsync(metrics).CountAsync()).Should().Be(20_000);
        metrics.RowGroups.Should().Be(20);
    }

    [TestCase(ParquetCompression.Snappy)]
    [TestCase(ParquetCompression.Zstd)]
    [TestCase(ParquetCompression.Gzip)]
    [TestCase(ParquetCompression.None)]
    public async Task Every_compression_reads_back(ParquetCompression compression)
    {
        var dataset = Writable<Sale>(o => o.Compression(compression));
        await dataset.AppendAsync(Sales(50));

        (await dataset.ReadAsync().CountAsync()).Should().Be(50);
    }

    [Test]
    public async Task Values_that_would_be_changed_are_errors_and_leave_nothing_behind()
    {
        var dataset = Writable<Everything>();
        var unspecified = () => dataset.AppendAsync([new Everything { CreatedUtc = new DateTime(2026, 1, 1) }]);
        var tooPrecise = () => dataset.AppendAsync([new Everything { Amount = 1.234m }]);
        var tooLarge = () => dataset.AppendAsync([new Everything { Amount = 123_456_789m }]);

        (await unspecified.Should().ThrowAsync<InvalidDataException>()).WithMessage("*CreatedUtc*Kind.Unspecified*UnzonedDateTime*");
        (await tooPrecise.Should().ThrowAsync<InvalidDataException>()).WithMessage("*Amount*1.234*more than 2 digits*");
        (await tooLarge.Should().ThrowAsync<InvalidDataException>()).WithMessage("*Amount*does not fit decimal(10, 2)*");
        (await FilesOf()).Should().BeEmpty();
    }

    [Test]
    public async Task A_failed_write_removes_the_files_it_already_wrote()
    {
        var dataset = Writable<Sale>(o => o.RowGroupSize(10).TargetFileSize(1));

        var append = () => dataset.AppendAsync(Failing());

        await append.Should().ThrowAsync<TimeoutException>();
        (await FilesOf()).Should().BeEmpty();

        static async IAsyncEnumerable<Sale> Failing()
        {
            foreach (var sale in Sales(35)) yield return sale;
            await Task.Yield();
            throw new TimeoutException("source failed");
        }
    }

    [Test]
    public void Partition_properties_are_checked_when_the_dataset_is_first_used()
    {
        var notAProperty = Lake(m => m.Dataset<Sale>("data").PartitionBy(s => s.Amount + 1));
        var wrongType = Lake(m => m.Dataset<Sale>("data").PartitionBy(s => s.Amount));

        FluentActions.Invoking(() => notAProperty.Dataset<Sale>("data")).Should().Throw<ArgumentException>().WithMessage("*PartitionBy takes properties*");
        FluentActions.Invoking(() => wrongType.Dataset<Sale>("data")).Should().Throw<NotSupportedException>().WithMessage("*Amount is Decimal*");
    }

    [Test]
    public void The_in_memory_provider_writes_no_temporary_files()
    {
        if (provider != "memory") Assert.Ignore("local disk uses temporary files on purpose");
        var before = Directory.GetFiles(Path.GetTempPath(), "datalake-*.parquet").Length;

        Writable<Sale>(s => s.Date).AppendAsync(Sales(100)).GetAwaiter().GetResult();

        Directory.GetFiles(Path.GetTempPath(), "datalake-*.parquet").Length.Should().Be(before);
        _memory.Locations.Should().HaveCount(3).And.OnlyContain(l => l.StartsWith("memory://lake/data/Date="));
    }

    // Declared so that the order by value (Us, Eu) differs from the order by name (Eu, Us).
    public enum Region { Us = 0, Eu = 1 }

    public sealed class Visit
    {
        public long Id { get; set; }
        public Region Region { get; set; }
        public string? Country { get; set; }
    }

    [Test]
    public async Task Enum_partitions_can_be_deleted_and_overwritten_by_filter()
    {
        var dataset = Writable<Visit>(v => v.Region);
        await dataset.AppendAsync([new Visit { Id = 1, Region = Region.Us }, new Visit { Id = 2, Region = Region.Eu }]);

        await dataset.OverwriteAsync([new Visit { Id = 3, Region = Region.Eu }], where: v => v.Region == Region.Eu);
        (await dataset.ReadAsync().OrderBy(v => v.Id).Select(v => v.Id).ToListAsync()).Should().Equal(1, 3);

        await dataset.DeleteAsync(v => v.Region == Region.Us);
        (await dataset.ReadAsync().Select(v => v.Id).ToListAsync()).Should().Equal(3);
    }

    [TestCase("equals")]
    [TestCase("contains")]
    public async Task Enum_filters_do_not_skip_row_groups_by_statistics_ordered_by_name(string kind)
    {
        var dataset = Writable<Visit>();
        await dataset.AppendAsync([new Visit { Id = 1, Region = Region.Eu }, new Visit { Id = 2, Region = Region.Us }]);
        Region[] wanted = [Region.Us];
        Expression<Func<Visit, bool>> filter = kind == "equals" ? v => v.Region == Region.Us : v => wanted.Contains(v.Region);

        var read = await dataset.Where(filter).ReadAsync().ToListAsync();

        read.Select(v => v.Id).Should().Equal(2);
    }

    [Test]
    public async Task An_empty_string_partition_is_null_for_filters_too()
    {
        var dataset = Writable<Visit>(v => v.Country);
        await dataset.AppendAsync([new Visit { Id = 1, Country = "" }, new Visit { Id = 2, Country = "PL" }]);

        var byEmpty = () => dataset.OverwriteAsync([new Visit { Id = 3, Country = "" }], where: v => v.Country == "");
        (await byEmpty.Should().ThrowAsync<NotSupportedException>()).WithMessage("*Hive's default partition*Compare with null*");

        await dataset.OverwriteAsync([new Visit { Id = 3, Country = "" }], where: v => v.Country == null);
        (await dataset.ReadAsync().OrderBy(v => v.Id).Select(v => v.Id).ToListAsync()).Should().Equal(2, 3);
    }

    [Test]
    public async Task When_the_first_old_file_can_not_be_removed_nothing_changes()
    {
        var storage = new FlakyDeletes(new MemoryStorage(), failAfter: 0);
        var dataset = LakeContext.Create(o => o.UseParquet("memory://lake", storage), m => m.Dataset<Sale>("data").PartitionBy(s => s.Date)).Dataset<Sale>("data");
        await dataset.AppendAsync(Sales(30));
        storage.Arm();

        var overwrite = () => dataset.OverwriteAsync(Sales(3, 100));

        (await overwrite.Should().ThrowAsync<IOException>()).WithMessage("*new files were removed again, nothing changed*");
        (await dataset.ReadAsync().Select(s => s.Id).ToListAsync()).Should().BeEquivalentTo(Enumerable.Range(0, 30).Select(i => (long)i));
    }

    [Test]
    public async Task When_a_later_old_file_can_not_be_removed_the_error_lists_it()
    {
        var storage = new FlakyDeletes(new MemoryStorage(), failAfter: 1);
        var dataset = LakeContext.Create(o => o.UseParquet("memory://lake", storage), m => m.Dataset<Sale>("data").PartitionBy(s => s.Date)).Dataset<Sale>("data");
        await dataset.AppendAsync(Sales(30));
        storage.Arm();

        var overwrite = () => dataset.OverwriteAsync(Sales(3, 100));

        (await overwrite.Should().ThrowAsync<IOException>()).WithMessage("*has the new data, but 2 old file(s) could not be removed*Date=2026-10-0*");
    }

    private sealed class FlakyDeletes(MemoryStorage inner, int failAfter) : ILakeStorage
    {
        private HashSet<string> _old = [];
        private int _deletes;

        // From now on, deleting the files that exist now fails after the first failAfter; new files delete fine.
        public void Arm() => _old = inner.Locations.ToHashSet();
        public bool CanHandle(string location) => inner.CanHandle(location);
        public IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken ct = default) => inner.ListAsync(location, ct);
        public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken ct = default) => inner.ReadAsync(file, offset, buffer, ct);
        public ValueTask<LakeFile> WriteAsync(string location, Stream content, WriteMode mode = WriteMode.Overwrite, CancellationToken ct = default) =>
            inner.WriteAsync(location, content, mode, ct);

        public ValueTask DeleteAsync(string location, CancellationToken ct = default)
        {
            if (_old.Contains(location) && _deletes++ >= failAfter) throw new IOException("storage is down");
            return inner.DeleteAsync(location, ct);
        }
    }

    private static long Micros(DateTime utc) => (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10;
}
