using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using SolTechnology.DataLake.Arrow;
using SolTechnology.Time;
using ArrowTimeUnit = Apache.Arrow.Types.TimeUnit;

namespace SolTechnology.DataLake.UnitTests;

public class ArrowConvertTests
{
    public enum Status { New, Paid }

    public sealed class Line
    {
        public string Sku { get; set; } = "";
        public int? Quantity { get; set; }
    }

    public sealed class Order
    {
        public long Id { get; set; }
        public int? Quantity { get; set; }
        public double Ratio { get; set; }
        public bool Paid { get; set; }
        [DecimalPrecision(12, 2)] public decimal Amount { get; set; }
        public decimal? Precise { get; set; }
        public string Name { get; set; } = "";
        public string? Note { get; set; }
        public byte[]? Payload { get; set; }
        public Guid Key { get; set; }
        public Status State { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly At { get; set; }
        public TimeSpan Took { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTimeOffset? Seen { get; set; }
        public DateTimeZone Zoned { get; set; }
        public UnzonedDateTime Local { get; set; }
        public List<Line>? Lines { get; set; }
        public int[] Scores { get; set; } = [];
        public Dictionary<string, int?>? Attributes { get; set; }
        public Line? Main { get; set; }
    }

    private static Order Sample(int i) => new()
    {
        Id = i,
        Quantity = i % 3 == 0 ? null : i,
        Ratio = i / 4.0,
        Paid = i % 2 == 0,
        Amount = i * 1.25m,
        Precise = i % 2 == 0 ? null : 12345.6789m,
        Name = $"name-{i}",
        Note = i % 4 == 0 ? null : "ż",
        Payload = i % 2 == 0 ? null : [1, 2, (byte)i],
        Key = Guid.Parse($"6b1f2a3c-0000-4000-8000-{i:000000000000}"),
        State = (Status)(i % 2),
        Day = new DateOnly(2026, 10, 1).AddDays(i),
        At = new TimeOnly(10, 0).Add(TimeSpan.FromTicks(i * 10)),
        Took = TimeSpan.FromMilliseconds(i * 1.5),
        CreatedUtc = new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc).AddTicks(i * 10),
        Seen = i % 2 == 0 ? null : new DateTimeOffset(2026, 10, 1, 10, 0, 0, TimeSpan.FromHours(2)),
        Zoned = DateTimeZone.FromUtc(new DateTime(2026, 10, 1, 6, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Utc),
        Local = new UnzonedDateTime(new DateOnly(2026, 10, 1), new TimeOnly(9, i % 60)),
        Lines = i % 3 == 0 ? null : [new Line { Sku = "a", Quantity = i }, new Line { Sku = "b" }],
        Scores = Enumerable.Range(0, i % 4).ToArray(),
        Attributes = i % 2 == 0 ? null : new() { ["x"] = i, ["y"] = null },
        Main = i % 5 == 0 ? null : new Line { Sku = $"main-{i}", Quantity = 1 },
    };

    private static void ShouldMatch(IEnumerable<Order> actual, IEnumerable<Order> expected) =>
        actual.Should().BeEquivalentTo(expected, o => o.WithStrictOrdering()
            .Using<DateTimeOffset>(c => c.Subject.Should().Be(c.Expectation)).WhenTypeIs<DateTimeOffset>());

    [Test]
    public void A_record_batch_round_trips()
    {
        var rows = Enumerable.Range(0, 50).Select(Sample).ToList();

        using var batch = ArrowConvert.Serialize(rows);
        var read = ArrowConvert.Deserialize<Order>(batch);

        batch.Length.Should().Be(50);
        ShouldMatch(read, rows);
    }

    [Test]
    public void The_schema_uses_the_types_other_engines_expect()
    {
        var schema = ArrowConvert.SchemaOf<Order>();

        IArrowType Type(string name) => schema.GetFieldByName(name).DataType;
        Type("Id").Should().BeOfType<Int64Type>();
        schema.GetFieldByName("Id").IsNullable.Should().BeFalse();
        schema.GetFieldByName("Quantity").IsNullable.Should().BeTrue();
        Type("Amount").Should().BeOfType<Decimal128Type>().Which.Precision.Should().Be(12);
        Type("Key").Should().BeOfType<GuidType>();
        Type("State").Should().BeOfType<StringType>();
        Type("At").Should().BeOfType<Time64Type>().Which.Unit.Should().Be(ArrowTimeUnit.Microsecond);
        Type("Took").Should().BeOfType<DurationType>();
        Type("CreatedUtc").Should().BeOfType<TimestampType>().Which.Timezone.Should().Be("UTC");
        Type("Local").Should().BeOfType<TimestampType>().Which.Timezone.Should().BeNull();
        Type("Lines").Should().BeOfType<ListType>().Which.ValueDataType.Should().BeOfType<StructType>();
        Type("Attributes").Should().BeOfType<MapType>();
    }

    public sealed class Narrow
    {
        public long Id { get; set; }
        public string? Category { get; set; }
        public string? Big { get; set; }
        public DateTimeZone When { get; set; }
        public UnzonedDateTime Wall { get; set; }
        public decimal Value { get; set; }
        public DateOnly? Day { get; set; }
    }

    [Test]
    public void Reads_what_other_writers_produce()
    {
        // int32 into long, dictionary-encoded strings, large strings, timestamps in seconds with a zone and in nanoseconds
        // without, decimal256, date64: what ADBC drivers and pyarrow hand out.
        var ids = new Int32Array.Builder().Append(1).Append(2).Build();
        var dictionary = new DictionaryArray(new DictionaryType(Int8Type.Default, StringType.Default, false),
            new Int8Array.Builder().Append(1).Append(0).Build(), new StringArray.Builder().Append("books").Append("toys").Build());
        var big = new LargeStringArray.Builder().Append("a").AppendNull().Build();
        var when = new TimestampArray.Builder(new TimestampType(ArrowTimeUnit.Second, "Europe/Warsaw"))
            .Append(new DateTimeOffset(2026, 10, 1, 8, 0, 0, TimeSpan.Zero)).Append(new DateTimeOffset(2026, 10, 2, 8, 0, 0, TimeSpan.Zero)).Build();
        var wall = new TimestampArray.Builder(new TimestampType(ArrowTimeUnit.Nanosecond, (string?)null))
            .Append(new DateTimeOffset(2026, 10, 1, 9, 30, 0, TimeSpan.Zero)).Append(new DateTimeOffset(2026, 10, 1, 9, 31, 0, TimeSpan.Zero)).Build();
        var value = new Decimal256Array.Builder(new Decimal256Type(40, 3)).Append(1.5m).Append(-2.25m).Build();
        var day = new Date64Array.Builder().Append(new DateOnly(2026, 1, 2)).AppendNull().Build();
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("category").DataType(dictionary.Data.DataType))
            .Field(f => f.Name("big").DataType(LargeStringType.Default))
            .Field(f => f.Name("when").DataType(when.Data.DataType))
            .Field(f => f.Name("wall").DataType(wall.Data.DataType))
            .Field(f => f.Name("value").DataType(value.Data.DataType))
            .Field(f => f.Name("day").DataType(Date64Type.Default))
            .Build();
        using var batch = new RecordBatch(schema, [ids, dictionary, big, when, wall, value, day], 2);

        var rows = ArrowConvert.Deserialize<Narrow>(batch);

        rows[0].Should().BeEquivalentTo(new Narrow
        {
            Id = 1, Category = "toys", Big = "a",
            When = DateTimeZone.FromUtc(new DateTime(2026, 10, 1, 8, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Utc),
            Wall = new UnzonedDateTime(new DateOnly(2026, 10, 1), new TimeOnly(9, 30)),
            Value = 1.5m, Day = new DateOnly(2026, 1, 2),
        });
        rows[1].Category.Should().Be("books");
        rows[1].Big.Should().BeNull();
        rows[1].Value.Should().Be(-2.25m);
        rows[1].Day.Should().BeNull();
    }

    public sealed class WrongTypes
    {
        public int Id { get; set; }
    }

    public sealed class Required
    {
        public long Id { get; set; }
        public long Missing { get; set; }
    }

    [Test]
    public void Lossy_or_missing_columns_are_errors()
    {
        using var batch = ArrowConvert.Serialize([new Narrow { Id = 1 }]);

        FluentActions.Invoking(() => ArrowConvert.Deserialize<WrongTypes>(batch))
            .Should().Throw<NotSupportedException>().WithMessage("*'Id' of type Int64 can't be read into Int32*");
        FluentActions.Invoking(() => ArrowConvert.Deserialize<Required>(batch))
            .Should().Throw<InvalidDataException>().WithMessage("*no column 'Missing'*");
    }

    [Test]
    public void Column_naming_applies_both_ways()
    {
        var options = new ArrowOptions { ColumnNaming = System.Text.Json.JsonNamingPolicy.SnakeCaseLower };

        using var batch = ArrowConvert.Serialize([new Line { Sku = "a", Quantity = 2 }], options);

        batch.Schema.FieldsList.Select(f => f.Name).Should().Equal("sku", "quantity");
        ArrowConvert.Deserialize<Line>(batch, options).Single().Should().BeEquivalentTo(new Line { Sku = "a", Quantity = 2 });
    }

    [TestCase(ArrowIpcFormat.File)]
    [TestCase(ArrowIpcFormat.Stream)]
    public async Task Ipc_round_trips_in_batches(ArrowIpcFormat format)
    {
        var rows = Enumerable.Range(0, 250).Select(Sample).ToList();
        using var stream = new MemoryStream();

        await ArrowConvert.WriteIpcAsync(stream, rows, format, new ArrowOptions { BatchSize = 100 });
        stream.Position = 0;
        var read = await ArrowConvert.ReadIpcAsync<Order>(stream).ToListAsync();

        ShouldMatch(read, rows);
        stream.Position = 0;
        using var reader = format == ArrowIpcFormat.File ? new ArrowFileReader(stream, leaveOpen: true) : new ArrowStreamReader(stream, leaveOpen: true);
        var batches = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            batch.Dispose();
            batches++;
        }
        batches.Should().Be(3);
    }

    [TestCase(ArrowIpcFormat.File)]
    [TestCase(ArrowIpcFormat.Stream)]
    public async Task Ipc_reads_from_a_stream_that_cannot_seek(ArrowIpcFormat format)
    {
        var rows = Enumerable.Range(0, 10).Select(Sample).ToList();
        using var stream = new MemoryStream();
        await ArrowConvert.WriteIpcAsync(stream, rows, format);

        var read = await ArrowConvert.ReadIpcAsync<Order>(new ForwardOnlyStream(stream.ToArray())).ToListAsync();

        ShouldMatch(read, rows);
    }

    public sealed class Sale
    {
        public long Id { get; set; }
        public string Country { get; set; } = "";
        public decimal Amount { get; set; }
    }

    [Test]
    public async Task Arrow_files_are_read_as_a_dataset_with_partitions()
    {
        using var lake = new TempDataset();
        foreach (var country in new[] { "PL", "DE" })
        {
            Directory.CreateDirectory(lake.PathOf($"country={country}"));
            await using var file = File.Create(lake.PathOf($"country={country}/part-0.arrow"));
            await ArrowConvert.WriteIpcAsync(file, Enumerable.Range(0, 5).Select(i => new { Id = (long)i, Amount = i * 2m }).Select(x => new IdAmount { Id = x.Id, Amount = x.Amount }));
        }
        var metrics = new ScanMetrics();

        var rows = await Dataset.Open<Sale>(lake.Root).Where(s => s.Country == "PL" && s.Amount > 4).ReadAsync(metrics).ToListAsync();

        rows.Select(r => (r.Id, r.Country)).Should().Equal((3, "PL"), (4, "PL"));
        metrics.FilesSkippedByPartition.Should().Be(1);
    }

    public sealed class IdAmount
    {
        public long Id { get; set; }
        public decimal Amount { get; set; }
    }
}
