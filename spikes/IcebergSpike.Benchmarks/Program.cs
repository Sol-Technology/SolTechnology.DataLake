using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Parquet;
using Parquet.Schema;

if (args is ["verify"])
{
    var b = new ParquetToClasses();
    await b.WriteFile();
    var columns = await b.Columns();
    var arrow = await b.ThroughArrow();
    var serializer = await b.ParquetSerializer();
    static string Row(OrderRow o) => $"{o.Id}|{o.Country}|{o.Amount?.ToString("0.############################", System.Globalization.CultureInfo.InvariantCulture)}|{o.CreatedAt.Ticks}|{o.Score}";
    Console.WriteLine($"rows: {columns.Count} {arrow.Count} {serializer.Count}");
    Console.WriteLine($"same as Columns: arrow={columns.Select(Row).SequenceEqual(arrow.Select(Row))} serializer={columns.Select(Row).SequenceEqual(serializer.Select(Row))}");
    var diff = Enumerable.Range(0, columns.Count).FirstOrDefault(i => Row(columns[i]) != Row(arrow[i]), -1);
    if (diff >= 0) Console.WriteLine($"first difference at {diff}: {Row(columns[diff])} / {Row(arrow[diff])}");
    return;
}

BenchmarkRunner.Run<ParquetToClasses>(args: args);

// D2 (DataLakePlan.md): which way from Parquet to C# classes.
//  Columns         - Parquet.Net column arrays, then straight into the classes (the column layer we chose)
//  ThroughArrow    - the same column arrays copied into an Arrow RecordBatch, then into the classes
//  ParquetSerializer - Parquet.Net's own class mapper, as a baseline
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ParquetToClasses
{
    private const int RowGroups = 10;
    private const int RowsPerGroup = 100_000;

    private static readonly DataField<long> Id = new("Id");
    private static readonly DataField<string> Country = new("Country");
    private static readonly DataField<decimal?> Amount = new("Amount");
    private static readonly DataField<DateTime> CreatedAt = new("CreatedAt");
    private static readonly DataField<double?> Score = new("Score");

    private static readonly Schema ArrowSchema = new Schema.Builder()
        .Field(f => f.Name("Id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("Country").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("Amount").DataType(new Decimal128Type(38, 18)).Nullable(true))
        .Field(f => f.Name("CreatedAt").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")).Nullable(false))
        .Field(f => f.Name("Score").DataType(DoubleType.Default).Nullable(true))
        .Build();

    private byte[] _file = [];

    [GlobalSetup]
    public async Task WriteFile()
    {
        var countries = new[] { "PL", "DE", "US", "FR", "GB" };
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(Id, Country, Amount, CreatedAt, Score), stream))
        {
            for (var g = 0; g < RowGroups; g++)
            {
                var first = (long)g * RowsPerGroup;
                using var group = writer.CreateRowGroup();
                await group.WriteAsync(Id, (ReadOnlyMemory<long>)Enumerable.Range(0, RowsPerGroup).Select(i => first + i).ToArray());
                await group.WriteAsync(Country, Enumerable.Range(0, RowsPerGroup).Select(i => countries[i % countries.Length]).ToArray());
                await group.WriteAsync(Amount, (ReadOnlyMemory<decimal?>)Enumerable.Range(0, RowsPerGroup).Select(i => i % 10 == 0 ? (decimal?)null : (first + i) / 100m).ToArray());
                await group.WriteAsync(CreatedAt, (ReadOnlyMemory<DateTime>)Enumerable.Range(0, RowsPerGroup).Select(i => start.AddSeconds(first + i)).ToArray());
                await group.WriteAsync(Score, (ReadOnlyMemory<double?>)Enumerable.Range(0, RowsPerGroup).Select(i => i % 7 == 0 ? (double?)null : i / 8.0).ToArray());
            }
        }
        _file = stream.ToArray();
    }

    [Benchmark(Baseline = true)]
    public async Task<List<OrderRow>> Columns()
    {
        var result = new List<OrderRow>(RowGroups * RowsPerGroup);
        await using var reader = await ParquetReader.CreateAsync(new MemoryStream(_file));
        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            var c = await ReadGroup(group);
            for (var i = 0; i < c.Count; i++)
                result.Add(new OrderRow { Id = c.Ids[i], Country = c.Countries[i]!, Amount = c.Amounts[i], CreatedAt = c.Created[i], Score = c.Scores[i] });
        }
        return result;
    }

    [Benchmark]
    public async Task<List<OrderRow>> ThroughArrow()
    {
        var result = new List<OrderRow>(RowGroups * RowsPerGroup);
        await using var reader = await ParquetReader.CreateAsync(new MemoryStream(_file));
        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            using var batch = ToArrow(await ReadGroup(group));

            var ids = (Int64Array)batch.Column(0);
            var countries = (StringArray)batch.Column(1);
            var amounts = (Decimal128Array)batch.Column(2);
            var created = (TimestampArray)batch.Column(3);
            var scores = (DoubleArray)batch.Column(4);
            for (var i = 0; i < batch.Length; i++)
                result.Add(new OrderRow
                {
                    Id = ids.GetValue(i)!.Value,
                    Country = countries.GetString(i),
                    Amount = amounts.GetValue(i),
                    CreatedAt = created.GetTimestamp(i)!.Value.UtcDateTime,
                    Score = scores.GetValue(i),
                });
        }
        return result;
    }

    [Benchmark]
    public async Task<IList<OrderRow>> ParquetSerializer() =>
        (await Parquet.Serialization.ParquetSerializer.DeserializeAsync<OrderRow>(new MemoryStream(_file))).Data;

    private static async Task<GroupColumns> ReadGroup(ParquetRowGroupReader group)
    {
        var n = (int)group.RowCount;
        var c = new GroupColumns(n, new long[n], new string?[n], new decimal?[n], new DateTime[n], new double?[n]);
        await group.ReadAsync(Id, c.Ids.AsMemory());
        await group.ReadAsync(Country, c.Countries.AsMemory());
        await group.ReadAsync(Amount, c.Amounts.AsMemory());
        await group.ReadAsync(CreatedAt, c.Created.AsMemory());
        await group.ReadAsync(Score, c.Scores.AsMemory());
        return c;
    }

    private static RecordBatch ToArrow(GroupColumns c)
    {
        var ids = new Int64Array.Builder().Reserve(c.Count).AppendRange(c.Ids).Build();
        var countries = new StringArray.Builder();
        foreach (var country in c.Countries) countries.Append(country);
        var amounts = new Decimal128Array.Builder(new Decimal128Type(38, 18));
        foreach (var amount in c.Amounts)
            if (amount is { } a) amounts.Append(a); else amounts.AppendNull();
        var created = new TimestampArray.Builder(TimeUnit.Microsecond, "UTC");
        foreach (var t in c.Created) created.Append(new DateTimeOffset(DateTime.SpecifyKind(t, DateTimeKind.Utc)));
        var scores = new DoubleArray.Builder();
        foreach (var score in c.Scores)
            if (score is { } s) scores.Append(s); else scores.AppendNull();

        return new RecordBatch(ArrowSchema, [ids, countries.Build(), amounts.Build(), created.Build(), scores.Build()], c.Count);
    }

    private sealed record GroupColumns(int Count, long[] Ids, string?[] Countries, decimal?[] Amounts, DateTime[] Created, double?[] Scores);
}

public sealed class OrderRow
{
    public long Id { get; set; }
    public string Country { get; set; } = "";
    public decimal? Amount { get; set; }
    public DateTime CreatedAt { get; set; }
    public double? Score { get; set; }
}
