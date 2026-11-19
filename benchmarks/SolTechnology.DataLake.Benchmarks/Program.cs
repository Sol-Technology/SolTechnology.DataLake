using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Parquet;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Serialization.Attributes;
using SolTechnology.DataLake;

if (args is ["verify"])
{
    var b = new DatasetBenchmarks();
    await b.WriteDataset();
    try
    {
        foreach (var (name, lake, manual) in new (string, Func<Task<List<Sale>>>, Func<Task<List<Sale>>>)[]
                 {
                     ("all rows", b.DataLake_all, b.ParquetNet_all),
                     ("one day, one country", b.DataLake_one_partition, b.ParquetNet_one_partition),
                     ("id range", b.DataLake_id_range, b.ParquetNet_id_range),
                 })
        {
            var l = await lake();
            var m = await manual();
            Console.WriteLine($"{name}: {l.Count} rows, same as Parquet.Net: {l.Select(Row).SequenceEqual(m.Select(Row))}");
        }
    }
    finally
    {
        b.DeleteDataset();
    }
    return;
}

BenchmarkSwitcher.FromTypes([typeof(DatasetBenchmarks), typeof(NestedBenchmarks)]).Run(args);

static string Row(Sale s) => $"{s.Id}|{s.Amount}|{s.Product}|{s.Date}|{s.Country}";

/// <summary>
/// 90 files (30 days x 3 countries), 10 000 rows each in 2 row groups: 900 000 rows. DataLake against what people
/// do today with Parquet.Net: list the files, deserialize each with ParquetSerializer, filter in memory.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class DatasetBenchmarks
{
    private const int Days = 30;
    private const int RowsPerFile = 10_000;
    private static readonly string[] Countries = ["DE", "PL", "US"];
    private static readonly DateOnly First = new(2026, 10, 1);

    private string _root = "";

    [GlobalSetup]
    public async Task WriteDataset()
    {
        _root = Path.Combine(Path.GetTempPath(), "datalake-bench", Guid.NewGuid().ToString("N"));
        var id = new DataField<long>("Id");
        var amount = new DataField<decimal?>("Amount");
        var product = new DataField<string>("Product");
        var next = 0L;
        for (var d = 0; d < Days; d++)
        foreach (var country in Countries)
        {
            var path = Path.Combine(_root, $"Date={First.AddDays(d):yyyy-MM-dd}", $"Country={country}", "part-00000.parquet");
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await using var stream = File.Create(path);
            await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(id, amount, product), stream);
            for (var g = 0; g < 2; g++)
            {
                var ids = Enumerable.Range(0, RowsPerFile / 2).Select(_ => next++).ToArray();
                using var group = writer.CreateRowGroup();
                await group.WriteAsync(id, (ReadOnlyMemory<long>)ids);
                await group.WriteAsync(amount, (ReadOnlyMemory<decimal?>)ids.Select(i => i % 10 == 0 ? (decimal?)null : i / 100m).ToArray());
                await group.WriteAsync(product, ids.Select(i => $"product-{i % 50}").ToArray());
            }
        }
    }

    [GlobalCleanup]
    public void DeleteDataset() => Directory.Delete(_root, recursive: true);

    private static readonly DateOnly Day = First.AddDays(17);
    private const long From = 400_000;
    private const long To = 405_000;

    [Benchmark]
    public Task<List<Sale>> DataLake_all() => DataLake.Open<Sale>(_root).ReadAsync().ToListAsync().AsTask();

    [Benchmark]
    public Task<List<Sale>> ParquetNet_all() => Manual(_ => true);

    [Benchmark]
    public Task<List<Sale>> DataLake_one_partition() =>
        DataLake.Open<Sale>(_root).Where(s => s.Date == Day && s.Country == "PL").ReadAsync().ToListAsync().AsTask();

    [Benchmark]
    public Task<List<Sale>> ParquetNet_one_partition() => Manual(s => s.Date == Day && s.Country == "PL");

    [Benchmark]
    public Task<List<Sale>> DataLake_id_range() =>
        DataLake.Open<Sale>(_root).Where(s => s.Id >= From && s.Id < To).ReadAsync().ToListAsync().AsTask();

    [Benchmark]
    public Task<List<Sale>> ParquetNet_id_range() => Manual(s => s.Id >= From && s.Id < To);

    // List the files, read each with ParquetSerializer, set the partition values from the path, filter.
    private async Task<List<Sale>> Manual(Func<Sale, bool> filter)
    {
        var result = new List<Sale>();
        foreach (var path in Directory.EnumerateFiles(_root, "*.parquet", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            var parts = Path.GetRelativePath(_root, path).Split(Path.DirectorySeparatorChar);
            var date = DateOnly.Parse(parts[0]["Date=".Length..]);
            var country = parts[1]["Country=".Length..];
            foreach (var sale in (await ParquetSerializer.DeserializeAsync<Sale>(path)).Data)
            {
                sale.Date = date;
                sale.Country = country;
                if (filter(sale)) result.Add(sale);
            }
        }
        return result;
    }
}

public sealed class Sale
{
    public long Id { get; set; }
    public decimal? Amount { get; set; }
    public string Product { get; set; } = "";

    [ParquetIgnore] public DateOnly Date { get; set; }
    [ParquetIgnore] public string Country { get; set; } = "";
}
