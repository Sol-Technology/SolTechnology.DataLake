using BenchmarkDotNet.Attributes;
using Parquet.Serialization;
using SolTechnology.DataLake;

/// <summary>
/// 200 000 orders in one file, each with a list of 3 lines (struct) and an address (struct): nested columns, assembled
/// from definition and repetition levels. DataLake against Parquet.Net's ParquetSerializer on the same file.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class NestedBenchmarks
{
    private const int Orders = 200_000;
    private string _root = "";

    [GlobalSetup]
    public async Task WriteFile()
    {
        _root = Path.Combine(Path.GetTempPath(), "datalake-bench-nested", Guid.NewGuid().ToString("N"));
        var orders = Enumerable.Range(0, Orders).Select(i => new NestedOrder
        {
            Id = i,
            Address = new NestedAddress { City = $"city-{i % 100}", Zip = i % 3 == 0 ? null : i % 99_999 },
            Lines = Enumerable.Range(0, 3).Select(l => new NestedLine { Sku = $"sku-{(i + l) % 500}", Quantity = l + 1, Price = l == 2 ? null : (i + l) / 100m }).ToList(),
        });
        var lake = LakeContext.Create(o => o.UseParquet(Path.GetDirectoryName(_root)!, configure: p => p.RowGroupSize(50_000)),
            m => m.Dataset<NestedOrder>(Path.GetFileName(_root)));
        await lake.Dataset<NestedOrder>(Path.GetFileName(_root)).AppendAsync(orders);
    }

    [GlobalCleanup]
    public void DeleteFile() => Directory.Delete(_root, recursive: true);

    [Benchmark]
    public Task<List<NestedOrder>> DataLake_nested() => Dataset.Open<NestedOrder>(_root).ReadAsync().ToListAsync().AsTask();

    [Benchmark]
    public async Task<IList<NestedOrder>> ParquetNet_nested() =>
        (await ParquetSerializer.DeserializeAsync<NestedOrder>(Directory.GetFiles(_root, "*.parquet")[0])).Data;
}

public sealed class NestedOrder
{
    public long Id { get; set; }
    public NestedAddress? Address { get; set; }
    public List<NestedLine>? Lines { get; set; }
}

public sealed class NestedAddress
{
    public string? City { get; set; }
    public int? Zip { get; set; }
}

public sealed class NestedLine
{
    public string? Sku { get; set; }
    public int Quantity { get; set; }
    public decimal? Price { get; set; }
}
