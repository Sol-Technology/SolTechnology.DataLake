using Parquet;
using Parquet.Schema;

namespace SolTechnology.DataLake.UnitTests;

/// <summary>Temporary datasets written with Parquet.Net, deleted after each test.</summary>
internal sealed class TempDataset : IDisposable
{
    public string Root { get; } = Path.Combine(Path.GetTempPath(), "datalake-tests", Guid.NewGuid().ToString("N"));

    public TempDataset() => Directory.CreateDirectory(Root);

    public string PathOf(string relative) => Path.Combine(Root, relative);

    /// <summary>One Parquet file; each element of <paramref name="rowGroups"/> is one row group: column arrays in schema order.</summary>
    public async Task WriteAsync(string relative, DataField[] schema, params Array[][] rowGroups)
    {
        var path = PathOf(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        await using var stream = File.Create(path);
        await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(schema), stream);
        foreach (var columns in rowGroups)
        {
            using var group = writer.CreateRowGroup();
            for (var c = 0; c < schema.Length; c++) await Write(group, schema[c], columns[c]);
        }
    }

    public void Touch(string relative)
    {
        var path = PathOf(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, "not parquet");
    }

    private static Task Write(ParquetRowGroupWriter group, DataField field, Array values) => values switch
    {
        string?[] s => group.WriteAsync(field, s),
        long[] v => group.WriteAsync(field, (ReadOnlyMemory<long>)v),
        long?[] v => group.WriteAsync(field, (ReadOnlyMemory<long?>)v),
        int[] v => group.WriteAsync(field, (ReadOnlyMemory<int>)v),
        int?[] v => group.WriteAsync(field, (ReadOnlyMemory<int?>)v),
        decimal?[] v => group.WriteAsync(field, (ReadOnlyMemory<decimal?>)v),
        double?[] v => group.WriteAsync(field, (ReadOnlyMemory<double?>)v),
        bool[] v => group.WriteAsync(field, (ReadOnlyMemory<bool>)v),
        DateTime[] v => group.WriteAsync(field, (ReadOnlyMemory<DateTime>)v),
        DateOnly[] v => group.WriteAsync(field, (ReadOnlyMemory<DateOnly>)v),
        _ => throw new NotSupportedException(values.GetType().Name),
    };

    public void Dispose()
    {
        try { Directory.Delete(Root, recursive: true); }
        catch (IOException) { }
    }
}
