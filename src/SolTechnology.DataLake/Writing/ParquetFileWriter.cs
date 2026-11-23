using System.Collections.Concurrent;
using System.Text.Json;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Writing;

/// <summary>The columns of <typeparamref name="T"/> written to Parquet: every mapped property except the partition ones.</summary>
internal sealed class WriteLayout<T> where T : class
{
    private static readonly ConcurrentDictionary<(JsonNamingPolicy? Naming, string Excluded), WriteLayout<T>> Cache = new();

    private WriteLayout(JsonNamingPolicy? naming, IReadOnlySet<string> excluded)
    {
        var map = ClassMap<T>.For(naming);
        Columns = map.Properties
            .Where(p => !excluded.Contains(p.Property.Name))
            .Select(p => p.Property.GetMethod is { IsPublic: true }
                ? ColumnWriter<T>.Create(p.Property, p.Column, naming)
                : throw new NotSupportedException($"{typeof(T).Name}.{p.Property.Name} has no public getter, so it can't be written."))
            .ToArray();
        if (Columns.Length == 0) throw new NotSupportedException($"{typeof(T).Name} has no properties to write besides the partition ones.");
        Schema = new ParquetSchema(Columns.Select(c => c.Field).ToArray());
        foreach (var column in Columns) column.Bind(Schema);
    }

    public ParquetSchema Schema { get; }
    public ColumnWriter<T>[] Columns { get; }

    public static WriteLayout<T> For(JsonNamingPolicy? naming, IReadOnlySet<string> excluded) =>
        Cache.GetOrAdd((naming, string.Join('\n', excluded.Order(StringComparer.Ordinal))), _ => new WriteLayout<T>(naming, excluded));
}

/// <summary>
/// One Parquet file being written, row group by row group, to a seekable stream. <see cref="CompleteAsync"/> writes the
/// footer and fixes the timestamp statistics in it (<see cref="FooterPatch"/>).
/// </summary>
internal sealed class ParquetFileWriter<T> : IAsyncDisposable where T : class
{
    private readonly Stream _output;
    private readonly WriteLayout<T> _layout;
    private readonly ParquetWriter _writer;
    private readonly List<Dictionary<int, (long Min, long Max)>> _minMax = [];
    private bool _completed;

    private ParquetFileWriter(Stream output, WriteLayout<T> layout, ParquetWriter writer)
    {
        _output = output;
        _layout = layout;
        _writer = writer;
    }

    public long Rows { get; private set; }

    /// <summary>Bytes written so far; grows by a row group at a time.</summary>
    public long Length => _output.Length;

    public static async Task<ParquetFileWriter<T>> CreateAsync(Stream output, WriteLayout<T> layout, ParquetCompression compression, CancellationToken ct)
    {
        var options = new Parquet.ParquetOptions { CompressionMethod = Method(compression) };
        var writer = await ParquetWriter.CreateAsync(layout.Schema, output, options, append: false, ct).ConfigureAwait(false);
        writer.CustomMetadata = new Dictionary<string, string> { ["writer"] = "SolTechnology.DataLake" };
        return new ParquetFileWriter<T>(output, layout, writer);
    }

    public async Task WriteRowGroupAsync(IReadOnlyList<T> rows, CancellationToken ct)
    {
        if (rows.Count == 0) return;
        var minMax = new RowGroupMinMax();
        using (var group = _writer.CreateRowGroup())
        {
            foreach (var column in _layout.Columns) await column.WriteAsync(group, rows, minMax, ct).ConfigureAwait(false);
        }
        _minMax.Add(minMax.Columns);
        Rows += rows.Count;
    }

    public async Task CompleteAsync(CancellationToken ct)
    {
        _completed = true;
        await _writer.DisposeAsync().ConfigureAwait(false);
        await FooterPatch.FixAsync(_output, _minMax, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_completed) await _writer.DisposeAsync().ConfigureAwait(false);
    }

    private static CompressionMethod Method(ParquetCompression compression) => compression switch
    {
        ParquetCompression.Snappy => CompressionMethod.Snappy,
        ParquetCompression.Zstd => CompressionMethod.Zstd,
        ParquetCompression.Gzip => CompressionMethod.Gzip,
        ParquetCompression.None => CompressionMethod.None,
        _ => throw new ArgumentOutOfRangeException(nameof(compression)),
    };
}
