using System.Globalization;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Cli;

/// <summary>
/// What <c>datalake stats</c> prints. Parquet files are summed from their footers (no data pages read); Avro and Arrow
/// files are counted row by row. Min / max come from row group statistics; a column without them in any row group shows
/// "?", because a partial range would mislead.
/// </summary>
internal sealed class DatasetStats
{
    private readonly Dictionary<string, ColumnStats> _columns = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<string?>> _partitions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<FileFormat, int> _formats = [];
    private long _rows;
    private long _rowGroups;
    private long _bytes;
    private int _files;

    public static async Task<DatasetStats> CollectAsync(ILakeStorage storage, string location, CancellationToken ct)
    {
        var stats = new DatasetStats();
        foreach (var file in await DatasetFiles.ListAsync(storage, location, ct))
        {
            stats._files++;
            stats._bytes += file.Length;
            var format = DatasetFiles.FormatOf(file.RelativePath);
            stats._formats[format] = stats._formats.GetValueOrDefault(format) + 1;
            foreach (var (key, value) in HivePartitions.Parse(file.RelativePath))
            {
                if (!stats._partitions.TryGetValue(key, out var values)) stats._partitions[key] = values = [];
                values.Add(value);
            }

            if (format == FileFormat.Parquet) await stats.AddParquetAsync(storage, file, ct);
            else await foreach (var _ in UntypedReader.ReadAsync(storage, file.Location, ct)) stats._rows++;
        }
        return stats;
    }

    private async Task AddParquetAsync(ILakeStorage storage, LakeFile file, CancellationToken ct)
    {
        await using var stream = new RangeStream(storage, file);
        await using var reader = await ParquetReader.CreateAsync(stream, new ParquetOptions { UseDateOnlyTypeForDates = true }, leaveStreamOpen: true, ct);
        var trust = ParquetStatistics.TrustsTimestamps(reader.Metadata?.CreatedBy);
        var fields = reader.Schema.GetDataFields();
        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            _rows += group.RowCount;
            _rowGroups++;
            foreach (var field in fields)
            {
                var name = Name(field);
                if (!_columns.TryGetValue(name, out var column)) _columns[name] = column = new ColumnStats(Program.TypeName(field));
                column.Add(Statistics(group, field), field, trust);
            }
        }
    }

    // lines/list/element/sku → lines[].sku, attributes/key_value/key → attributes.key
    private static string Name(DataField field) =>
        string.Join('.', field.Path.ToList()).Replace(".list.element", "[]").Replace(".key_value.", ".");

    private static Parquet.Data.DataColumnStatistics? Statistics(ParquetRowGroupReader group, DataField field)
    {
        try
        {
            return group.GetStatistics(field);
        }
        catch (Exception e) when (e is NotSupportedException or InvalidCastException or InvalidDataException)
        {
            return null;
        }
    }

    public async Task WriteAsync(TextWriter output)
    {
        var formats = string.Join(", ", _formats.OrderBy(f => f.Key).Select(f => $"{f.Value} {f.Key.ToString().ToLowerInvariant()}"));
        await output.WriteLineAsync(string.Create(CultureInfo.InvariantCulture,
            $"{_files} files ({formats}), {Program.Size(_bytes)}, {_rows:N0} rows, {_rowGroups} row groups"));
        foreach (var (key, values) in _partitions)
            await output.WriteLineAsync($"partition {key}: {values.Count} values ({string.Join(", ", values.Order(StringComparer.Ordinal).Take(5).Select(v => v ?? "null"))}{(values.Count > 5 ? ", ..." : "")})");
        if (_columns.Count == 0) return;

        var rows = _columns.Select(c => (c.Key, c.Value.Type, Nulls: c.Value.Nulls?.ToString("N0", CultureInfo.InvariantCulture) ?? "?",
            Min: c.Value.Text(c.Value.Min), Max: c.Value.Text(c.Value.Max))).ToList();
        var widths = new[]
        {
            Math.Max(6, rows.Max(r => r.Key.Length)), Math.Max(4, rows.Max(r => r.Type.Length)), Math.Max(5, rows.Max(r => r.Nulls.Length)),
            Math.Max(3, rows.Max(r => r.Min.Length)),
        };
        await output.WriteLineAsync($"{"column".PadRight(widths[0])}  {"type".PadRight(widths[1])}  {"nulls".PadLeft(widths[2])}  {"min".PadRight(widths[3])}  max");
        foreach (var r in rows)
            await output.WriteLineAsync($"{r.Key.PadRight(widths[0])}  {r.Type.PadRight(widths[1])}  {r.Nulls.PadLeft(widths[2])}  {r.Min.PadRight(widths[3])}  {r.Max}".TrimEnd());
    }

    private sealed class ColumnStats(string type)
    {
        private bool _rangeKnown = true;

        public string Type { get; } = type;
        public long? Nulls { get; private set; } = 0;
        public object? Min { get; private set; }
        public object? Max { get; private set; }

        public void Add(Parquet.Data.DataColumnStatistics? statistics, DataField field, bool trust)
        {
            Nulls = statistics?.NullCount is { } nulls && Nulls is { } sum ? sum + nulls : null;
            var min = ParquetStatistics.Decode(statistics?.MinValue, field, trust);
            var max = ParquetStatistics.Decode(statistics?.MaxValue, field, trust);
            if (min is not IComparable || max is not IComparable)
            {
                // All nulls has no range, and that's fine; anything else unknown makes the range unknown.
                if (statistics?.NullCount is null || statistics.MinValue is not null || statistics.MaxValue is not null) _rangeKnown = false;
                return;
            }
            if (Min is null || Compare(min, Min) < 0) Min = min;
            if (Max is null || Compare(max, Max) > 0) Max = max;
        }

        public string Text(object? value) => !_rangeKnown ? "?" : value switch
        {
            null => "",
            DateTime dt => dt.ToString(dt.Kind == DateTimeKind.Utc ? "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ" : "yyyy-MM-ddTHH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
            DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            byte[] bytes => Convert.ToHexString(bytes),
            string s => s.Length > 40 ? s[..37] + "..." : s,
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "",
        };

        private static int Compare(object a, object b) =>
            a is string sa && b is string sb ? string.CompareOrdinal(sa, sb) : ((IComparable)a).CompareTo(b);
    }
}
