using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SolTechnology.DataLake.Filtering;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Providers;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Writing;

/// <summary>The five writes of <see cref="IDataset{T}"/>, as the files provider runs them.</summary>
internal enum DatasetWrite { Append, Overwrite, OverwriteWhere, ReplacePartitions }

/// <summary>
/// Rows to Parquet files under a location, one folder per partition. Each partition buffers one row group; full row
/// groups go to a temporary stream, and a finished file goes to the storage with <see cref="WriteMode.CreateNew"/>.
/// Old files are removed only after every new file is written; a failed write removes the files it wrote.
/// </summary>
internal sealed class DatasetWriter<T> where T : class
{
    private readonly ILakeStorage _storage;
    private readonly string _location;
    private readonly DatasetSpec _spec;
    private readonly FileFormats _formats;
    private readonly ParquetWriteOptions _options;
    private readonly Func<Stream> _tempStream;
    private readonly string _provider;
    private readonly ClassMap<T> _map;
    private readonly WriteLayout<T> _layout;
    private readonly PartitionKey<T>[] _keys;
    private readonly string _writeId = Guid.NewGuid().ToString("N")[..12];
    private readonly Dictionary<string, OpenPartition> _open = new(StringComparer.Ordinal);
    private readonly List<LakeFile> _written = [];
    private int _sequence;

    public DatasetWriter(ILakeStorage storage, string location, DatasetSpec spec, FileFormats formats, ParquetWriteOptions options, Func<Stream> tempStream, string provider)
    {
        _storage = storage;
        _location = location.TrimEnd('/');
        _spec = spec;
        _formats = formats;
        _options = options;
        _tempStream = tempStream;
        _provider = provider;
        _map = ClassMap<T>.For(spec.Naming);
        _keys = PartitionKey<T>.For(_map, spec.PartitionBy);
        _layout = WriteLayout<T>.For(spec.Naming, _keys.Select(k => k.Property.Property.Name).ToHashSet());
    }

    public async Task<LakeWriteResult> WriteAsync(DatasetWrite mode, IAsyncEnumerable<T> rows, Expression<Func<T, bool>>? where, CancellationToken ct)
    {
        if (mode == DatasetWrite.ReplacePartitions && _keys.Length == 0)
            throw new InvalidOperationException($"Dataset '{_spec.Name}' is not partitioned, so it has no partitions to replace. Use OverwriteAsync to replace all of it.");
        var filter = where is null ? null : CreateFilter(where);

        var existing = await ExistingAsync(ct).ConfigureAwait(false);
        CheckLayout(existing);
        // Everything that can fail about the old files fails here, before a single new file is written.
        var candidates = mode switch
        {
            DatasetWrite.Overwrite => existing.Select(f => (File: f, Partition: "")).ToList(),
            DatasetWrite.OverwriteWhere => existing.Where(filter!.Matches).Select(f => (File: f, Partition: "")).ToList(),
            DatasetWrite.ReplacePartitions => existing.Select(f => (File: f, Partition: Canonical(PartitionTexts(f)))).ToList(),
            _ => [],
        };

        long count = 0;
        try
        {
            await foreach (var row in rows.WithCancellation(ct).ConfigureAwait(false))
            {
                ArgumentNullException.ThrowIfNull(row, nameof(rows));
                if (filter is not null && !filter.Covers(row))
                    throw new InvalidDataException($"A row does not match the filter of the overwrite on '{_spec.Name}' ({where}); it would not be replaced next time.");
                var values = _keys.Select(k => k.Value(row)).ToArray();
                var folder = Folder(values);
                if (!_open.TryGetValue(folder, out var partition)) _open[folder] = partition = new OpenPartition(folder, Canonical(values));
                partition.Buffer.Add(row);
                if (partition.Buffer.Count >= _options.RowGroupSizeValue) await FlushAsync(partition, ct).ConfigureAwait(false);
                count++;
            }

            foreach (var partition in _open.Values)
            {
                await FlushAsync(partition, ct).ConfigureAwait(false);
                await FinishFileAsync(partition, ct).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            // The write failure is what the caller needs; files that could not be removed are listed on it.
            var left = await RemoveWrittenAsync().ConfigureAwait(false);
            if (left.Count > 0) e.Data["SolTechnology.DataLake.LeftoverFiles"] = left.Select(f => f.Location).ToArray();
            throw;
        }
        finally
        {
            foreach (var partition in _open.Values) await partition.DisposeAsync().ConfigureAwait(false);
        }

        var touched = _open.Values.Select(p => p.Canonical).ToHashSet(StringComparer.Ordinal);
        var remove = mode == DatasetWrite.ReplacePartitions
            ? candidates.Where(c => touched.Contains(c.Partition)).Select(c => c.File).ToList()
            : candidates.Select(c => c.File).ToList();
        var removed = await RemoveOldAsync(remove).ConfigureAwait(false);
        return new LakeWriteResult { Written = _written, Removed = removed, Rows = count };
    }

    // The new files are committed: from here a cancellation would leave both old and new data, so it is not honoured.
    // If the very first delete fails, nothing has changed yet and the new files go; later failures are reported.
    private async Task<List<LakeFile>> RemoveOldAsync(List<LakeFile> files)
    {
        var removed = new List<LakeFile>();
        var failed = new List<(LakeFile File, Exception Error)>();
        foreach (var file in files)
        {
            try
            {
                await _storage.DeleteAsync(file.Location, CancellationToken.None).ConfigureAwait(false);
                removed.Add(file);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                if (removed.Count == 0 && failed.Count == 0)
                {
                    var left = await RemoveWrittenAsync().ConfigureAwait(false);
                    if (left.Count == 0)
                        throw new IOException($"Could not remove {file.Location} from '{_spec.Name}'; the new files were removed again, nothing changed.", e);
                    // Usually the same cause (no delete permission, storage down): old and new rows are both read now.
                    throw new IOException(
                        $"Could not remove {file.Location} from '{_spec.Name}', nor the new files again: rows are now read twice. " +
                        $"Delete either the old files or these new ones: {string.Join(", ", left.Select(f => f.Location))}.", e);
                }
                failed.Add((file, e));
            }
        }
        if (failed.Count > 0)
            throw new IOException(
                $"'{_spec.Name}' has the new data, but {failed.Count} old file(s) could not be removed and are still read: " +
                $"{string.Join(", ", failed.Select(f => f.File.Location))}. Delete them, or repeat the write.",
                new AggregateException(failed.Select(f => f.Error)));
        return removed;
    }

    private async Task<List<LakeFile>> ExistingAsync(CancellationToken ct)
    {
        try
        {
            return (await DatasetFiles.ListAsync(_storage, _location, _formats, ct).ConfigureAwait(false)).Select(f => f.File).ToList();
        }
        catch (DirectoryNotFoundException)
        {
            return [];
        }
    }

    // Files laid out by other keys (Spark wrote date=, the model says nothing) would be mixed with ours, or deleted by
    // an overwrite that thinks they are one partition. Keys match case-insensitively, like reads.
    private void CheckLayout(List<LakeFile> existing)
    {
        var declared = _keys.Select(k => k.Column).ToArray();
        foreach (var file in existing)
        {
            var found = PartitionSegments(file.RelativePath).Select(s => s.Key).ToArray();
            if (found.SequenceEqual(declared, StringComparer.OrdinalIgnoreCase)) continue;
            throw new InvalidOperationException(
                $"'{_location}' has files partitioned by ({string.Join(", ", found)}), e.g. {file.RelativePath}, but dataset '{_spec.Name}' is declared " +
                $"with ({string.Join(", ", declared)}). Declare the same partitions in the model (PartitionBy), or write to another dataset.");
        }
    }

    private static IEnumerable<(string Key, string? Value)> PartitionSegments(string relativePath)
    {
        var segments = relativePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments[..^1])
        {
            var eq = segment.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var value = Uri.UnescapeDataString(segment[(eq + 1)..]);
            yield return (Uri.UnescapeDataString(segment[..eq]), value == HivePartitions.DefaultPartition ? null : value);
        }
    }

    private string?[] PartitionTexts(LakeFile file)
    {
        var parsed = HivePartitions.Parse(file.RelativePath);
        return _keys.Select(k => parsed.GetValueOrDefault(k.Column)).ToArray();
    }

    // Partition values as text, null as its own marker: equal for the same partition however its folder was escaped.
    private static string Canonical(string?[] values) => string.Join('\u001F', values.Select(v => v ?? "\u0000"));

    private string Folder(string?[] values)
    {
        if (_keys.Length == 0) return "";
        var builder = new StringBuilder();
        for (var i = 0; i < _keys.Length; i++)
        {
            if (builder.Length > 0) builder.Append('/');
            builder.Append(HivePath.Escape(_keys[i].Column)).Append('=').Append(HivePath.Escape(values[i] ?? HivePartitions.DefaultPartition));
        }
        return builder.ToString();
    }

    private FilesFilter CreateFilter(Expression<Func<T, bool>> where)
    {
        var predicate = PredicateTranslator.Translate(SpanContains.Rewrite(where));
        var partitionProperties = _keys.Select(k => k.Property.Property.Name).ToHashSet(StringComparer.Ordinal);
        var outside = predicate.Properties().FirstOrDefault(p => !partitionProperties.Contains(p));
        if (HasEmptyString(predicate))
            throw new NotSupportedException($"The filter on '{_spec.Name}' compares a partition with \"\", but an empty partition value is stored as Hive's default partition and reads as null. Compare with null instead: {where}.");
        if (HasOpaque(predicate) || outside is not null || !predicate.Properties().Any())
            throw new NotSupportedException(
                $"The {_provider} provider deletes whole files, so the filter of an overwrite or delete on '{_spec.Name}' must compare partition columns " +
                $"({(_keys.Length == 0 ? "none declared" : string.Join(", ", _keys.Select(k => k.Property.Property.Name)))}) with values" +
                $"{(outside is null ? "" : $"; '{outside}' is not one")}: {where}. Rows inside files are not rewritten; a table format (Iceberg) can do that.");
        // Rows are checked on the values they are stored with ("" as null), the same way as the files they replace.
        return new FilesFilter(predicate, file =>
        {
            var partitions = FilesDataset<T>.PartitionValues(_map, file);
            return name => FilesDataset<T>.Exact(partitions, name);
        }, row => name => _keys.FirstOrDefault(k => k.Property.Property.Name == name) is { } key ? new ColumnFacts.Exact(key.Stored(row)) : null, _spec.Name);
    }

    private static bool HasEmptyString(Predicate predicate) => predicate switch
    {
        Predicate.Compare c => c.Value is "",
        Predicate.In i => i.Values.Any(v => v is ""),
        Predicate.And a => HasEmptyString(a.Left) || HasEmptyString(a.Right),
        Predicate.Or o => HasEmptyString(o.Left) || HasEmptyString(o.Right),
        Predicate.Not n => HasEmptyString(n.Inner),
        _ => false,
    };

    private static bool HasOpaque(Predicate predicate) => predicate switch
    {
        Predicate.Opaque => true,
        Predicate.And a => HasOpaque(a.Left) || HasOpaque(a.Right),
        Predicate.Or o => HasOpaque(o.Left) || HasOpaque(o.Right),
        Predicate.Not n => HasOpaque(n.Inner),
        _ => false,
    };

    private async Task FlushAsync(OpenPartition partition, CancellationToken ct)
    {
        if (partition.Buffer.Count == 0) return;
        if (partition.Writer is null)
        {
            partition.Temp = _tempStream();
            partition.Writer = await ParquetFileWriter<T>.CreateAsync(partition.Temp, _layout, _options.CompressionValue, ct).ConfigureAwait(false);
        }
        await partition.Writer.WriteRowGroupAsync(partition.Buffer, ct).ConfigureAwait(false);
        partition.Buffer.Clear();
        if (partition.Writer.Length >= _options.TargetFileSizeValue) await FinishFileAsync(partition, ct).ConfigureAwait(false);
    }

    private async Task FinishFileAsync(OpenPartition partition, CancellationToken ct)
    {
        if (partition.Writer is null) return;
        await partition.Writer.CompleteAsync(ct).ConfigureAwait(false);
        var temp = partition.Temp!;
        temp.Position = 0;
        var name = $"part-{Interlocked.Increment(ref _sequence):00000}-{_writeId}{Extension()}";
        var relative = partition.Folder.Length == 0 ? name : $"{partition.Folder}/{name}";
        var file = await _storage.WriteAsync($"{_location}/{relative}", temp, WriteMode.CreateNew, ct).ConfigureAwait(false);
        _written.Add(file with { RelativePath = relative });
        await partition.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Removes the files this write wrote, best effort. Returns the ones that are still there.</summary>
    private async Task<List<LakeFile>> RemoveWrittenAsync()
    {
        var left = new List<LakeFile>();
        foreach (var file in _written)
        {
            try
            {
                await _storage.DeleteAsync(file.Location, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                left.Add(file);
            }
        }
        return left;
    }

    private string Extension() => _options.CompressionValue switch
    {
        ParquetCompression.Snappy => ".snappy.parquet",
        ParquetCompression.Zstd => ".zstd.parquet",
        ParquetCompression.Gzip => ".gz.parquet",
        _ => ".parquet",
    };

    private sealed class OpenPartition(string folder, string canonical) : IAsyncDisposable
    {
        public string Folder { get; } = folder;
        public string Canonical { get; } = canonical;
        public List<T> Buffer { get; } = [];
        public ParquetFileWriter<T>? Writer { get; set; }
        public Stream? Temp { get; set; }

        public async ValueTask DisposeAsync()
        {
            if (Writer is not null) await Writer.DisposeAsync().ConfigureAwait(false);
            if (Temp is not null) await Temp.DisposeAsync().ConfigureAwait(false);
            Writer = null;
            Temp = null;
        }
    }

    /// <summary>The filter of an overwrite or delete: which existing files it covers, and whether a new row is inside it.</summary>
    private sealed class FilesFilter(Predicate predicate, Func<LakeFile, Func<string, ColumnFacts?>> facts, Func<T, Func<string, ColumnFacts?>> rowFacts, string dataset)
    {
        public bool Covers(T row) => predicate.Evaluate(rowFacts(row)) == Tri.True;

        public bool Matches(LakeFile file) => predicate.Evaluate(facts(file)) switch
        {
            Tri.True => true,
            Tri.False => false,
            _ => throw new InvalidOperationException(
                $"Can't tell whether {file.RelativePath} of '{dataset}' matches the filter (its partition values don't compare with the filter's values); nothing was written or deleted."),
        };
    }
}

/// <summary>A partition property of <typeparamref name="T"/>: its column name and its value in a row as folder text.</summary>
internal sealed class PartitionKey<T> where T : class
{
    private static readonly Type[] Supported =
    [
        typeof(string), typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong),
        typeof(DateOnly), typeof(Guid),
    ];

    private Func<T, object?> _get = null!;

    public PropertyMap<T> Property { get; private init; } = null!;
    public string Column => Property.Column;

    public static PartitionKey<T>[] For(ClassMap<T> map, IReadOnlyList<PropertyInfo> properties) =>
        properties.Select(p => Create(map, p)).ToArray();

    private static PartitionKey<T> Create(ClassMap<T> map, PropertyInfo info)
    {
        var property = map.Property(info.Name);
        var type = Nullable.GetUnderlyingType(property.Type) ?? property.Type;
        if (!Supported.Contains(type) && !type.IsEnum)
            throw new NotSupportedException($"PartitionBy: {typeof(T).Name}.{info.Name} is {type.Name}; partition by a string, integer, bool, DateOnly, Guid or enum.");
        var row = Expression.Parameter(typeof(T));
        var get = Expression.Lambda<Func<T, object?>>(Expression.Convert(Expression.Property(row, info), typeof(object)), row).Compile();
        return new PartitionKey<T> { Property = property, _get = get };
    }

    /// <summary>The value as it reads back: the property value, but null for the empty string (Hive's default partition).</summary>
    public object? Stored(T row) => _get(row) is "" ? null : _get(row);

    /// <summary>The value as folder text before escaping; null for null and the empty string (Hive's default partition).</summary>
    public string? Value(T row) => _get(row) switch
    {
        null or "" => null,
        string s => s,
        bool b => b ? "true" : "false",
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        Guid g => g.ToString("D"),
        Enum e => e.ToString(),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        var other => other.ToString(),
    };
}

/// <summary>Hive's escaping of partition folder names, which Spark, Trino and DuckDB share.</summary>
internal static class HivePath
{
    private const string Special = "\"#%'*/:=?\\\u007F{[]^";

    public static string Escape(string value)
    {
        if (value == HivePartitions.DefaultPartition) return value;
        StringBuilder? builder = null;
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (c >= ' ' && !Special.Contains(c))
            {
                builder?.Append(c);
                continue;
            }
            builder ??= new StringBuilder(value, 0, i, value.Length + 8);
            builder.Append('%').Append(((int)c).ToString("X2", CultureInfo.InvariantCulture));
        }
        return builder?.ToString() ?? value;
    }
}
