using System.Globalization;
using System.Linq.Expressions;
using System.Text;
using SolTechnology.DataLake.Mapping;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Writing;

/// <summary>
/// Rows to Parquet files under a location, one folder per partition. Each partition buffers one row group; full row
/// groups go to a temporary local file, and a finished file goes to the storage with <see cref="WriteMode.CreateNew"/>.
/// </summary>
internal sealed class DatasetWriter<T> where T : class
{
    private readonly ILakeStorage _storage;
    private readonly string _location;
    private readonly DatasetWriteOptions _options;
    private readonly WriteLayout<T> _layout;
    private readonly PartitionKey[] _keys;
    private readonly string _writeId = Guid.NewGuid().ToString("N")[..12];
    private readonly Dictionary<string, OpenPartition> _open = new(StringComparer.Ordinal);
    private readonly List<LakeFile> _written = [];
    private int _sequence;

    public DatasetWriter(ILakeStorage storage, string location, DataLakeOptions lakeOptions, DatasetWriteOptions options)
    {
        if (options.RowGroupSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "RowGroupSize must be positive.");
        if (options.TargetFileSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "TargetFileSize must be positive.");
        _storage = storage;
        _location = location.TrimEnd('/');
        _options = options;
        var map = ClassMap<T>.For(lakeOptions.ColumnNaming);
        _keys = options.PartitionBy.Select(name => PartitionKey.Create(map, name)).ToArray();
        if (_keys.Select(k => k.Property.Property.Name).Distinct().Count() != _keys.Length)
            throw new ArgumentException("PartitionBy lists a property twice.", nameof(options));
        _layout = WriteLayout<T>.For(lakeOptions.ColumnNaming, _keys.Select(k => k.Property.Property.Name).ToHashSet());
    }

    public async Task<DatasetWriteResult> WriteAsync(IAsyncEnumerable<T> rows, CancellationToken ct)
    {
        var existing = await ExistingAsync(ct).ConfigureAwait(false);
        if (_options.Mode == DatasetWriteMode.ErrorIfExists && existing.Count > 0)
            throw new IOException($"'{_location}' already has {existing.Count} data file(s). Use DatasetWriteMode.Append or OverwritePartitions to write there.");

        long count = 0;
        try
        {
            await foreach (var row in rows.WithCancellation(ct).ConfigureAwait(false))
            {
                ArgumentNullException.ThrowIfNull(row, nameof(rows));
                var folder = Folder(row);
                if (!_open.TryGetValue(folder, out var partition)) _open[folder] = partition = new OpenPartition(folder);
                partition.Buffer.Add(row);
                if (partition.Buffer.Count >= _options.RowGroupSize) await FlushAsync(partition, ct).ConfigureAwait(false);
                count++;
            }

            foreach (var partition in _open.Values)
            {
                await FlushAsync(partition, ct).ConfigureAwait(false);
                await FinishFileAsync(partition, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            foreach (var partition in _open.Values) await partition.DisposeAsync().ConfigureAwait(false);
        }

        var deleted = new List<LakeFile>();
        if (_options.Mode == DatasetWriteMode.OverwritePartitions)
        {
            foreach (var file in existing)
            {
                if (!_open.ContainsKey(FolderOf(file.RelativePath))) continue;
                await _storage.DeleteAsync(file.Location, ct).ConfigureAwait(false);
                deleted.Add(file);
            }
        }
        return new DatasetWriteResult(_written, count, deleted);
    }

    private async Task<List<LakeFile>> ExistingAsync(CancellationToken ct)
    {
        try
        {
            return await DatasetFiles.ListAsync(_storage, _location, ct).ConfigureAwait(false);
        }
        catch (DirectoryNotFoundException)
        {
            return [];
        }
    }

    private async Task FlushAsync(OpenPartition partition, CancellationToken ct)
    {
        if (partition.Buffer.Count == 0) return;
        if (partition.Writer is null)
        {
            partition.Temp = new FileStream(Path.Combine(Path.GetTempPath(), $"datalake-{Guid.NewGuid():N}.parquet"), FileMode.CreateNew,
                FileAccess.ReadWrite, FileShare.None, 1 << 16, FileOptions.DeleteOnClose | FileOptions.Asynchronous);
            partition.Writer = await ParquetFileWriter<T>.CreateAsync(partition.Temp, _layout, _options.Compression, ct).ConfigureAwait(false);
        }
        await partition.Writer.WriteRowGroupAsync(partition.Buffer, ct).ConfigureAwait(false);
        partition.Buffer.Clear();
        if (partition.Writer.Length >= _options.TargetFileSize) await FinishFileAsync(partition, ct).ConfigureAwait(false);
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

    private string Extension() => _options.Compression switch
    {
        ParquetCompression.Snappy => ".snappy.parquet",
        ParquetCompression.Zstd => ".zstd.parquet",
        ParquetCompression.Gzip => ".gz.parquet",
        _ => ".parquet",
    };

    private string Folder(T row)
    {
        if (_keys.Length == 0) return "";
        var builder = new StringBuilder();
        foreach (var key in _keys)
        {
            if (builder.Length > 0) builder.Append('/');
            builder.Append(HivePath.Escape(key.Column)).Append('=').Append(HivePath.Escape(key.Format(row)));
        }
        return builder.ToString();
    }

    private string FolderOf(string relativePath)
    {
        if (_keys.Length == 0) return "";
        var segments = relativePath.Split('/');
        return segments.Length > _keys.Length ? string.Join('/', segments[.._keys.Length]) : "\0";
    }

    private sealed class OpenPartition(string folder) : IAsyncDisposable
    {
        public string Folder { get; } = folder;
        public List<T> Buffer { get; } = [];
        public ParquetFileWriter<T>? Writer { get; set; }
        public FileStream? Temp { get; set; }

        public async ValueTask DisposeAsync()
        {
            if (Writer is not null) await Writer.DisposeAsync().ConfigureAwait(false);
            if (Temp is not null) await Temp.DisposeAsync().ConfigureAwait(false);
            Writer = null;
            Temp = null;
        }
    }

    private sealed class PartitionKey
    {
        private static readonly Type[] Supported =
        [
            typeof(string), typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong),
            typeof(DateOnly), typeof(Guid),
        ];

        private Func<T, object?> _get = null!;

        public PropertyMap<T> Property { get; private init; } = null!;
        public string Column => Property.Column;

        public static PartitionKey Create(ClassMap<T> map, string name)
        {
            var property = map.Properties.FirstOrDefault(p => p.Property.Name == name) ?? map.Find(name)
                ?? throw new ArgumentException($"PartitionBy: {typeof(T).Name} has no property '{name}'.");
            var type = Nullable.GetUnderlyingType(property.Type) ?? property.Type;
            if (!Supported.Contains(type) && !type.IsEnum)
                throw new NotSupportedException($"PartitionBy: {typeof(T).Name}.{property.Property.Name} is {type.Name}; partition by a string, integer, bool, DateOnly, Guid or enum.");
            var row = Expression.Parameter(typeof(T));
            var get = Expression.Lambda<Func<T, object?>>(Expression.Convert(Expression.Property(row, property.Property), typeof(object)), row).Compile();
            return new PartitionKey { Property = property, _get = get };
        }

        public string Format(T row) => _get(row) switch
        {
            null or "" => HivePartitions.DefaultPartition,
            string s => s,
            bool b => b ? "true" : "false",
            DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            Guid g => g.ToString("D"),
            Enum e => e.ToString(),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            var other => other.ToString()!,
        };
    }
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
