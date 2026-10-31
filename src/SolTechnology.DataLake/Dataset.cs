using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using SolTechnology.DataLake.Filtering;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>Entry point: a folder of data files read as one table.</summary>
public static class DataLake
{
    /// <summary>
    /// A dataset at <paramref name="location"/>: a local path, <c>file://</c>, or any URI the
    /// <see cref="DataLakeOptions.Storage"/> handles (<c>s3://</c>, <c>abfss://</c>, ...). Nothing is read until
    /// <see cref="Dataset{T}.ReadAsync"/>.
    /// </summary>
    public static Dataset<T> Open<T>(string location, DataLakeOptions? options = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(location);
        options ??= new DataLakeOptions();
        var storage = options.Storage is { } s && s.CanHandle(location) ? s
            : LocalStorage.Instance.CanHandle(location) ? LocalStorage.Instance
            : throw new NotSupportedException($"No storage for '{location}'. Pass one in DataLakeOptions.Storage, e.g. S3Storage from SolTechnology.DataLake.Aws.");
        return new Dataset<T>(location, storage, options, null);
    }
}

/// <summary>
/// Parquet and Avro files under one location, possibly partitioned hive-style (<c>date=2026-10-29/country=PL/</c>), read into
/// <typeparamref name="T"/>. Columns map to properties by name; partition values from the path fill properties too.
/// </summary>
public sealed class Dataset<T> where T : class
{
    private readonly ILakeStorage _storage;
    private readonly DataLakeOptions _options;
    private readonly Expression<Func<T, bool>>? _filter;

    internal Dataset(string location, ILakeStorage storage, DataLakeOptions options, Expression<Func<T, bool>>? filter)
    {
        Location = location;
        _storage = storage;
        _options = options;
        _filter = filter;
    }

    /// <summary>The location the dataset was opened at.</summary>
    public string Location { get; }

    /// <summary>
    /// Keeps rows matching <paramref name="predicate"/>. Comparisons of properties with values (<c>==</c>, <c>&lt;</c>,
    /// <c>&amp;&amp;</c>, <c>||</c>, <c>!</c>, <c>list.Contains(x.P)</c>) also skip whole files by partition and whole row
    /// groups by statistics. Anything else still filters correctly, row by row.
    /// </summary>
    public Dataset<T> Where(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        predicate = SpanContains.Rewrite(predicate);
        if (_filter is null) return new Dataset<T>(Location, _storage, _options, predicate);
        var parameter = _filter.Parameters[0];
        var body = Expression.AndAlso(_filter.Body, new Rebind(predicate.Parameters[0], parameter).Visit(predicate.Body));
        return new Dataset<T>(Location, _storage, _options, Expression.Lambda<Func<T, bool>>(body, parameter));
    }

    /// <summary>
    /// Rows in file order (files sorted by path). Pass <paramref name="metrics"/> to see what was skipped and read.
    /// Each read is also an activity <c>datalake.scan</c> and adds to the <c>datalake.scan.*</c> counters (<see cref="DataLakeTelemetry"/>).
    /// </summary>
    public async IAsyncEnumerable<T> ReadAsync(ScanMetrics? metrics = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        metrics ??= new ScanMetrics();
        var before = metrics.Copy();
        using var activity = DataLakeTelemetry.Source.StartActivity("datalake.scan");
        activity?.SetTag("datalake.location", Location);
        activity?.SetTag("datalake.row_type", typeof(T).FullName);
        if (_filter is not null) activity?.SetTag("datalake.filter", _filter.ToString());
        try
        {
            await foreach (var row in ScanAsync(metrics, cancellationToken).ConfigureAwait(false))
                yield return row;
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
        }
        finally
        {
            DataLakeTelemetry.Record(metrics.Minus(before), activity);
        }
    }

    private async IAsyncEnumerable<T> ScanAsync(ScanMetrics metrics, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var map = ClassMap<T>.For(_options.ColumnNaming);
        var predicate = _filter is null ? null : PredicateTranslator.Translate(_filter);
        var residual = _filter?.Compile();

        var files = await DatasetFiles.ListAsync(_storage, Location, cancellationToken).ConfigureAwait(false);

        foreach (var file in files)
        {
            metrics.Files++;
            var partitions = PartitionValues(map, file);
            if (predicate is not null && predicate.Evaluate(name => Exact(partitions, name)) == Tri.False)
            {
                metrics.FilesSkippedByPartition++;
                continue;
            }

            var scan = new FileScan<T>(map, partitions, predicate, residual, metrics);
            await using var stream = new RangeStream(_storage, file, bytes => metrics.BytesRead += bytes);
            var rows = DatasetFiles.FormatOf(file.RelativePath) == FileFormat.Avro
                ? AvroFile<T>.ReadAsync(stream, file.Location, scan, cancellationToken)
                : ParquetFile<T>.ReadAsync(stream, file.Location, scan, cancellationToken);
            await foreach (var row in rows.ConfigureAwait(false))
                yield return row;
        }
    }

    private static Dictionary<PropertyMap<T>, object?> PartitionValues(ClassMap<T> map, LakeFile file)
    {
        var values = new Dictionary<PropertyMap<T>, object?>();
        foreach (var (key, text) in HivePartitions.Parse(file.RelativePath))
        {
            if (map.Find(key) is not { } property) continue;
            var value = Conversions.ParsePartition(text, property.Type, key);
            if (value is null && !property.AcceptsNull)
                throw new InvalidDataException($"{file.Location}: partition '{key}' is null, but {typeof(T).Name}.{property.Property.Name} ({property.Type.Name}) does not accept null.");
            values[property] = value;
        }
        return values;
    }

    private static ColumnFacts? Exact(Dictionary<PropertyMap<T>, object?> partitions, string property)
    {
        foreach (var (map, value) in partitions)
            if (map.Property.Name == property) return new ColumnFacts.Exact(value);
        return null;
    }

    private sealed class Rebind(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : node;
    }
}

internal enum FileFormat { Unknown, Parquet, Avro }

/// <summary>The data files of a dataset: hidden files (<c>_SUCCESS</c>, <c>.crc</c>, <c>_temporary/</c>) and unknown formats left out, sorted by path.</summary>
internal static class DatasetFiles
{
    public static async Task<List<LakeFile>> ListAsync(ILakeStorage storage, string location, CancellationToken ct)
    {
        var files = new List<LakeFile>();
        await foreach (var file in storage.ListAsync(location, ct).ConfigureAwait(false))
            if (!HivePartitions.IsHidden(file.RelativePath) && FormatOf(file.RelativePath) != FileFormat.Unknown) files.Add(file);
        files.Sort((a, b) => string.CompareOrdinal(a.RelativePath, b.RelativePath));
        return files;
    }

    public static FileFormat FormatOf(string path) =>
        path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".parq", StringComparison.OrdinalIgnoreCase) ? FileFormat.Parquet
        : path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase) ? FileFormat.Avro
        : FileFormat.Unknown;
}
