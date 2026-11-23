using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using SolTechnology.DataLake.Filtering;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;
using SolTechnology.DataLake.Writing;

namespace SolTechnology.DataLake.Providers;

// Everything a folder dataset needs besides its filter. Provider is null for Dataset.Open, Writes null when the
// dataset can't be written; MissingIsEmpty: a location that does not exist reads as empty instead of failing.
internal sealed record FilesDatasetSettings(
    LakeProvider? Provider,
    ILakeStorage Storage,
    string Location,
    DatasetSpec Spec,
    FileFormats Formats,
    ParquetOptions? Writes,
    Func<Stream> TempStream,
    bool MissingIsEmpty)
{
    public string ProviderName => Provider?.Name ?? "files";
}

/// <summary>
/// Data files under one location, possibly partitioned hive-style (<c>date=2026-10-29/country=PL/</c>), read into
/// <typeparamref name="T"/> by their <see cref="FileFormats"/>, and Parquet files written from it.
/// </summary>
internal sealed class FilesDataset<T> : IDataset<T>, IMeasuredQuery<T> where T : class
{
    private readonly FilesDatasetSettings _settings;
    private readonly Expression<Func<T, bool>>? _filter;

    public FilesDataset(FilesDatasetSettings settings, Expression<Func<T, bool>>? filter)
    {
        _settings = settings;
        _filter = filter;
        // Partition properties of an unsupported type fail here, when the dataset is first asked for, not at the first write.
        if (filter is null) PartitionKey<T>.For(ClassMap<T>.For(settings.Spec.Naming), settings.Spec.PartitionBy);
    }

    public string Name => _settings.Spec.Name;

    public IDatasetQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        predicate = SpanContains.Rewrite(predicate);
        if (_filter is null) return new FilesDataset<T>(_settings, predicate);
        var parameter = _filter.Parameters[0];
        var body = Expression.AndAlso(_filter.Body, new Rebind(predicate.Parameters[0], parameter).Visit(predicate.Body));
        return new FilesDataset<T>(_settings, Expression.Lambda<Func<T, bool>>(body, parameter));
    }

    public IAsyncEnumerable<T> ReadAsync(CancellationToken cancellationToken) => ReadAsync(new ScanMetrics(), cancellationToken);

    public async IAsyncEnumerable<T> ReadAsync(ScanMetrics metrics, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var before = metrics.Copy();
        using var activity = DataLakeTelemetry.Source.StartActivity("datalake.scan");
        Tag(activity);
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

    /// <summary>Without a filter, each file is counted by its format: Parquet from the footer, others row by row.</summary>
    public async Task<long> CountAsync(CancellationToken cancellationToken)
    {
        if (_filter is not null) return await ReadAsync(cancellationToken).LongCountAsync(cancellationToken).ConfigureAwait(false);

        long count = 0;
        foreach (var (file, format) in await ListAsync(cancellationToken).ConfigureAwait(false))
        {
            await using var stream = new RangeStream(_settings.Storage, file);
            count += await format.CountAsync(stream, file.Location, cancellationToken).ConfigureAwait(false);
        }
        return count;
    }

    public Task<LakeWriteResult> AppendAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken) =>
        WriteAsync(DatasetWrite.Append, rows, null, cancellationToken);

    public Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken) =>
        WriteAsync(DatasetWrite.Overwrite, rows, null, cancellationToken);

    public Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, Expression<Func<T, bool>> where, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(where);
        return WriteAsync(DatasetWrite.OverwriteWhere, rows, where, cancellationToken);
    }

    public Task<LakeWriteResult> ReplacePartitionsAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken) =>
        WriteAsync(DatasetWrite.ReplacePartitions, rows, null, cancellationToken);

    public Task<LakeWriteResult> DeleteAsync(Expression<Func<T, bool>> where, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(where);
        return WriteAsync(DatasetWrite.OverwriteWhere, AsyncEnumerable.Empty<T>(), where, cancellationToken);
    }

    private async Task<LakeWriteResult> WriteAsync(DatasetWrite mode, IAsyncEnumerable<T> rows, Expression<Func<T, bool>>? where, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (_filter is not null) throw new InvalidOperationException("Where filters reads; write to the dataset itself.");
        var spec = _settings.Spec;
        if (_settings.Provider is null)
            throw new InvalidOperationException("Dataset.Open is read-only. Write through a LakeContext; for a script, LakeContext.Create(o => o.UseFiles(root), m => m.Dataset<T>(name)).");
        if (spec.Declared is null || _settings.Writes is null)
            throw new InvalidOperationException($"Dataset '{spec.Name}' is not declared in {spec.Context}'s model, so it can only be read. " +
                                                $"Declare it in OnModelCreating: model.Dataset<{typeof(T).Name}>(\"{spec.Name}\").");

        using var activity = DataLakeTelemetry.Source.StartActivity("datalake.write");
        Tag(activity);
        activity?.SetTag("datalake.write.mode", mode.ToString());
        if (where is not null) activity?.SetTag("datalake.filter", where.ToString());
        var writer = new DatasetWriter<T>(_settings.Storage, _settings.Location, spec, _settings.Formats, _settings.Writes, _settings.TempStream, _settings.ProviderName);
        var result = await writer.WriteAsync(mode, rows, where, ct).ConfigureAwait(false);
        DataLakeTelemetry.RecordWrite(result, activity);
        activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
        return result;
    }

    private void Tag(System.Diagnostics.Activity? activity)
    {
        if (activity is null) return;
        activity.SetTag("datalake.location", _settings.Location);
        activity.SetTag("datalake.dataset", _settings.Spec.Name);
        activity.SetTag("datalake.context", _settings.Spec.Context);
        activity.SetTag("datalake.provider", _settings.ProviderName);
    }

    private async Task<List<(LakeFile File, ILakeFileFormat Format)>> ListAsync(CancellationToken ct)
    {
        try
        {
            return await DatasetFiles.ListAsync(_settings.Storage, _settings.Location, _settings.Formats, ct).ConfigureAwait(false);
        }
        catch (DirectoryNotFoundException) when (_settings.MissingIsEmpty)
        {
            return [];
        }
    }

    private async IAsyncEnumerable<T> ScanAsync(ScanMetrics metrics, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var map = ClassMap<T>.For(_settings.Spec.Naming);
        var predicate = _filter is null ? null : PredicateTranslator.Translate(_filter);
        var residual = _filter?.Compile();

        foreach (var (file, format) in await ListAsync(cancellationToken).ConfigureAwait(false))
        {
            metrics.Files++;
            var partitions = PartitionValues(map, file);
            if (predicate is not null && predicate.Evaluate(name => Exact(partitions, name)) == Tri.False)
            {
                metrics.FilesSkippedByPartition++;
                continue;
            }

            var scan = new FileScan<T>(map, partitions, predicate, residual, metrics);
            await using var stream = new RangeStream(_settings.Storage, file, bytes => metrics.BytesRead += bytes);
            await foreach (var row in format.ReadAsync(stream, file.Location, scan, cancellationToken).ConfigureAwait(false))
                yield return row;
        }
    }

    internal static Dictionary<PropertyMap<T>, object?> PartitionValues(ClassMap<T> map, LakeFile file)
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

    internal static ColumnFacts? Exact(Dictionary<PropertyMap<T>, object?> partitions, string property)
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
