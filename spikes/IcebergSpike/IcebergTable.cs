using System.Reflection;
using Parquet;
using Parquet.Schema;
using SolTechnology.Avro;

namespace IcebergSpike;

// metadata.json -> snapshot -> manifest list -> manifests -> Parquet data files -> rows.
public sealed class IcebergTable(TableMetadata metadata, Func<string, string> resolve)
{
    public TableMetadata Metadata { get; } = metadata;

    public static IcebergTable Load(string metadataPath, Func<string, string> resolve) => new(TableMetadata.Load(metadataPath), resolve);

    public async Task<List<DataFile>> PlanFiles(Snapshot snapshot, IReadOnlyList<Predicate> filter, ScanMetrics metrics)
    {
        var schema = Metadata.Schema(snapshot.SchemaId ?? Metadata.CurrentSchemaId);
        var files = new List<DataFile>();

        await foreach (var manifest in AvroConvert.Deserialize<ManifestFile>(File.OpenRead(resolve(snapshot.ManifestList))))
        {
            metrics.Manifests++;
            if (manifest.Content != 0)
                throw new NotSupportedException($"Delete files are not supported yet (manifest {manifest.ManifestPath}).");

            var spec = Metadata.Spec(manifest.PartitionSpecId);
            if (!ManifestMayMatch(manifest, spec, schema, filter))
            {
                metrics.ManifestsSkipped++;
                continue;
            }

            await foreach (var entry in AvroConvert.Deserialize<ManifestEntry>(File.OpenRead(resolve(manifest.ManifestPath))))
            {
                if (entry.Status == 2) continue;
                metrics.DataFiles++;
                if (!PartitionMayMatch(entry.DataFile, spec, schema, filter)) metrics.SkippedByPartition++;
                else if (!StatsMayMatch(entry.DataFile, schema, filter)) metrics.SkippedByStats++;
                else files.Add(entry.DataFile);
            }
        }
        return files;
    }

    public async Task<List<Dictionary<string, object?>>> Scan(long? snapshotId = null, IReadOnlyList<Predicate>? filter = null, ScanMetrics? metrics = null)
    {
        filter ??= [];
        metrics ??= new ScanMetrics();
        var snapshot = Metadata.Snapshot(snapshotId);
        var schema = Metadata.Schema(snapshot.SchemaId ?? Metadata.CurrentSchemaId);
        var rows = new List<Dictionary<string, object?>>();

        foreach (var file in await PlanFiles(snapshot, filter, metrics))
        {
            if (file.FileFormat != "PARQUET") throw new NotSupportedException($"File format {file.FileFormat}");
            await foreach (var row in ReadParquet(resolve(file.FilePath), schema))
                if (filter.All(p => p.Matches(row[p.Column]))) rows.Add(row);
        }
        return rows;
    }

    public IAsyncEnumerable<Dictionary<string, object?>> ReadFile(DataFile file, IcebergSchema schema) => ReadParquet(resolve(file.FilePath), schema);

    // Columns are matched by Iceberg field id, not by name: renamed columns keep their id, added columns are missing in old files.
    private static async IAsyncEnumerable<Dictionary<string, object?>> ReadParquet(string path, IcebergSchema schema)
    {
        await using var stream = File.OpenRead(path);
        await using var reader = await ParquetReader.CreateAsync(stream, new ParquetOptions { UseDateOnlyTypeForDates = true });
        var byId = reader.Schema.GetDataFields().ToDictionary(f => f.SchemaElement?.FieldId
            ?? throw new NotSupportedException($"{path} has no field ids; name mapping is not supported."));

        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            var count = (int)group.RowCount;
            var columns = new Dictionary<string, object?[]>();
            foreach (var field in schema.Fields)
                columns[field.Name] = byId.TryGetValue(field.Id, out var dataField)
                    ? (await ReadColumn(group, dataField, count)).Select(v => Values.Normalize(field.PrimitiveType, v)).ToArray()
                    : new object?[count];

            for (var r = 0; r < count; r++)
                yield return schema.Fields.ToDictionary(f => f.Name, f => columns[f.Name][r]);
        }
    }

    private static async Task<object?[]> ReadColumn(ParquetRowGroupReader group, DataField field, int count)
    {
        if (field.ClrType == typeof(ReadOnlyMemory<char>) || field.ClrType == typeof(string))
        {
            var strings = new string?[count];
            await group.ReadAsync(field, strings.AsMemory(), null, default);
            return strings;
        }
        var read = typeof(IcebergTable).GetMethod(nameof(ReadValues), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(field.ClrType);
        return await (Task<object?[]>)read.Invoke(null, [group, field, count])!;
    }

    private static async Task<object?[]> ReadValues<T>(ParquetRowGroupReader group, DataField field, int count) where T : struct
    {
        var values = new T?[count];
        await group.ReadAsync(field, values.AsMemory(), null, default);
        return values.Select(v => v.HasValue ? (object?)v.Value : null).ToArray();
    }

    private static bool ManifestMayMatch(ManifestFile manifest, PartitionSpec spec, IcebergSchema schema, IReadOnlyList<Predicate> filter)
    {
        if (manifest.Partitions is null) return true;
        for (var i = 0; i < spec.Fields.Count; i++)
        {
            var summary = manifest.Partitions[i];
            var partitionType = PartitionType(spec.Fields[i], schema);
            foreach (var projected in Projections(spec.Fields[i], schema, filter))
            {
                if (summary.LowerBound is null && summary.UpperBound is null) return false;
                var lower = summary.LowerBound is null ? null : Values.Decode(partitionType, summary.LowerBound);
                var upper = summary.UpperBound is null ? null : Values.Decode(partitionType, summary.UpperBound);
                if (!projected.Overlaps(lower, upper)) return false;
            }
        }
        return true;
    }

    private static bool PartitionMayMatch(DataFile file, PartitionSpec spec, IcebergSchema schema, IReadOnlyList<Predicate> filter)
    {
        var values = (IDictionary<string, object?>?)file.Partition ?? new Dictionary<string, object?>();
        foreach (var field in spec.Fields)
        {
            var value = Values.Normalize(PartitionType(field, schema), values[field.Name]);
            if (Projections(field, schema, filter).Any(p => !p.Matches(value))) return false;
        }
        return true;
    }

    private static bool StatsMayMatch(DataFile file, IcebergSchema schema, IReadOnlyList<Predicate> filter)
    {
        foreach (var predicate in filter)
        {
            var field = schema.Field(predicate.Column);
            var lower = file.LowerBounds?.GetValueOrDefault(field.Id);
            var upper = file.UpperBounds?.GetValueOrDefault(field.Id);
            if (!predicate.Overlaps(lower is null ? null : Values.Decode(field.PrimitiveType, lower),
                                    upper is null ? null : Values.Decode(field.PrimitiveType, upper)))
                return false;
        }
        return true;
    }

    private static IEnumerable<Predicate> Projections(PartitionField field, IcebergSchema schema, IReadOnlyList<Predicate> filter) =>
        filter.Where(p => schema.Field(p.Column).Id == field.SourceId).Select(p => p.Project(field)).OfType<Predicate>();

    private static IcebergType PartitionType(PartitionField field, IcebergSchema schema) => field.Transform switch
    {
        "identity" => schema.Field(field.SourceId).PrimitiveType,
        "day" => new IcebergType("date"),
        _ when field.Transform.StartsWith("bucket[", StringComparison.Ordinal) => new IcebergType("int"),
        _ => throw new NotSupportedException($"Transform {field.Transform}"),
    };
}
