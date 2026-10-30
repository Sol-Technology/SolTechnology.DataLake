using System.Reflection;
using Parquet;
using Parquet.Schema;
using SolTechnology.Avro;

namespace IcebergSpike;

// metadata.json -> snapshot -> manifest list -> manifests -> Parquet data files -> rows.
public sealed class IcebergTable(TableMetadata metadata, IFileIO io)
{
    public TableMetadata Metadata { get; } = metadata;

    public static IcebergTable Load(string metadataPath, IFileIO io) => new(TableMetadata.Load(metadataPath), io);

    public async Task<List<DataFile>> PlanFiles(Snapshot snapshot, IReadOnlyList<Predicate> filter, ScanMetrics metrics) =>
        (await Plan(snapshot, filter, metrics)).DataFiles.Select(t => t.File).ToList();

    public async Task<ScanPlan> Plan(Snapshot snapshot, IReadOnlyList<Predicate> filter, ScanMetrics metrics)
    {
        var schema = Metadata.Schema(snapshot.SchemaId ?? Metadata.CurrentSchemaId);
        var plan = new ScanPlan([], []);

        await foreach (var manifest in AvroConvert.Deserialize<ManifestFile>(await io.OpenAsync(snapshot.ManifestList)))
        {
            metrics.Manifests++;
            var spec = Metadata.Spec(manifest.PartitionSpecId);

            // Delete manifests are not pruned by the data filter: their column stats describe file_path and pos.
            if (manifest.Content == 1)
            {
                await foreach (var entry in AvroConvert.Deserialize<ManifestEntry>(await io.OpenAsync(manifest.ManifestPath)))
                {
                    if (entry.Status == 2) continue;
                    if (entry.DataFile.Content == 2)
                        throw new NotSupportedException($"Equality delete files are not supported yet ({entry.DataFile.FilePath}).");
                    plan.DeleteFiles.Add(new ScanTask(entry.DataFile, entry.SequenceNumber ?? manifest.SequenceNumber));
                }
                continue;
            }

            if (!ManifestMayMatch(manifest, spec, schema, filter))
            {
                metrics.ManifestsSkipped++;
                continue;
            }

            await foreach (var entry in AvroConvert.Deserialize<ManifestEntry>(await io.OpenAsync(manifest.ManifestPath)))
            {
                if (entry.Status == 2) continue;
                metrics.DataFiles++;
                if (!PartitionMayMatch(entry.DataFile, spec, schema, filter)) metrics.SkippedByPartition++;
                else if (!StatsMayMatch(entry.DataFile, schema, filter)) metrics.SkippedByStats++;
                // Sequence number inherited from the manifest when the entry has none (spec: "Sequence Number Inheritance").
                else plan.DataFiles.Add(new ScanTask(entry.DataFile, entry.SequenceNumber ?? manifest.SequenceNumber));
            }
        }
        metrics.DeleteFiles = plan.DeleteFiles.Count;
        return plan;
    }

    public async Task<List<Dictionary<string, object?>>> Scan(long? snapshotId = null, IReadOnlyList<Predicate>? filter = null, ScanMetrics? metrics = null)
    {
        filter ??= [];
        metrics ??= new ScanMetrics();
        var snapshot = Metadata.Snapshot(snapshotId);
        var schema = Metadata.Schema(snapshot.SchemaId ?? Metadata.CurrentSchemaId);
        var plan = await Plan(snapshot, filter, metrics);
        var deletes = await PositionDeletes(plan.DeleteFiles);
        var rows = new List<Dictionary<string, object?>>();

        foreach (var task in plan.DataFiles)
        {
            if (task.File.FileFormat != "PARQUET") throw new NotSupportedException($"File format {task.File.FileFormat}");
            // A position delete applies to data files with a sequence number <= its own.
            var deleted = deletes.TryGetValue(task.File.FilePath, out var list)
                ? list.Where(d => d.SequenceNumber >= task.SequenceNumber).Select(d => d.Position).ToHashSet()
                : null;
            metrics.RowsDeleted += deleted?.Count ?? 0;
            await foreach (var row in ReadFile(task.File, schema, deleted))
                if (filter.All(p => p.Matches(row[p.Column]))) rows.Add(row);
        }
        return rows;
    }

    public async IAsyncEnumerable<Dictionary<string, object?>> ReadFile(DataFile file, IcebergSchema schema, IReadOnlySet<long>? deletedPositions = null)
    {
        await using var stream = await io.OpenAsync(file.FilePath);
        await foreach (var row in ReadParquet(stream, file.FilePath, schema, deletedPositions)) yield return row;
    }

    // Position delete files: (file_path, pos) with reserved field ids 2147483546 and 2147483545.
    private async Task<Dictionary<string, List<(long SequenceNumber, long Position)>>> PositionDeletes(List<ScanTask> deleteFiles)
    {
        var schema = new IcebergSchema { Fields = [Reserved(2147483546, "file_path", "string"), Reserved(2147483545, "pos", "long")] };
        var result = new Dictionary<string, List<(long, long)>>();
        foreach (var task in deleteFiles)
            await foreach (var row in ReadFile(task.File, schema))
            {
                var path = (string)row["file_path"]!;
                if (!result.TryGetValue(path, out var list)) result[path] = list = [];
                list.Add((task.SequenceNumber, (long)row["pos"]!));
            }
        return result;

        static SchemaField Reserved(int id, string name, string type) =>
            new() { Id = id, Name = name, Required = true, Type = System.Text.Json.JsonDocument.Parse($"\"{type}\"").RootElement };
    }

    // Columns are matched by Iceberg field id, not by name: renamed columns keep their id, added columns are missing in old files.
    private static async IAsyncEnumerable<Dictionary<string, object?>> ReadParquet(Stream stream, string path, IcebergSchema schema, IReadOnlySet<long>? deletedPositions)
    {
        long position = 0;
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

            for (var r = 0; r < count; r++, position++)
                if (deletedPositions is null || !deletedPositions.Contains(position))
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
