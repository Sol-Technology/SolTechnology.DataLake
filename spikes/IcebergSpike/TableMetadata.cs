using System.Text.Json;
using System.Text.Json.Serialization;

namespace IcebergSpike;

// metadata.json, spec v1/v2. Only what reading needs.
public sealed class TableMetadata
{
    [JsonPropertyName("format-version")] public int FormatVersion { get; init; }
    [JsonPropertyName("location")] public string Location { get; init; } = "";
    [JsonPropertyName("current-schema-id")] public int CurrentSchemaId { get; init; }
    [JsonPropertyName("schemas")] public List<IcebergSchema> Schemas { get; init; } = [];
    [JsonPropertyName("default-spec-id")] public int DefaultSpecId { get; init; }
    [JsonPropertyName("partition-specs")] public List<PartitionSpec> PartitionSpecs { get; init; } = [];
    [JsonPropertyName("current-snapshot-id")] public long? CurrentSnapshotId { get; init; }
    [JsonPropertyName("snapshots")] public List<Snapshot> Snapshots { get; init; } = [];

    public IcebergSchema Schema(int id) => Schemas.Single(s => s.SchemaId == id);
    public PartitionSpec Spec(int id) => PartitionSpecs.Single(s => s.SpecId == id);

    public Snapshot Snapshot(long? id = null) =>
        Snapshots.Single(s => s.SnapshotId == (id ?? CurrentSnapshotId ?? throw new InvalidOperationException("Table has no snapshot.")));

    // Some catalogs (Lakekeeper) write gzip-compressed metadata: *.gz.metadata.json.
    public static TableMetadata Load(string path)
    {
        var bytes = File.ReadAllBytes(path);
        if (bytes is [0x1f, 0x8b, ..])
        {
            using var gzip = new System.IO.Compression.GZipStream(new MemoryStream(bytes), System.IO.Compression.CompressionMode.Decompress);
            using var json = new MemoryStream();
            gzip.CopyTo(json);
            bytes = json.ToArray();
        }
        return JsonSerializer.Deserialize<TableMetadata>(bytes) ?? throw new InvalidDataException(path);
    }
}

public sealed class IcebergSchema
{
    [JsonPropertyName("schema-id")] public int SchemaId { get; init; }
    [JsonPropertyName("fields")] public List<SchemaField> Fields { get; init; } = [];

    public SchemaField Field(string name) => Fields.Single(f => f.Name == name);
    public SchemaField Field(int id) => Fields.Single(f => f.Id == id);
}

public sealed class SchemaField
{
    [JsonPropertyName("id")] public int Id { get; init; }
    [JsonPropertyName("name")] public string Name { get; init; } = "";
    [JsonPropertyName("required")] public bool Required { get; init; }

    // A primitive type is a string ("long", "decimal(10, 2)"); struct, list and map are objects. The spike reads primitives only.
    [JsonPropertyName("type")] public JsonElement Type { get; init; }

    public IcebergType PrimitiveType => IcebergType.Parse(Type.GetString() ?? throw new NotSupportedException($"Nested type of '{Name}'"));
}

public sealed class PartitionSpec
{
    [JsonPropertyName("spec-id")] public int SpecId { get; init; }
    [JsonPropertyName("fields")] public List<PartitionField> Fields { get; init; } = [];
}

public sealed class PartitionField
{
    [JsonPropertyName("source-id")] public int SourceId { get; init; }
    [JsonPropertyName("field-id")] public int FieldId { get; init; }
    [JsonPropertyName("transform")] public string Transform { get; init; } = "";
    [JsonPropertyName("name")] public string Name { get; init; } = "";
}

public sealed class Snapshot
{
    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; init; }
    [JsonPropertyName("sequence-number")] public long SequenceNumber { get; init; }
    [JsonPropertyName("timestamp-ms")] public long TimestampMs { get; init; }
    [JsonPropertyName("manifest-list")] public string ManifestList { get; init; } = "";
    [JsonPropertyName("schema-id")] public int? SchemaId { get; init; }
}
