using System.Runtime.Serialization;

namespace IcebergSpike;

// Manifest list and manifest files are Avro containers; AvroConvert reads them into these classes by field name.

[DataContract]
public sealed class ManifestFile
{
    [DataMember(Name = "manifest_path")] public string ManifestPath { get; set; } = "";
    [DataMember(Name = "manifest_length")] public long ManifestLength { get; set; }
    [DataMember(Name = "partition_spec_id")] public int PartitionSpecId { get; set; }
    [DataMember(Name = "content")] public int Content { get; set; }
    [DataMember(Name = "sequence_number")] public long SequenceNumber { get; set; }
    [DataMember(Name = "added_snapshot_id")] public long AddedSnapshotId { get; set; }
    [DataMember(Name = "partitions")] public List<FieldSummary>? Partitions { get; set; }
}

[DataContract]
public sealed class FieldSummary
{
    [DataMember(Name = "contains_null")] public bool ContainsNull { get; set; }
    [DataMember(Name = "contains_nan")] public bool? ContainsNan { get; set; }
    [DataMember(Name = "lower_bound")] public byte[]? LowerBound { get; set; }
    [DataMember(Name = "upper_bound")] public byte[]? UpperBound { get; set; }
}

[DataContract]
public sealed class ManifestEntry
{
    // 0 existing, 1 added, 2 deleted
    [DataMember(Name = "status")] public int Status { get; set; }
    [DataMember(Name = "snapshot_id")] public long? SnapshotId { get; set; }
    [DataMember(Name = "sequence_number")] public long? SequenceNumber { get; set; }
    [DataMember(Name = "data_file")] public DataFile DataFile { get; set; } = new();
}

[DataContract]
public sealed class DataFile
{
    // 0 data, 1 position deletes, 2 equality deletes
    [DataMember(Name = "content")] public int Content { get; set; }
    [DataMember(Name = "file_path")] public string FilePath { get; set; } = "";
    [DataMember(Name = "file_format")] public string FileFormat { get; set; } = "";
    [DataMember(Name = "partition")] public dynamic? Partition { get; set; }
    [DataMember(Name = "record_count")] public long RecordCount { get; set; }
    [DataMember(Name = "file_size_in_bytes")] public long FileSizeInBytes { get; set; }
    [DataMember(Name = "null_value_counts")] public Dictionary<int, long>? NullValueCounts { get; set; }
    [DataMember(Name = "lower_bounds")] public Dictionary<int, byte[]>? LowerBounds { get; set; }
    [DataMember(Name = "upper_bounds")] public Dictionary<int, byte[]>? UpperBounds { get; set; }
}
