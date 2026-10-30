namespace IcebergSpike;

// Spike filter: a conjunction of inclusive ranges. Equality is a range with Min == Max.
public sealed record Predicate(string Column, object Min, object Max)
{
    public static Predicate Eq(string column, object value) => new(column, value, value);

    public bool Matches(object? value) =>
        value is not null && Values.Compare(value, Min) >= 0 && Values.Compare(value, Max) <= 0;

    public bool Overlaps(object? lower, object? upper) =>
        (upper is null || Values.Compare(upper, Min) >= 0) && (lower is null || Values.Compare(lower, Max) <= 0);

    // Predicate on a source column turned into a predicate on its partition value. Null: transform cannot prune.
    public Predicate? Project(PartitionField field)
    {
        if (field.Transform is "identity" or "day")
            return this with { Min = Transforms.Apply(field.Transform, Min)!, Max = Transforms.Apply(field.Transform, Max)! };
        if (field.Transform.StartsWith("bucket[", StringComparison.Ordinal) && Values.Compare(Min, Max) == 0)
            return this with { Min = Transforms.Apply(field.Transform, Min)!, Max = Transforms.Apply(field.Transform, Min)! };
        return null;
    }
}

public sealed class ScanMetrics
{
    public int Manifests { get; set; }
    public int ManifestsSkipped { get; set; }
    public int DataFiles { get; set; }
    public int SkippedByPartition { get; set; }
    public int SkippedByStats { get; set; }
    public int DeleteFiles { get; set; }
    public int RowsDeleted { get; set; }
    public int FilesRead => DataFiles - SkippedByPartition - SkippedByStats;
    public override string ToString() =>
        $"manifests {Manifests} (skipped {ManifestsSkipped}), files {DataFiles}: skipped by partition {SkippedByPartition}, by stats {SkippedByStats}, read {FilesRead}; delete files {DeleteFiles}, rows deleted {RowsDeleted}";
}

public sealed record ScanTask(DataFile File, long SequenceNumber);

public sealed record ScanPlan(List<ScanTask> DataFiles, List<ScanTask> DeleteFiles);
