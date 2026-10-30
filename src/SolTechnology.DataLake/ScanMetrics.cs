namespace SolTechnology.DataLake;

/// <summary>What a read did: how much was skipped before reading, and how much was read. Pass one to <c>ReadAsync</c>.</summary>
public sealed class ScanMetrics
{
    /// <summary>Data files found under the dataset location (hidden and unknown files not counted).</summary>
    public int Files { get; internal set; }

    /// <summary>Files skipped because their partition values (from the path) can't match the filter.</summary>
    public int FilesSkippedByPartition { get; internal set; }

    /// <summary>Parquet row groups in the files that were opened.</summary>
    public int RowGroups { get; internal set; }

    /// <summary>Row groups skipped because their min/max statistics can't match the filter.</summary>
    public int RowGroupsSkippedByStatistics { get; internal set; }

    /// <summary>Rows decoded from files.</summary>
    public long RowsRead { get; internal set; }

    /// <summary>Rows that matched the filter and were returned.</summary>
    public long RowsReturned { get; internal set; }

    /// <summary>Bytes fetched from storage.</summary>
    public long BytesRead { get; internal set; }

    /// <inheritdoc />
    public override string ToString() =>
        $"files {Files} (skipped by partition {FilesSkippedByPartition}), row groups {RowGroups} (skipped by statistics {RowGroupsSkippedByStatistics}), " +
        $"rows read {RowsRead}, returned {RowsReturned}, bytes read {BytesRead}";
}
