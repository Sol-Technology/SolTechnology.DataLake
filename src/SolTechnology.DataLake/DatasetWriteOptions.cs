using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>Options for <see cref="Dataset{T}.WriteAsync(IAsyncEnumerable{T}, DatasetWriteOptions?, CancellationToken)"/>.</summary>
public sealed class DatasetWriteOptions
{
    /// <summary>
    /// Properties that become hive-style folders (<c>date=2026-10-01/country=PL/</c>), in this order. Their values are in
    /// the path, not in the files, and fill the properties again on read. Strings, integers, <see cref="bool"/>,
    /// <see cref="DateOnly"/>, <see cref="Guid"/> and enums.
    /// </summary>
    public IReadOnlyList<string> PartitionBy { get; init; } = [];

    /// <summary>What happens to data already at the location. <see cref="DatasetWriteMode.ErrorIfExists"/> by default.</summary>
    public DatasetWriteMode Mode { get; init; } = DatasetWriteMode.ErrorIfExists;

    /// <summary>Compression of the column pages. Snappy by default, like Spark and pyarrow.</summary>
    public ParquetCompression Compression { get; init; } = ParquetCompression.Snappy;

    /// <summary>
    /// Rows per row group, 131 072 by default. Each partition being written keeps up to this many rows in memory before
    /// they go to its file.
    /// </summary>
    public int RowGroupSize { get; init; } = 128 * 1024;

    /// <summary>
    /// A file is closed and a new one started once it reaches this size, 512 MiB by default. Checked after each row group,
    /// so files end up a little larger.
    /// </summary>
    public long TargetFileSize { get; init; } = 512L << 20;
}

/// <summary>What a dataset write does with data already at the location.</summary>
public enum DatasetWriteMode
{
    /// <summary>Fail before writing anything if the location has data files.</summary>
    ErrorIfExists,

    /// <summary>Add new files next to the existing ones.</summary>
    Append,

    /// <summary>
    /// Replace the partitions this write has rows for; other partitions stay. Without <see cref="DatasetWriteOptions.PartitionBy"/>
    /// the whole dataset is one partition. Old files are deleted after the new ones are written, so a reader in between
    /// can see both. Table formats (Iceberg) make that atomic.
    /// </summary>
    OverwritePartitions,
}

/// <summary>Compression of Parquet column pages.</summary>
public enum ParquetCompression
{
    /// <summary>Fast, moderate ratio. The default of Spark and pyarrow.</summary>
    Snappy,

    /// <summary>Better ratio at similar speed; read by every current engine.</summary>
    Zstd,

    /// <summary>For old readers that know nothing newer.</summary>
    Gzip,

    /// <summary>No compression.</summary>
    None,
}

/// <summary>What a dataset write did.</summary>
/// <param name="Files">The data files written, in the order they were finished.</param>
/// <param name="Rows">Rows written.</param>
/// <param name="Deleted">Files removed by <see cref="DatasetWriteMode.OverwritePartitions"/>.</param>
public sealed record DatasetWriteResult(IReadOnlyList<LakeFile> Files, long Rows, IReadOnlyList<LakeFile> Deleted);
