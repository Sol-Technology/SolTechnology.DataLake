namespace SolTechnology.DataLake.Storage;

/// <summary>
/// A file in a data lake: full location (path or URI), size in bytes, and path relative to the listed location
/// with <c>/</c> separators (<c>date=2026-10-29/part-0.parquet</c>), which is where partitions are read from.
/// </summary>
public sealed record LakeFile(string Location, long Length, string RelativePath);

/// <summary>
/// Where the files live: local disk, S3, Azure Blob / ADLS Gen2, ... Reading needs only listing and reading byte
/// ranges, so a Parquet footer and the column chunks a query needs are fetched instead of whole files.
/// </summary>
public interface ILakeStorage
{
    /// <summary>Whether this storage handles <paramref name="location"/>, e.g. by its URI scheme.</summary>
    bool CanHandle(string location);

    /// <summary>All files under <paramref name="location"/>, recursively, with paths relative to it.</summary>
    IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="buffer"/>.Length bytes of <paramref name="file"/> starting at <paramref name="offset"/>.
    /// Returns the number of bytes read; less than requested only at the end of the file.
    /// </summary>
    ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default);
}
