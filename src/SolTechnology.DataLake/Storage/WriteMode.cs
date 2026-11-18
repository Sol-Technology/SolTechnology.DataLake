namespace SolTechnology.DataLake.Storage;

/// <summary>What <see cref="ILakeStorage.WriteAsync"/> does when the file already exists.</summary>
public enum WriteMode
{
    /// <summary>Replace the existing file.</summary>
    Overwrite,

    /// <summary>
    /// Write only if no file exists at the location, otherwise throw <see cref="LakeFileExistsException"/>. The check and
    /// the write are one atomic step in the storage (S3 <c>If-None-Match: *</c>, Azure <c>If-None-Match: *</c>, a hard link on
    /// local disk), so of two concurrent writers exactly one wins. Table formats commit new metadata this way.
    /// </summary>
    CreateNew,
}

/// <summary>A <see cref="WriteMode.CreateNew"/> write found a file already there, or another writer creating it.</summary>
public sealed class LakeFileExistsException(string location)
    : IOException($"'{location}' already exists.")
{
    /// <summary>Where the write was going.</summary>
    public string Location { get; } = location;
}
