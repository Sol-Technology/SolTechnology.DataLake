using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Providers;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>A quick look at one folder or file of data, without a <see cref="LakeContext"/>. Read-only.</summary>
public static class Dataset
{
    /// <summary>
    /// Data files at <paramref name="location"/> (a local path, <c>file://</c>, or any URI the
    /// <see cref="DataLakeOptions.Storage"/> handles: <c>s3://</c>, <c>abfss://</c>, ...), read into <typeparamref name="T"/>.
    /// Parquet is built in; Avro and Arrow IPC files are read once their package is added in <see cref="DataLakeOptions.Files"/>.
    /// Hive partitions in the path fill properties. Nothing is read until <c>ReadAsync</c>. To write, use a
    /// <see cref="LakeContext"/> (<see cref="LakeContext.Create"/> for a script).
    /// </summary>
    public static IDatasetQuery<T> Open<T>(string location, DataLakeOptions? options = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(location);
        options ??= new DataLakeOptions();
        var storage = options.Storage is { } s && s.CanHandle(location) ? s
            : LocalStorage.Instance.CanHandle(location) ? LocalStorage.Instance
            : throw new NotSupportedException($"No storage for '{location}'. Pass one in DataLakeOptions.Storage, e.g. S3Storage from SolTechnology.DataLake.Aws.");
        var formats = FilesOptions.From(options.Files).Formats(method => $"pass new DataLakeOptions {{ Files = f => f.{method} }} to Dataset.Open");
        var spec = new DatasetSpec(location, "Dataset.Open", null, options.ColumnNaming);
        var root = storage is LocalStorage ? LocalStorage.RootPath(location) : location;
        return new FilesDataset<T>(new FilesDatasetSettings(null, storage, root, spec, formats, null, TempStreams.File, MissingIsEmpty: false), null);
    }
}

/// <summary>
/// The data files of a dataset with their formats: hidden files (<c>_SUCCESS</c>, <c>.crc</c>, <c>_temporary/</c>) and
/// files no format knows left out, sorted by path.
/// </summary>
internal static class DatasetFiles
{
    public static async Task<List<(LakeFile File, ILakeFileFormat Format)>> ListAsync(ILakeStorage storage, string location, FileFormats formats, CancellationToken ct)
    {
        var files = new List<(LakeFile File, ILakeFileFormat Format)>();
        await foreach (var file in storage.ListAsync(location, ct).ConfigureAwait(false))
            if (!HivePartitions.IsHidden(file.RelativePath) && formats.Of(file) is { } format) files.Add((file, format));
        files.Sort((a, b) => string.CompareOrdinal(a.File.RelativePath, b.File.RelativePath));
        return files;
    }
}
