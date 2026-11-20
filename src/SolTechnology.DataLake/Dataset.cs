using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Providers;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>A quick look at one folder or file of data, without a <see cref="LakeContext"/>. Read-only.</summary>
public static class Dataset
{
    /// <summary>
    /// Parquet, Avro and Arrow IPC files at <paramref name="location"/> (a local path, <c>file://</c>, or any URI the
    /// <see cref="DataLakeOptions.Storage"/> handles: <c>s3://</c>, <c>abfss://</c>, ...), read into <typeparamref name="T"/>.
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
        var spec = new DatasetSpec(location, "Dataset.Open", null, options.ColumnNaming);
        return new ParquetDataset<T>(new ParquetDatasetSettings(null, storage, location, spec, null, TempStreams.File, MissingIsEmpty: false), null);
    }
}

internal enum FileFormat { Unknown, Parquet, Avro, Arrow }

/// <summary>The data files of a dataset: hidden files (<c>_SUCCESS</c>, <c>.crc</c>, <c>_temporary/</c>) and unknown formats left out, sorted by path.</summary>
internal static class DatasetFiles
{
    public static async Task<List<LakeFile>> ListAsync(ILakeStorage storage, string location, CancellationToken ct)
    {
        var files = new List<LakeFile>();
        await foreach (var file in storage.ListAsync(location, ct).ConfigureAwait(false))
            if (!HivePartitions.IsHidden(file.RelativePath) && FormatOf(file.RelativePath) != FileFormat.Unknown) files.Add(file);
        files.Sort((a, b) => string.CompareOrdinal(a.RelativePath, b.RelativePath));
        return files;
    }

    public static FileFormat FormatOf(string path) =>
        path.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".parq", StringComparison.OrdinalIgnoreCase) ? FileFormat.Parquet
        : path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase) ? FileFormat.Avro
        : path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".feather", StringComparison.OrdinalIgnoreCase)
          || path.EndsWith(".ipc", StringComparison.OrdinalIgnoreCase) ? FileFormat.Arrow
        : FileFormat.Unknown;
}
