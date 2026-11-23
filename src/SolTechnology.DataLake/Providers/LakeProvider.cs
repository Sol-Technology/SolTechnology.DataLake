using System.Text.Json;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Providers;

/// <summary>A storage technology behind a <see cref="LakeContext"/>. Internal until Iceberg proves the shape.</summary>
internal abstract class LakeProvider
{
    /// <summary>For messages and telemetry: <c>files</c>, <c>in-memory</c>.</summary>
    public abstract string Name { get; }

    public abstract IDataset<T> Dataset<T>(DatasetSpec spec) where T : class;

    public abstract Task EnsureCreatedAsync(LakeModel model, CancellationToken ct);
}

/// <summary>A dataset as the context asks for it: its name, where it comes from, and its declaration if the model has one.</summary>
internal sealed record DatasetSpec(string Name, string Context, DatasetModel? Declared, JsonNamingPolicy? Naming)
{
    public IReadOnlyList<System.Reflection.PropertyInfo> PartitionBy => Declared?.PartitionBy ?? [];
}

/// <summary>
/// A folder of files per dataset under one root, on any <see cref="ILakeStorage"/>: Parquet written, Parquet and the
/// added formats read. <paramref name="method"/> is the builder method, for messages.
/// </summary>
internal sealed class FilesProvider(string name, string method, string root, ILakeStorage storage, FilesOptions options, Func<Stream> tempStream) : LakeProvider
{
    private readonly FileFormats _formats = options.Formats(format => $"call f.{format} in {method}");

    public override string Name => name;

    public override IDataset<T> Dataset<T>(DatasetSpec spec) =>
        new FilesDataset<T>(new FilesDatasetSettings(this, storage, $"{root.TrimEnd('/', '\\')}/{spec.Name}", spec, _formats, options.ParquetSettings, tempStream,
            MissingIsEmpty: true), null);

    // A folder exists once a file is written into it.
    public override Task EnsureCreatedAsync(LakeModel model, CancellationToken ct) => Task.CompletedTask;
}

/// <summary>Where the provider keeps temporary files while a Parquet file is being written (it needs a seekable stream).</summary>
internal static class TempStreams
{
    public static Stream File() =>
        new FileStream(Path.Combine(Path.GetTempPath(), $"datalake-{Guid.NewGuid():N}.parquet"), FileMode.CreateNew,
            FileAccess.ReadWrite, FileShare.None, 1 << 16, FileOptions.DeleteOnClose | FileOptions.Asynchronous);

    public static Stream Memory() => new MemoryStream();
}
