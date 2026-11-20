using System.Text.Json;
using SolTechnology.DataLake.Providers;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>
/// The configuration of a <see cref="LakeContext"/>: one provider (the storage technology) and the settings every
/// provider shares. Build it with <see cref="For{TContext}"/> or through <c>services.AddDataLake&lt;TContext&gt;(...)</c>.
/// </summary>
public abstract class LakeContextOptions
{
    private protected LakeContextOptions(LakeContextOptionsBuilder builder)
    {
        Provider = builder.BuildProvider();
        Naming = builder.Naming;
    }

    internal LakeProvider Provider { get; }
    internal JsonNamingPolicy? Naming { get; }

    /// <summary>Options for <typeparamref name="TContext"/>, for creating a context without dependency injection.</summary>
    public static LakeContextOptions<TContext> For<TContext>(Action<LakeContextOptionsBuilder> configure) where TContext : LakeContext
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new LakeContextOptionsBuilder();
        configure(builder);
        return new LakeContextOptions<TContext>(builder);
    }
}

/// <summary>Options of one context class; dependency injection tells two contexts apart by this type.</summary>
public sealed class LakeContextOptions<TContext> : LakeContextOptions where TContext : LakeContext
{
    internal LakeContextOptions(LakeContextOptionsBuilder builder) : base(builder) { }
}

/// <summary>Chooses the provider of a context and the settings all providers share.</summary>
public sealed class LakeContextOptionsBuilder
{
    private Func<LakeProvider>? _provider;

    internal JsonNamingPolicy? Naming { get; private set; }

    /// <summary>
    /// Parquet files in folders under <paramref name="root"/>, one folder per dataset (<c>root/orders/</c>), partitioned
    /// hive-style. <paramref name="storage"/> is needed for anything but a local path: <c>S3Storage</c>, <c>AzureBlobStorage</c>.
    /// </summary>
    public LakeContextOptionsBuilder UseParquet(string root, ILakeStorage? storage = null, Action<ParquetProviderOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(root);
        storage ??= LocalStorage.Instance.CanHandle(root) ? LocalStorage.Instance
            : throw new ArgumentException($"'{root}' is not a local path; pass a storage that handles it, e.g. S3Storage or AzureBlobStorage.", nameof(storage));
        if (!storage.CanHandle(root)) throw new ArgumentException($"{storage.GetType().Name} does not handle '{root}'.", nameof(root));
        var options = Configure(configure);
        return Use(() => new ParquetProvider("parquet", root, storage, options, TempStreams.File));
    }

    /// <summary>
    /// The Parquet provider on memory instead of storage: the same writer, reader and rules, no disk. For tests. Pass one
    /// <paramref name="storage"/> to several contexts to share the data, or to look at the files written.
    /// </summary>
    public LakeContextOptionsBuilder UseInMemory(MemoryStorage? storage = null, Action<ParquetProviderOptions>? configure = null)
    {
        var options = Configure(configure);
        return Use(() => new ParquetProvider("in-memory", MemoryStorage.Root, storage ?? new MemoryStorage(), options, TempStreams.Memory));
    }

    /// <summary>
    /// How property names become column names without <c>[DataMember(Name = ...)]</c>, e.g. <see cref="JsonNamingPolicy.SnakeCaseLower"/>.
    /// Matching is case-insensitive either way.
    /// </summary>
    public LakeContextOptionsBuilder ColumnNaming(JsonNamingPolicy? naming)
    {
        Naming = naming;
        return this;
    }

    internal LakeProvider BuildProvider() =>
        (_provider ?? throw new InvalidOperationException("No provider configured: call UseParquet(...) or UseInMemory()."))();

    private LakeContextOptionsBuilder Use(Func<LakeProvider> provider)
    {
        if (_provider is not null) throw new InvalidOperationException("A context has one provider; for a second technology add a second context class.");
        _provider = provider;
        return this;
    }

    private static ParquetProviderOptions Configure(Action<ParquetProviderOptions>? configure)
    {
        var options = new ParquetProviderOptions();
        configure?.Invoke(options);
        return options;
    }
}

/// <summary>Settings of the Parquet provider: how files are written.</summary>
public sealed class ParquetProviderOptions
{
    internal ParquetCompression CompressionValue { get; private set; } = ParquetCompression.Snappy;
    internal int RowGroupSizeValue { get; private set; } = 128 * 1024;
    internal long TargetFileSizeValue { get; private set; } = 512L << 20;

    /// <summary>Compression of the column pages. Snappy by default, like Spark and pyarrow.</summary>
    public ParquetProviderOptions Compression(ParquetCompression compression)
    {
        CompressionValue = compression;
        return this;
    }

    /// <summary>Rows per row group, 131 072 by default. Each partition being written keeps this many rows in memory.</summary>
    public ParquetProviderOptions RowGroupSize(int rows)
    {
        RowGroupSizeValue = rows > 0 ? rows : throw new ArgumentOutOfRangeException(nameof(rows), "Must be positive.");
        return this;
    }

    /// <summary>A file is closed and a new one started after the row group that crosses this size, 512 MiB by default.</summary>
    public ParquetProviderOptions TargetFileSize(long bytes)
    {
        TargetFileSizeValue = bytes > 0 ? bytes : throw new ArgumentOutOfRangeException(nameof(bytes), "Must be positive.");
        return this;
    }
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
