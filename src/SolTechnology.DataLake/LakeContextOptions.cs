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
    /// A folder of files per dataset under <paramref name="root"/> (<c>root/orders/</c>), partitioned hive-style, without
    /// transactions. Writes are Parquet; reads take Parquet and the formats added in <paramref name="configure"/>, e.g.
    /// <c>f => f.Avro()</c>. <paramref name="storage"/> is needed for anything but a local path: <c>S3Storage</c>, <c>AzureBlobStorage</c>.
    /// </summary>
    public LakeContextOptionsBuilder UseFiles(string root, ILakeStorage? storage = null, Action<FilesOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(root);
        storage ??= LocalStorage.Instance.CanHandle(root) ? LocalStorage.Instance
            : throw new ArgumentException($"'{root}' is not a local path; pass a storage that handles it, e.g. S3Storage or AzureBlobStorage.", nameof(storage));
        if (!storage.CanHandle(root)) throw new ArgumentException($"{storage.GetType().Name} does not handle '{root}'.", nameof(root));
        if (storage is LocalStorage) root = LocalStorage.RootPath(root);
        var options = FilesOptions.From(configure);
        return Use(() => new FilesProvider("files", "UseFiles", root, storage, options, TempStreams.File));
    }

    /// <summary>
    /// <see cref="UseFiles"/> on memory instead of storage: the same writer, reader and rules, no disk. For tests. Pass one
    /// <paramref name="storage"/> to several contexts to share the data, or to look at the files written.
    /// </summary>
    public LakeContextOptionsBuilder UseInMemory(MemoryStorage? storage = null, Action<FilesOptions>? configure = null)
    {
        var options = FilesOptions.From(configure);
        return Use(() => new FilesProvider("in-memory", "UseInMemory", MemoryStorage.Root, storage ?? new MemoryStorage(), options, TempStreams.Memory));
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
        (_provider ?? throw new InvalidOperationException("No provider configured: call UseFiles(...) or UseInMemory()."))();

    private LakeContextOptionsBuilder Use(Func<LakeProvider> provider)
    {
        if (_provider is not null) throw new InvalidOperationException("A context has one provider; for a second technology add a second context class.");
        _provider = provider;
        return this;
    }
}
