using System.Collections.Concurrent;
using SolTechnology.DataLake.Providers;

namespace SolTechnology.DataLake;

/// <summary>
/// The datasets of one storage technology, like EF Core's DbContext but without change tracking: reads stream, writes go
/// straight to storage. Derive from it, declare datasets in <see cref="OnModelCreating"/>, expose them as properties.
/// The provider (Parquet folders, in memory, later Iceberg) is chosen in the options, so the code that reads and writes
/// does not change with it.
/// </summary>
/// <example>
/// <code>
/// public class ShopLake(LakeContextOptions&lt;ShopLake&gt; options) : LakeContext(options), IShopLake
/// {
///     public IDataset&lt;Order&gt; Orders => Dataset&lt;Order&gt;("orders");
///
///     protected override void OnModelCreating(LakeModelBuilder model) =>
///         model.Dataset&lt;Order&gt;("orders").PartitionBy(o => o.Date, o => o.Country);
/// }
///
/// services.AddDataLake&lt;IShopLake, ShopLake&gt;(o => o.UseParquet("s3://shop/", new S3Storage(s3)));
/// </code>
/// </example>
public abstract class LakeContext : IDataLake
{
    private static readonly ConcurrentDictionary<Type, LakeModel> Models = new();

    private readonly LakeContextOptions _options;
    private readonly Lazy<LakeModel> _model;
    private readonly ConcurrentDictionary<(string Name, Type Row), object> _datasets = new();

    /// <summary>A context with the given options. Derived classes pass their <see cref="LakeContextOptions{TContext}"/>.</summary>
    protected LakeContext(LakeContextOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        // Built on first use, after the derived constructor ran, and cached per type: it does not depend on the options.
        _model = new Lazy<LakeModel>(() => CachesModel ? Models.GetOrAdd(GetType(), _ => BuildModel()) : BuildModel());
    }

    /// <summary>Declares the datasets that can be written, and how they are partitioned.</summary>
    protected virtual void OnModelCreating(LakeModelBuilder model) { }

    /// <inheritdoc />
    public IDataset<T> Dataset<T>(string name) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return (IDataset<T>)_datasets.GetOrAdd((name, typeof(T)), _ => CreateDataset<T>(name));
    }

    /// <summary>
    /// Creates the declared datasets that don't exist yet. Folder-based providers have nothing to create (a folder exists
    /// once a file is written into it); table formats create tables from the model.
    /// </summary>
    public Task EnsureCreatedAsync(CancellationToken cancellationToken = default) =>
        _options.Provider.EnsureCreatedAsync(_model.Value, cancellationToken);

    /// <summary>
    /// A context without a class of its own, for scripts and tools: the provider in <paramref name="configure"/>, the
    /// datasets that can be written in <paramref name="model"/>.
    /// </summary>
    public static LakeContext Create(Action<LakeContextOptionsBuilder> configure, Action<LakeModelBuilder>? model = null) =>
        new InlineLakeContext(LakeContextOptions.For<InlineLakeContext>(configure), model);

    private protected virtual bool CachesModel => true;

    private protected virtual string ContextName => GetType().Name;

    private protected LakeModel BuildModel()
    {
        var builder = new LakeModelBuilder();
        OnModelCreating(builder);
        return builder.Build();
    }

    private IDataset<T> CreateDataset<T>(string name) where T : class
    {
        var declared = _model.Value.Find(name);
        if (declared is not null && declared.RowType != typeof(T))
            throw new InvalidOperationException($"Dataset '{name}' is declared in {ContextName}'s model for {declared.RowType.Name}, not {typeof(T).Name}.");
        var spec = new DatasetSpec(name, ContextName, declared, _options.Naming);
        return _options.Provider.Dataset<T>(spec);
    }
}

internal sealed class InlineLakeContext(LakeContextOptions options, Action<LakeModelBuilder>? model) : LakeContext(options)
{
    protected override void OnModelCreating(LakeModelBuilder builder) => model?.Invoke(builder);

    private protected override bool CachesModel => false;

    private protected override string ContextName => "LakeContext.Create";
}
