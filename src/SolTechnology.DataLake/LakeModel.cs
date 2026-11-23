using System.Linq.Expressions;
using System.Reflection;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake;

/// <summary>Declares the datasets of a context in <see cref="LakeContext.OnModelCreating"/>.</summary>
public sealed class LakeModelBuilder
{
    private readonly Dictionary<string, IDatasetBuilder> _datasets = new(StringComparer.Ordinal);

    internal LakeModelBuilder() { }

    /// <summary>The dataset called <paramref name="name"/>, with rows of <typeparamref name="T"/>. Declared datasets can be written.</summary>
    public DatasetBuilder<T> Dataset<T>(string name) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (!_datasets.TryGetValue(name, out var existing)) _datasets[name] = existing = new DatasetBuilder<T>(name);
        return existing as DatasetBuilder<T>
               ?? throw new InvalidOperationException($"Dataset '{name}' is already declared for {existing.RowType.Name}, not {typeof(T).Name}.");
    }

    internal LakeModel Build() => new(_datasets.Values.Select(d => d.Build()).ToDictionary(d => d.Name, StringComparer.Ordinal));
}

internal interface IDatasetBuilder
{
    Type RowType { get; }
    DatasetModel Build();
}

/// <summary>One dataset in the model.</summary>
public sealed class DatasetBuilder<T> : IDatasetBuilder where T : class
{
    private readonly string _name;
    private PropertyInfo[] _partitionBy = [];
    private string[] _namespace = [];

    internal DatasetBuilder(string name) => _name = name;

    Type IDatasetBuilder.RowType => typeof(T);

    /// <summary>
    /// Properties the dataset is partitioned by, in this order. Used when the dataset is created; an existing dataset
    /// partitioned differently is an error on write, not silently rewritten.
    /// </summary>
    public DatasetBuilder<T> PartitionBy(params Expression<Func<T, object?>>[] properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var mapped = ColumnNames.MappedProperties(typeof(T)).ToList();
        _partitionBy = properties.Select(p => Property(p, mapped)).ToArray();
        if (_partitionBy.Distinct().Count() != _partitionBy.Length)
            throw new ArgumentException($"Dataset '{_name}': PartitionBy lists a property twice.", nameof(properties));
        return this;
    }

    /// <summary>The namespace of the dataset in a catalog (Iceberg). Folder-based providers ignore it.</summary>
    public DatasetBuilder<T> InNamespace(params string[] parts)
    {
        ArgumentNullException.ThrowIfNull(parts);
        if (parts.Any(string.IsNullOrWhiteSpace)) throw new ArgumentException("Namespace parts can't be empty.", nameof(parts));
        _namespace = parts;
        return this;
    }

    DatasetModel IDatasetBuilder.Build() => new(_name, typeof(T), _partitionBy, _namespace);

    // The expression's property is reflected on the class that declares it, maybe a base class, so it is matched by name;
    // when a property is hidden with 'new', its declaring class tells the two apart.
    private PropertyInfo Property(Expression<Func<T, object?>> expression, List<PropertyInfo> mapped)
    {
        var body = expression.Body is UnaryExpression { NodeType: ExpressionType.Convert } convert ? convert.Operand : expression.Body;
        if (body is MemberExpression { Member: PropertyInfo property } member && member.Expression == expression.Parameters[0]
            && (mapped.FirstOrDefault(m => m.Name == property.Name && m.DeclaringType == property.DeclaringType)
                ?? mapped.FirstOrDefault(m => m.Name == property.Name)) is { } match)
            return match;
        throw new ArgumentException($"Dataset '{_name}': PartitionBy takes properties of {typeof(T).Name} that map to columns, like o => o.Date; got {expression}.");
    }
}

/// <summary>The datasets a context declares. Built once per context type; independent of the provider and naming.</summary>
internal sealed class LakeModel(IReadOnlyDictionary<string, DatasetModel> datasets)
{
    public DatasetModel? Find(string name) => datasets.GetValueOrDefault(name);

    public IEnumerable<DatasetModel> Datasets => datasets.Values;
}

internal sealed record DatasetModel(string Name, Type RowType, IReadOnlyList<PropertyInfo> PartitionBy, IReadOnlyList<string> Namespace);
