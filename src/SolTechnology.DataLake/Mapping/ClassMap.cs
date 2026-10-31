using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.Json;

namespace SolTechnology.DataLake.Mapping;

/// <summary>A writable property of <typeparamref name="T"/> and the column it reads from.</summary>
internal sealed class PropertyMap<T>(PropertyInfo property, string column)
{
    public PropertyInfo Property { get; } = property;
    public string Column { get; } = column;
    public Type Type => Property.PropertyType;

    /// <summary>Whether a missing or null value is acceptable (reference type or <see cref="Nullable{T}"/>).</summary>
    public bool AcceptsNull { get; } = !property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) is not null;

    // (T target, TProperty value) => target.Property = value, compiled once.
    public Delegate Setter { get; } = CompileSetter(property);

    // Boxed variant for values known once per file (partition values).
    public Action<T, object?> BoxedSetter { get; } = CompileBoxedSetter(property);

    private static Delegate CompileSetter(PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(T), "target");
        var value = Expression.Parameter(property.PropertyType, "value");
        return Expression.Lambda(typeof(Action<,>).MakeGenericType(typeof(T), property.PropertyType),
            Expression.Assign(Expression.Property(target, property), value), target, value).Compile();
    }

    private static Action<T, object?> CompileBoxedSetter(PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(T), "target");
        var value = Expression.Parameter(typeof(object), "value");
        var assign = Expression.Assign(Expression.Property(target, property), Expression.Convert(value, property.PropertyType));
        return Expression.Lambda<Action<T, object?>>(assign, target, value).Compile();
    }
}

/// <summary>How <typeparamref name="T"/> maps to columns. One per type and naming policy.</summary>
internal sealed class ClassMap<T> where T : class
{
    private static readonly ConcurrentDictionary<JsonNamingPolicy, ClassMap<T>> ByPolicy = new();
    private static readonly Lazy<ClassMap<T>> WithoutPolicy = new(() => new ClassMap<T>(null));

    private readonly Dictionary<string, PropertyMap<T>> _byColumn;

    public Func<T> Create { get; }
    public JsonNamingPolicy? Naming { get; }
    public IReadOnlyList<PropertyMap<T>> Properties { get; }

    public static ClassMap<T> For(JsonNamingPolicy? naming) =>
        naming is null ? WithoutPolicy.Value : ByPolicy.GetOrAdd(naming, n => new ClassMap<T>(n));

    private ClassMap(JsonNamingPolicy? naming)
    {
        Naming = naming;
        var constructor = typeof(T).GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException($"{typeof(T).Name} needs a public parameterless constructor.");
        Create = Expression.Lambda<Func<T>>(Expression.New(constructor)).Compile();

        Properties = ColumnNames.MappedProperties(typeof(T))
            .Select(p => new PropertyMap<T>(p, ColumnName(p, naming)))
            .ToList();
        _byColumn = new Dictionary<string, PropertyMap<T>>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in Properties)
            if (!_byColumn.TryAdd(property.Column, property))
                throw new NotSupportedException($"{typeof(T).Name}: properties {_byColumn[property.Column].Property.Name} and {property.Property.Name} both map to column '{property.Column}'.");
    }

    public PropertyMap<T>? Find(string column) => _byColumn.GetValueOrDefault(column);

    public PropertyMap<T> Property(string propertyName) =>
        Properties.Single(p => p.Property.Name == propertyName);

    private static string ColumnName(PropertyInfo property, JsonNamingPolicy? naming) => ColumnNames.Of(property, naming);
}

internal static class ColumnNames
{
    /// <summary><c>[DataMember(Name = ...)]</c>, else the naming policy, else the property name.</summary>
    public static string Of(PropertyInfo property, JsonNamingPolicy? naming) =>
        property.GetCustomAttribute<DataMemberAttribute>()?.Name is { Length: > 0 } name ? name
        : naming?.ConvertName(property.Name) ?? property.Name;

    /// <summary>Public settable properties that map to columns.</summary>
    public static IEnumerable<PropertyInfo> MappedProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is { IsPublic: true } && p.GetIndexParameters().Length == 0)
            .Where(p => p.GetCustomAttribute<IgnoreDataMemberAttribute>() is null);
}
