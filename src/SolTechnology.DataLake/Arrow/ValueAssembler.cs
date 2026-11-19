using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Arrow;

/// <summary>
/// A plain value (<see cref="ArrowValues"/>) into a property type: lists into arrays and lists, structs into classes,
/// maps into dictionaries, single values through <see cref="Conversions"/>, with the same rules as Parquet columns.
/// </summary>
internal static class ValueAssembler
{
    private static readonly ConcurrentDictionary<Type, Func<object>> Constructors = new();
    private static readonly ConcurrentDictionary<(Type, JsonNamingPolicy?), Dictionary<string, (Type Type, Action<object, object?> Set)>> Properties = new();

    public static object? Convert(object? value, Type target, JsonNamingPolicy? naming, string path)
    {
        if (value is null)
        {
            return !target.IsValueType || Nullable.GetUnderlyingType(target) is not null ? null
                : throw new InvalidDataException($"Column '{path}' has a null value, but property type {target.Name} does not accept null. Make it {target.Name}?.");
        }
        if (target == typeof(object)) return value;

        switch (value)
        {
            case List<KeyValuePair<object?, object?>> pairs:
                return Dictionary(pairs, target, naming, path);
            case List<object?> items:
                return Collection(items, target, naming, path);
            case Dictionary<string, object?> fields:
                return Struct(fields, target, naming, path);
            default:
                var logical = new Logical(value is DateTime { Kind: DateTimeKind.Utc });
                return Conversions.ConvertValue(value, target, logical, path);
        }
    }

    private static object Collection(List<object?> items, Type target, JsonNamingPolicy? naming, string path)
    {
        var element = NestedBuilder.CollectionElement(target)
                      ?? throw new NotSupportedException($"Column '{path}' is a list; read it into T[], List<T> or IReadOnlyList<T>, not {target.Name}.");
        if (target.IsArray)
        {
            var array = System.Array.CreateInstance(element, items.Count);
            for (var i = 0; i < items.Count; i++) array.SetValue(Convert(items[i], element, naming, path + "[]"), i);
            return array;
        }
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(element), items.Count)!;
        foreach (var item in items) list.Add(Convert(item, element, naming, path + "[]"));
        return list;
    }

    private static object Dictionary(List<KeyValuePair<object?, object?>> pairs, Type target, JsonNamingPolicy? naming, string path)
    {
        var definition = target.IsGenericType ? target.GetGenericTypeDefinition() : null;
        if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>) && definition != typeof(IReadOnlyDictionary<,>))
            throw new NotSupportedException($"Column '{path}' is a map; read it into Dictionary<K, V> or IReadOnlyDictionary<K, V>, not {target.Name}.");
        var args = target.GetGenericArguments();
        var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(args))!;
        foreach (var (key, value) in pairs)
            dictionary[Convert(key ?? throw new InvalidDataException($"Map '{path}' has a null key."), args[0], naming, path + ".key")!] =
                Convert(value, args[1], naming, path + ".value");
        return dictionary;
    }

    private static object Struct(Dictionary<string, object?> fields, Type target, JsonNamingPolicy? naming, string path)
    {
        if (target == typeof(Dictionary<string, object?>) || target == typeof(IDictionary<string, object?>) || target == typeof(IReadOnlyDictionary<string, object?>))
            return fields;
        if (target == typeof(string) || target.IsValueType || NestedBuilder.CollectionElement(target) is not null)
            throw new NotSupportedException($"Column '{path}' is a struct; read it into a class with properties, not {target.Name}.");

        var result = Constructor(target)();
        foreach (var (column, (type, set)) in PropertiesOf(target, naming))
        {
            if (fields.TryGetValue(column, out var value)) set(result, Convert(value, type, naming, $"{path}.{column}"));
            else if (type.IsValueType && Nullable.GetUnderlyingType(type) is null)
                throw new InvalidDataException($"Struct '{path}' has no field '{column}' for {target.Name} ({type.Name}). Make the property nullable.");
        }
        return result;
    }

    private static Dictionary<string, (Type, Action<object, object?>)> PropertiesOf(Type type, JsonNamingPolicy? naming) =>
        Properties.GetOrAdd((type, naming), key => ColumnNames.MappedProperties(key.Item1).ToDictionary(
            p => ColumnNames.Of(p, key.Item2),
            p => (p.PropertyType, Setter(p)),
            StringComparer.OrdinalIgnoreCase));

    private static Func<object> Constructor(Type type) => Constructors.GetOrAdd(type, t =>
    {
        var constructor = t.GetConstructor(Type.EmptyTypes) ?? throw new NotSupportedException($"{t.Name} needs a public parameterless constructor.");
        return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(constructor), typeof(object))).Compile();
    });

    private static Action<object, object?> Setter(PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(object));
        var value = Expression.Parameter(typeof(object));
        return Expression.Lambda<Action<object, object?>>(
            Expression.Assign(Expression.Property(Expression.Convert(target, property.DeclaringType!), property), Expression.Convert(value, property.PropertyType)),
            target, value).Compile();
    }
}
