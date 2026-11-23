using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using SolTechnology.Avro;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Avro;

/// <summary>
/// Avro object container files. AvroConvert decodes each record generically (records as dictionaries,
/// <c>timestamp-*</c> as UTC <see cref="DateTime"/>, <c>local-timestamp-*</c> as unspecified); the mapping to
/// <typeparamref name="T"/> is the same as for Parquet: names, nullability, timestamps, partitions.
/// Avro has no statistics, so only partitions skip files.
/// </summary>
internal static class AvroFile<T> where T : class
{
    public static async IAsyncEnumerable<T> ReadAsync(Stream stream, string location, FileScan<T> scan, [EnumeratorCancellation] CancellationToken ct)
    {
        var fields = SchemaFields(AvroConvert.GetSchema(stream), location);
        stream.Position = 0;

        var assignments = new List<(string Field, PropertyMap<T> Property)>();
        foreach (var field in fields)
            if (scan.Map.Find(field) is { } property && !scan.Partitions.ContainsKey(property))
                assignments.Add((field, property));

        var missing = scan.Map.Properties.FirstOrDefault(p => !p.AcceptsNull && !scan.Partitions.ContainsKey(p) && assignments.All(a => a.Property != p));
        if (missing is not null)
            throw new InvalidDataException($"{location} has no column '{missing.Column}' for {typeof(T).Name}.{missing.Property.Name} ({missing.Type.Name}). " +
                                           $"Make the property nullable to read files without it, or mark it [IgnoreDataMember].");

        await foreach (var record in AvroConvert.Deserialize<object>(stream, cancellationToken: ct).ConfigureAwait(false))
        {
            var values = (IDictionary<string, object?>)record;
            var row = scan.Map.Create();
            foreach (var (field, property) in assignments)
                property.BoxedSetter(row, ObjectMapper.Map(values.TryGetValue(field, out var value) ? value : null, property.Type, scan.Map.Naming, field));
            foreach (var (property, value) in scan.Partitions) property.BoxedSetter(row, value);

            scan.Metrics.RowsRead++;
            if (scan.Residual is not null && !scan.Residual(row)) continue;
            scan.Metrics.RowsReturned++;
            yield return row;
        }
    }

    private static List<string> SchemaFields(string schema, string location)
    {
        using var document = JsonDocument.Parse(schema);
        if (!document.RootElement.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array)
            throw new NotSupportedException($"{location}: the Avro schema is not a record.");
        return fields.EnumerateArray().Select(f => f.GetProperty("name").GetString()!).ToList();
    }
}

/// <summary>Generic values (dictionaries, lists, primitives) into a CLR type, with the same rules as Parquet columns.</summary>
internal static class ObjectMapper
{
    private static readonly ConcurrentDictionary<(Type, JsonNamingPolicy?), Dictionary<string, (PropertyInfo Property, Action<object, object?> Set)>> Properties = new();
    private static readonly ConcurrentDictionary<Type, Func<object>> Constructors = new();

    public static object? Map(object? value, Type target, JsonNamingPolicy? naming, string path)
    {
        if (value is null)
            return !target.IsValueType || Nullable.GetUnderlyingType(target) is not null ? null
                : throw new InvalidDataException($"Column '{path}' has a null value, but property type {target.Name} does not accept null. Make it {target.Name}?.");
        if (target == typeof(object)) return value;

        switch (value)
        {
            case IDictionary<string, object?> record when value is System.Dynamic.ExpandoObject:
                return Record(record, target, naming, path);
            case IDictionary map:
                return Map(map, target, naming, path);
            case IEnumerable items when value is not string and not byte[]:
                return List(items, target, naming, path);
            default:
                return Conversions.ConvertValue(value, target, new Logical(value is DateTime { Kind: DateTimeKind.Utc }), path);
        }
    }

    private static object Record(IDictionary<string, object?> record, Type target, JsonNamingPolicy? naming, string path)
    {
        if (target == typeof(Dictionary<string, object?>) || target == typeof(IDictionary<string, object?>) || target == typeof(IReadOnlyDictionary<string, object?>))
            return new Dictionary<string, object?>(record, StringComparer.OrdinalIgnoreCase);
        if (target == typeof(string) || target.IsValueType || NestedBuilder.CollectionElement(target) is not null)
            throw new NotSupportedException($"Column '{path}' is a record; read it into a class with properties, not {target.Name}.");

        var result = Constructors.GetOrAdd(target, t =>
        {
            var constructor = t.GetConstructor(Type.EmptyTypes) ?? throw new NotSupportedException($"{t.Name} needs a public parameterless constructor.");
            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(constructor), typeof(object))).Compile();
        })();
        var properties = Properties.GetOrAdd((target, naming), key => ColumnNames.MappedProperties(key.Item1)
            .ToDictionary(p => ColumnNames.Of(p, key.Item2), p => (p, Setter(p)), StringComparer.OrdinalIgnoreCase));

        foreach (var (name, (property, set)) in properties)
        {
            var found = record.FirstOrDefault(kv => string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase));
            if (found.Key is null)
            {
                if (property.PropertyType.IsValueType && Nullable.GetUnderlyingType(property.PropertyType) is null)
                    throw new InvalidDataException($"Record '{path}' has no field '{name}' for {target.Name}.{property.Name} ({property.PropertyType.Name}). Make the property nullable.");
                continue;
            }
            set(result, Map(found.Value, property.PropertyType, naming, $"{path}.{found.Key}"));
        }
        return result;
    }

    private static object Map(IDictionary map, Type target, JsonNamingPolicy? naming, string path)
    {
        var definition = target.IsGenericType ? target.GetGenericTypeDefinition() : null;
        if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>) && definition != typeof(IReadOnlyDictionary<,>))
            throw new NotSupportedException($"Column '{path}' is a map; read it into Dictionary<K, V> or IReadOnlyDictionary<K, V>, not {target.Name}.");
        var args = target.GetGenericArguments();
        var result = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(args))!;
        foreach (DictionaryEntry entry in map)
            result[Map(entry.Key, args[0], naming, path + ".key")!] = Map(entry.Value, args[1], naming, path + ".value");
        return result;
    }

    private static object List(IEnumerable items, Type target, JsonNamingPolicy? naming, string path)
    {
        var element = NestedBuilder.CollectionElement(target)
            ?? throw new NotSupportedException($"Column '{path}' is a list; read it into T[], List<T> or IReadOnlyList<T>, not {target.Name}.");
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(element))!;
        foreach (var item in items) list.Add(Map(item, element, naming, path + "[]"));
        if (!target.IsArray) return list;
        var array = Array.CreateInstance(element, list.Count);
        list.CopyTo(array, 0);
        return array;
    }

    private static Action<object, object?> Setter(PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(object));
        var value = Expression.Parameter(typeof(object));
        var assign = Expression.Assign(Expression.Property(Expression.Convert(target, property.DeclaringType!), property), Expression.Convert(value, property.PropertyType));
        return Expression.Lambda<Action<object, object?>>(assign, target, value).Compile();
    }
}
