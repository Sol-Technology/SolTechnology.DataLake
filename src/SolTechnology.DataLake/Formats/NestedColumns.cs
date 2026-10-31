using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// One leaf column of a nested field, as entries: definition level, repetition level and the value (null when the
/// entry is not defined at the leaf's level). Parquet.Net returns defined values densely; they are spread back here.
/// </summary>
internal sealed class Shredded
{
    private static readonly ConcurrentDictionary<Type, Func<RawColumnData, object?[]>> Boxers = new();

    private Shredded(DataField field, int[]? def, int[]? rep, object?[] values)
    {
        Field = field;
        Def = def;
        Rep = rep;
        Values = values;
    }

    public DataField Field { get; }
    public int[]? Def { get; }
    public int[]? Rep { get; }
    public object?[] Values { get; }
    public int Count => Values.Length;

    public int DefAt(int i) => Def?[i] ?? Field.MaxDefinitionLevel;
    public int RepAt(int i) => Rep?[i] ?? 0;

    public static async Task<Shredded> ReadAsync(ParquetRowGroupReader group, DataField field, CancellationToken ct)
    {
        using var raw = await group.ReadRawColumnDataBaseAsync(field, ct).ConfigureAwait(false);
        // Parquet.Net throws when asked for levels the column does not have.
        var def = field.MaxDefinitionLevel > 0 ? raw.DefinitionLevels.ToArray() : null;
        var rep = field.MaxRepetitionLevel > 0 ? raw.RepetitionLevels.ToArray() : null;
        var dense = Boxers.GetOrAdd(raw.GetType().GenericTypeArguments[0], CreateBoxer)(raw);

        var count = def?.Length ?? rep?.Length ?? dense.Length;
        var values = new object?[count];
        var next = 0;
        for (var i = 0; i < count; i++)
            if ((def?[i] ?? field.MaxDefinitionLevel) == field.MaxDefinitionLevel)
                values[i] = dense[next++];
        return new Shredded(field, def, rep, values);
    }

    private static Func<RawColumnData, object?[]> CreateBoxer(Type element) =>
        typeof(Shredded).GetMethod(nameof(Box), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(element).CreateDelegate<Func<RawColumnData, object?[]>>();

    private static object?[] Box<TValue>(RawColumnData raw) where TValue : struct
    {
        var values = ((RawColumnData<TValue>)raw).Values;
        var boxed = new object?[values.Length];
        for (var i = 0; i < values.Length; i++)
            boxed[i] = values[i] switch
            {
                ReadOnlyMemory<char> chars => chars.ToString(),
                // A view into the reader's pooled buffer, which is returned when the column is disposed: copy it.
                ReadOnlyMemory<byte> bytes => bytes.ToArray(),
                var value => value,
            };
        return boxed;
    }
}

/// <summary>
/// A node of a nested field (struct, list, map, leaf) bound to a CLR type. Assembles one value from the entries of
/// its leaves in [starts, ends), following the Dremel record assembly: definition levels tell null from empty from
/// present, repetition levels split lists into elements.
/// </summary>
internal abstract class NestedNode
{
    /// <summary>Indexes of the leaf columns under this node.</summary>
    public required int[] Leaves { get; init; }

    public abstract object? Assemble(Shredded[] columns, int[] starts, int[] ends);

    protected int ProbeDef(Shredded[] columns, int[] starts) => columns[Leaves[0]].DefAt(starts[Leaves[0]]);
}

internal sealed class LeafNode(DataField field, int column, Type target, Logical logical, string path) : NestedNode
{
    private readonly bool _acceptsNull = !target.IsValueType || Nullable.GetUnderlyingType(target) is not null;

    public override object? Assemble(Shredded[] columns, int[] starts, int[] ends)
    {
        var i = starts[column];
        var value = columns[column].DefAt(i) == field.MaxDefinitionLevel ? columns[column].Values[i] : null;
        if (value is null)
            return _acceptsNull ? null
                : throw new InvalidDataException($"Column '{path}' has a null value, but property type {target.Name} does not accept null. Make it {target.Name}?.");
        return target == typeof(object) ? Untyped(value) : Conversions.ConvertValue(value, target, logical, path);
    }

    private object Untyped(object value) => value switch
    {
        long or int when logical.Time != TimeScale.None => Conversions.ConvertValue(value, typeof(TimeOnly), logical, path)!,
        DateTime dt => DateTime.SpecifyKind(dt, logical.Utc ? DateTimeKind.Utc : DateTimeKind.Unspecified),
        byte[] json when logical.Json => System.Text.Encoding.UTF8.GetString(json),
        _ => value,
    };
}

internal sealed class StructNode(bool nullable, int definedAt, Func<object> create, (NestedNode Node, Action<object, object?>? Set)[] children) : NestedNode
{
    public override object? Assemble(Shredded[] columns, int[] starts, int[] ends)
    {
        if (nullable && ProbeDef(columns, starts) < definedAt) return null;
        var result = create();
        foreach (var (node, set) in children)
        {
            var value = node.Assemble(columns, starts, ends);
            set?.Invoke(result, value);
        }
        return result;
    }
}

/// <summary>A list (LIST group, or a legacy repeated field) or a map (MAP group): a repeated node with elements.</summary>
internal sealed class RepeatedNode(Field field, int elementDef, int nullBelow, Func<IList<object?>, object> finish, NestedNode[] parts) : NestedNode
{
    public override object? Assemble(Shredded[] columns, int[] starts, int[] ends)
    {
        var def = ProbeDef(columns, starts);
        if (def < nullBelow) return null;
        var elements = new List<object?>();
        if (def < elementDef) return finish(elements);

        // Element boundaries per leaf: a new element starts where the repetition level is at most this node's.
        var bounds = new List<int>[Leaves.Length];
        for (var l = 0; l < Leaves.Length; l++)
        {
            var column = columns[Leaves[l]];
            var b = new List<int> { starts[Leaves[l]] };
            for (var i = starts[Leaves[l]] + 1; i < ends[Leaves[l]]; i++)
                if (column.RepAt(i) <= field.MaxRepetitionLevel) b.Add(i);
            bounds[l] = b;
        }

        var subStarts = (int[])starts.Clone();
        var subEnds = (int[])ends.Clone();
        for (var e = 0; e < bounds[0].Count; e++)
        {
            for (var l = 0; l < Leaves.Length; l++)
            {
                subStarts[Leaves[l]] = bounds[l][e];
                subEnds[Leaves[l]] = e + 1 < bounds[l].Count ? bounds[l][e + 1] : ends[Leaves[l]];
            }
            if (parts.Length == 1)
                elements.Add(parts[0].Assemble(columns, subStarts, subEnds));
            else
                elements.Add(new KeyValuePair<object?, object?>(parts[0].Assemble(columns, subStarts, subEnds), parts[1].Assemble(columns, subStarts, subEnds)));
        }
        return finish(elements);
    }
}

/// <summary>Builds the node tree of a nested field for a property type, and checks the types up front.</summary>
internal static class NestedBuilder
{
    private static readonly ConcurrentDictionary<Type, Func<object>> Constructors = new();
    private static readonly ConcurrentDictionary<PropertyInfo, Action<object, object?>> Setters = new();

    // path: logical path for messages, e.g. lines[].sku or attributes.value.
    public static NestedNode Build(Field field, Type target, List<DataField> leaves, JsonNamingPolicy? naming, string location, string path)
    {
        switch (field)
        {
            case DataField { IsArray: false } data:
            {
                var logical = ParquetStatistics.LogicalOf(data);
                var input = ParquetStatistics.ClrOf(data);
                if (target != typeof(object) && !Conversions.CanConvert(input, target, logical))
                    throw new NotSupportedException($"{location}: {Conversions.Mismatch(path, input, target, logical).Message}");
                leaves.Add(data);
                return new LeafNode(data, leaves.Count - 1, target, logical, path) { Leaves = [leaves.Count - 1] };
            }

            case DataField data:
                // A legacy repeated field: an element exists when the entry is defined at the leaf's level; the list is never null.
                return RepeatedField(data, target, -1, location, path);

            case ListField list when list.SchemaElement?.RepetitionType == Parquet.Meta.FieldRepetitionType.REPEATED:
            {
                // A repeated group without the LIST annotation (protobuf style, repeated_group { nested }): a list of
                // structs. Parquet.Net shows the group's one field as the list item.
                var (element, finish) = Collection(target, path, location);
                var item = BuildStruct([list.Item], false, 0, element, leaves, naming, location, path + "[]");
                return new RepeatedNode(list, list.MaxDefinitionLevel, -1, finish, [item]) { Leaves = item.Leaves };
            }

            case ListField { Item: DataField { IsArray: true } repeated } list when list.Path[list.Path.Length - 1] == list.Name:
                // Two-level list from old writers (parquet-mr before 1.9, Spark 1.x): optional group x (LIST) { repeated double array; }.
                // No middle group: the repeated field itself is the element.
                return RepeatedField(repeated, target, list.IsNullable ? list.MaxDefinitionLevel : -1, location, path);

            case ListField list:
            {
                var (element, finish) = Collection(target, path, location);
                var item = Build(list.Item, element, leaves, naming, location, path + "[]");
                return new RepeatedNode(list, list.MaxDefinitionLevel, list.IsNullable ? list.MaxDefinitionLevel - 1 : -1, finish, [item]) { Leaves = item.Leaves };
            }

            case MapField map:
            {
                var (key, value, finish) = Dictionary(target, path, location);
                var keyNode = Build(map.Key, key, leaves, naming, location, path + ".key");
                var valueNode = Build(map.Value, value, leaves, naming, location, path + ".value");
                return new RepeatedNode(map, map.MaxDefinitionLevel, map.IsNullable ? map.MaxDefinitionLevel - 1 : -1, finish, [keyNode, valueNode])
                {
                    Leaves = [.. keyNode.Leaves, .. valueNode.Leaves],
                };
            }

            case StructField structField:
                return BuildStruct(structField.Fields, structField.IsNullable, structField.MaxDefinitionLevel, target, leaves, naming, location, path);

            default:
                throw new NotSupportedException($"{location}: column '{path}' ({field.SchemaType}) is not supported.");
        }

        NestedNode RepeatedField(DataField data, Type target, int nullBelow, string location, string path)
        {
            // An element exists when the entry is defined at the leaf's level.
            var (element, finish) = Collection(target, path, location);
            var logical = ParquetStatistics.LogicalOf(data);
            var input = ParquetStatistics.ClrOf(data);
            if (element != typeof(object) && !Conversions.CanConvert(input, element, logical))
                throw new NotSupportedException($"{location}: {Conversions.Mismatch(path, input, element, logical).Message}");
            leaves.Add(data);
            var leaf = new LeafNode(data, leaves.Count - 1, element, logical, path) { Leaves = [leaves.Count - 1] };
            return new RepeatedNode(data, data.MaxDefinitionLevel, nullBelow, finish, [leaf]) { Leaves = leaf.Leaves };
        }
    }

    private static NestedNode BuildStruct(IReadOnlyList<Field> fields, bool nullable, int definedAt, Type target, List<DataField> leaves, JsonNamingPolicy? naming, string location, string path)
    {
        var children = new List<(NestedNode, Action<object, object?>?)>();

        if (target == typeof(object) || target == typeof(Dictionary<string, object?>) || target == typeof(IDictionary<string, object?>)
            || target == typeof(IReadOnlyDictionary<string, object?>))
        {
            foreach (var child in fields)
            {
                var name = child.Name;
                children.Add((Build(child, typeof(object), leaves, naming, location, $"{path}.{name}"), (o, v) => ((Dictionary<string, object?>)o)[name] = v));
            }
            return new StructNode(nullable, definedAt, () => new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase), [.. children])
            {
                Leaves = children.SelectMany(c => c.Item1.Leaves).ToArray(),
            };
        }

        if (target == typeof(string) || target.IsValueType || CollectionElement(target) is not null)
            throw new NotSupportedException($"{location}: column '{path}' is a struct; read it into a class with properties, not {target.Name}.");

        var properties = ColumnNames.MappedProperties(target)
            .ToDictionary(p => ColumnNames.Of(p, naming), StringComparer.OrdinalIgnoreCase);
        var bound = new HashSet<PropertyInfo>();
        foreach (var child in fields)
        {
            if (properties.TryGetValue(child.Name, out var property))
            {
                children.Add((Build(child, property.PropertyType, leaves, naming, location, $"{path}.{child.Name}"), Setter(property)));
                bound.Add(property);
            }
            else
            {
                // Read anyway: unmapped children still tell whether the struct is null.
                children.Add((Build(child, typeof(object), leaves, naming, location, $"{path}.{child.Name}"), null));
            }
        }

        var missing = properties.Values.FirstOrDefault(p => !bound.Contains(p) && p.PropertyType.IsValueType && Nullable.GetUnderlyingType(p.PropertyType) is null);
        if (missing is not null)
            throw new InvalidDataException($"{location}: struct '{path}' has no field '{ColumnNames.Of(missing, naming)}' for {target.Name}.{missing.Name} ({missing.PropertyType.Name}). Make the property nullable.");

        return new StructNode(nullable, definedAt, Constructor(target, location), [.. children]) { Leaves = children.SelectMany(c => c.Item1.Leaves).ToArray() };
    }

    public static Type? CollectionElement(Type type)
    {
        if (type.IsArray) return type.GetElementType();
        if (!type.IsGenericType) return null;
        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(ICollection<>) || definition == typeof(IEnumerable<>)
               || definition == typeof(IReadOnlyList<>) || definition == typeof(IReadOnlyCollection<>)
            ? type.GetGenericArguments()[0]
            : null;
    }

    private static (Type Element, Func<IList<object?>, object> Finish) Collection(Type target, string path, string location)
    {
        if (target == typeof(object)) return (typeof(object), items => items.ToList());
        var element = CollectionElement(target)
            ?? throw new NotSupportedException($"{location}: column '{path}' is a list; read it into T[], List<T> or IReadOnlyList<T>, not {target.Name}.");
        var list = typeof(List<>).MakeGenericType(element);
        return (element, items =>
        {
            var typed = (IList)Activator.CreateInstance(list, items.Count)!;
            foreach (var item in items) typed.Add(item);
            if (!target.IsArray) return typed;
            var array = Array.CreateInstance(element, typed.Count);
            typed.CopyTo(array, 0);
            return array;
        });
    }

    private static (Type Key, Type Value, Func<IList<object?>, object> Finish) Dictionary(Type target, string path, string location)
    {
        if (target == typeof(object))
            return (typeof(object), typeof(object), items => items.Cast<KeyValuePair<object?, object?>>().ToDictionary(kv => kv.Key!, kv => kv.Value));

        var definition = target.IsGenericType ? target.GetGenericTypeDefinition() : null;
        if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>) && definition != typeof(IReadOnlyDictionary<,>))
            throw new NotSupportedException($"{location}: column '{path}' is a map; read it into Dictionary<K, V> or IReadOnlyDictionary<K, V>, not {target.Name}.");

        var args = target.GetGenericArguments();
        var dictionary = typeof(Dictionary<,>).MakeGenericType(args);
        return (args[0], args[1], items =>
        {
            var typed = (IDictionary)Activator.CreateInstance(dictionary)!;
            foreach (var (key, value) in items.Cast<KeyValuePair<object?, object?>>())
                typed[key ?? throw new InvalidDataException($"Map '{path}' has a null key.")] = value;
            return typed;
        });
    }

    private static Func<object> Constructor(Type type, string location) => Constructors.GetOrAdd(type, t =>
    {
        var constructor = t.GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException($"{location}: {t.Name} needs a public parameterless constructor.");
        return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(constructor), typeof(object))).Compile();
    });

    private static Action<object, object?> Setter(PropertyInfo property) => Setters.GetOrAdd(property, p =>
    {
        var target = Expression.Parameter(typeof(object));
        var value = Expression.Parameter(typeof(object));
        var assign = Expression.Assign(
            Expression.Property(Expression.Convert(target, p.DeclaringType!), p),
            Expression.Convert(value, p.PropertyType));
        return Expression.Lambda<Action<object, object?>>(assign, target, value).Compile();
    });
}

/// <summary>A top-level nested column assigned to one property of the rows.</summary>
internal sealed class NestedBinding<T>(PropertyMap<T> property, NestedNode root, DataField[] leaves) where T : class
{
    public PropertyMap<T> Property { get; } = property;

    public static NestedBinding<T> Create(Field field, PropertyMap<T> property, JsonNamingPolicy? naming, string location)
    {
        var leaves = new List<DataField>();
        var root = NestedBuilder.Build(field, property.Type, leaves, naming, location, field.Name);
        return new NestedBinding<T>(property, root, [.. leaves]);
    }

    public async Task AssignAsync(ParquetRowGroupReader group, T[] rows, CancellationToken ct)
    {
        var values = await NestedColumn.AssembleAsync(group, root, leaves, rows.Length, ct).ConfigureAwait(false);
        for (var r = 0; r < rows.Length; r++) Property.BoxedSetter(rows[r], values[r]);
    }
}

/// <summary>A nested (or any) top-level column of one row group, assembled into one value per row.</summary>
internal static class NestedColumn
{
    public static async Task<object?[]> AssembleAsync(ParquetRowGroupReader group, NestedNode root, DataField[] leaves, int rowCount, CancellationToken ct)
    {
        var columns = new Shredded[leaves.Length];
        for (var l = 0; l < leaves.Length; l++) columns[l] = await Shredded.ReadAsync(group, leaves[l], ct).ConfigureAwait(false);

        // Row r of leaf l starts at its r-th entry with repetition level 0.
        var rowStarts = new int[leaves.Length][];
        for (var l = 0; l < leaves.Length; l++)
        {
            var starts = new List<int>(rowCount);
            if (columns[l].Count > 0 && columns[l].RepAt(0) != 0)
                throw new InvalidDataException($"Column '{leaves[l].Path}' starts in the middle of a row (repetition level {columns[l].RepAt(0)}); the file is corrupt.");
            for (var i = 0; i < columns[l].Count; i++)
                if (columns[l].RepAt(i) == 0) starts.Add(i);
            if (starts.Count != rowCount)
                throw new InvalidDataException($"Column '{leaves[l].Path}' has {starts.Count} rows in a row group of {rowCount}.");
            rowStarts[l] = [.. starts];
        }

        var values = new object?[rowCount];
        var from = new int[leaves.Length];
        var to = new int[leaves.Length];
        for (var r = 0; r < rowCount; r++)
        {
            for (var l = 0; l < leaves.Length; l++)
            {
                from[l] = rowStarts[l][r];
                to[l] = r + 1 < rowCount ? rowStarts[l][r + 1] : columns[l].Count;
            }
            values[r] = root.Assemble(columns, from, to);
        }
        return values;
    }
}
