using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Writing;

/// <summary>Min and max of the INT64 timestamp columns of one row group, by leaf column index.</summary>
internal sealed class RowGroupMinMax
{
    public Dictionary<int, (long Min, long Max)> Columns { get; } = [];

    public void Add(int column, IEnumerable<DateTime> stored)
    {
        long? min = null, max = null;
        foreach (var value in stored)
        {
            var micros = ValueKind.Micros(value);
            if (min is null || micros < min) min = micros;
            if (max is null || micros > max) max = micros;
        }
        if (min is not null) Columns[column] = (min.Value, max!.Value);
    }
}

/// <summary>One top-level column of <typeparamref name="T"/>: its Parquet field and how to write a row group of it.</summary>
internal abstract class ColumnWriter<T>
{
    public abstract Field Field { get; }

    /// <summary>Called once the schema exists: leaf indexes and definition / repetition levels are known then.</summary>
    public abstract void Bind(ParquetSchema schema);

    public abstract Task WriteAsync(ParquetRowGroupWriter group, IReadOnlyList<T> rows, RowGroupMinMax minMax, CancellationToken ct);

    public static ColumnWriter<T> Create(PropertyInfo property, string column, JsonNamingPolicy? naming)
    {
        var type = property.PropertyType;
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        if (ValueKinds.For(underlying, property, column) is { } kind)
        {
            var slot = kind.Store.IsValueType && (type != underlying || !type.IsValueType) ? typeof(Nullable<>).MakeGenericType(kind.Store) : kind.Store;
            var writer = typeof(FlatColumn<,>).MakeGenericType(typeof(T), slot);
            return (ColumnWriter<T>)Activator.CreateInstance(writer, property, column, kind)!;
        }
        return new NestedColumnWriter<T>(property, column, naming);
    }

}

internal static class SchemaLeaves
{
    /// <summary>Index of <paramref name="leaf"/> among the schema's leaf columns, which is its index in each row group.</summary>
    public static int IndexOf(ParquetSchema schema, DataField leaf)
    {
        var fields = schema.DataFields;
        for (var i = 0; i < fields.Length; i++)
            if (ReferenceEquals(fields[i], leaf)) return i;
        throw new InvalidOperationException($"Column '{leaf.Path}' is not in the schema.");
    }
}

/// <summary>
/// A column of single values. <typeparamref name="TSlot"/> is what Parquet.Net takes: the stored type, or its
/// <see cref="Nullable{T}"/> when the property accepts null. Values are read with a compiled getter into one array.
/// </summary>
internal sealed class FlatColumn<T, TSlot> : ColumnWriter<T>
{
    private static readonly Func<ParquetRowGroupWriter, DataField, TSlot[], CancellationToken, Task> Write = CreateWrite();

    private readonly DataField _field;
    private readonly Func<T, TSlot> _get;
    private readonly bool _timestamp;
    private int _leaf;

    public FlatColumn(PropertyInfo property, string column, ValueKind kind)
    {
        var acceptsNull = !property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) is not null;
        _field = kind.Field(column, acceptsNull);
        _timestamp = kind.Timestamp;

        var row = Expression.Parameter(typeof(T), "row");
        Expression value = Expression.Property(row, property);
        Expression Convert(Expression nonNull) => Expression.Convert(Expression.Invoke(Expression.Constant(kind.Convert), nonNull), typeof(TSlot));
        Expression body = Nullable.GetUnderlyingType(property.PropertyType) is not null
            ? Expression.Condition(Expression.Property(value, "HasValue"), Convert(Expression.Property(value, "Value")), Expression.Default(typeof(TSlot)))
            : !property.PropertyType.IsValueType
                ? Expression.Condition(Expression.NotEqual(value, Expression.Constant(null, property.PropertyType)), Convert(value), Expression.Default(typeof(TSlot)))
                : Convert(value);
        _get = Expression.Lambda<Func<T, TSlot>>(body, row).Compile();
    }

    public override Field Field => _field;

    public override void Bind(ParquetSchema schema) => _leaf = SchemaLeaves.IndexOf(schema, _field);

    public override Task WriteAsync(ParquetRowGroupWriter group, IReadOnlyList<T> rows, RowGroupMinMax minMax, CancellationToken ct)
    {
        var values = new TSlot[rows.Count];
        for (var i = 0; i < values.Length; i++) values[i] = _get(rows[i]);
        if (_timestamp)
        {
            if (values is DateTime[] required) minMax.Add(_leaf, required);
            else if (values is DateTime?[] optional) minMax.Add(_leaf, optional.Where(v => v.HasValue).Select(v => v!.Value));
        }
        return Write(group, _field, values, ct);
    }

    private static Func<ParquetRowGroupWriter, DataField, TSlot[], CancellationToken, Task> CreateWrite()
    {
        var name = typeof(TSlot) == typeof(string) ? nameof(ColumnWrites.Strings)
            : typeof(TSlot) == typeof(byte[]) ? nameof(ColumnWrites.Bytes)
            : Nullable.GetUnderlyingType(typeof(TSlot)) is not null ? nameof(ColumnWrites.Optional)
            : nameof(ColumnWrites.Required);
        var method = typeof(ColumnWrites).GetMethod(name, BindingFlags.Static | BindingFlags.Public)!;
        if (method.IsGenericMethodDefinition) method = method.MakeGenericMethod(Nullable.GetUnderlyingType(typeof(TSlot)) ?? typeof(TSlot));
        return method.CreateDelegate<Func<ParquetRowGroupWriter, DataField, TSlot[], CancellationToken, Task>>();
    }
}

internal static class ColumnWrites
{
    public static Task Strings(ParquetRowGroupWriter group, DataField field, string?[] values, CancellationToken ct) =>
        group.WriteAsync(field, values, null);

    public static Task Bytes(ParquetRowGroupWriter group, DataField field, byte[]?[] values, CancellationToken ct) =>
        group.WriteAsync(field, values, null);

    public static Task Required<TValue>(ParquetRowGroupWriter group, DataField field, TValue[] values, CancellationToken ct) where TValue : struct =>
        group.WriteAsync(field, (ReadOnlyMemory<TValue>)values, null, null, ct);

    public static Task Optional<TValue>(ParquetRowGroupWriter group, DataField field, TValue?[] values, CancellationToken ct) where TValue : struct =>
        group.WriteAsync(field, (ReadOnlyMemory<TValue?>)values, null, null, ct);

    /// <summary>A leaf of a nested column: defined values only, with their definition and repetition levels.</summary>
    public static Task Parts<TValue>(ParquetRowGroupWriter group, DataField field, List<object> values, int[] definition, int[] repetition, CancellationToken ct)
        where TValue : struct
    {
        var typed = new TValue[values.Count];
        for (var i = 0; i < typed.Length; i++) typed[i] = (TValue)values[i];
        return WriteParts(group, field, typed, definition, repetition, ct);
    }

    public static Task StringParts(ParquetRowGroupWriter group, DataField field, List<object> values, int[] definition, int[] repetition, CancellationToken ct)
    {
        var typed = new ReadOnlyMemory<char>[values.Count];
        for (var i = 0; i < typed.Length; i++) typed[i] = ((string)values[i]).AsMemory();
        return WriteParts(group, field, typed, definition, repetition, ct);
    }

    public static Task ByteParts(ParquetRowGroupWriter group, DataField field, List<object> values, int[] definition, int[] repetition, CancellationToken ct)
    {
        var typed = new ReadOnlyMemory<byte>[values.Count];
        for (var i = 0; i < typed.Length; i++) typed[i] = (byte[])values[i];
        return WriteParts(group, field, typed, definition, repetition, ct);
    }

    private static Task WriteParts<TValue>(ParquetRowGroupWriter group, DataField field, TValue[] values, int[] definition, int[] repetition, CancellationToken ct)
        where TValue : struct =>
        group.WriteAllPartsAsync(field, (ReadOnlyMemory<TValue>)values,
            field.MaxDefinitionLevel > 0 ? definition : null,
            field.MaxRepetitionLevel > 0 ? repetition : null, ct);
}

/// <summary>
/// A struct, list or map column. Each row is shredded the Dremel way into its leaf columns: per entry a definition
/// level (how much of the path is present), a repetition level (which list starts a new element) and, when the leaf
/// itself is present, its value.
/// </summary>
internal sealed class NestedColumnWriter<T> : ColumnWriter<T>
{
    private readonly Func<T, object?> _get;
    private readonly Shape _shape;
    private ShredNode _root = null!;
    private Leaf[] _leaves = [];

    public NestedColumnWriter(PropertyInfo property, string column, JsonNamingPolicy? naming)
    {
        _shape = Shape.Of(column, property.PropertyType, property, naming, column, []);
        var row = Expression.Parameter(typeof(T), "row");
        _get = Expression.Lambda<Func<T, object?>>(Expression.Convert(Expression.Property(row, property), typeof(object)), row).Compile();
    }

    public override Field Field => _shape.Field;

    public override void Bind(ParquetSchema schema)
    {
        var leaves = new List<Leaf>();
        _root = _shape.Build(leaves, schema);
        _leaves = [.. leaves];
    }

    public override async Task WriteAsync(ParquetRowGroupWriter group, IReadOnlyList<T> rows, RowGroupMinMax minMax, CancellationToken ct)
    {
        var sinks = _leaves.Select(_ => new LeafSink()).ToArray();
        foreach (var row in rows) _root.Shred(_get(row), 0, 0, sinks);

        for (var l = 0; l < _leaves.Length; l++)
        {
            var leaf = _leaves[l];
            if (leaf.Kind.Timestamp) minMax.Add(leaf.Index, sinks[l].Values.Cast<DateTime>());
            await leaf.Write(group, leaf.Field, sinks[l].Values, [.. sinks[l].Definition], [.. sinks[l].Repetition], ct).ConfigureAwait(false);
        }
    }
}

internal sealed class LeafSink
{
    public List<int> Definition { get; } = [];
    public List<int> Repetition { get; } = [];
    public List<object> Values { get; } = [];
}

internal sealed record Leaf(DataField Field, int Index, ValueKind Kind, Func<ParquetRowGroupWriter, DataField, List<object>, int[], int[], CancellationToken, Task> Write);

/// <summary>The CLR shape of a nested column and its Parquet field, before levels are known.</summary>
internal abstract class Shape
{
    public abstract Field Field { get; }

    /// <summary>The shredder for this shape, with leaves appended to <paramref name="leaves"/> in schema order.</summary>
    public abstract ShredNode Build(List<Leaf> leaves, ParquetSchema schema);

    // enclosing: classes this one is nested in; Parquet has no recursive schemas.
    public static Shape Of(string name, Type type, PropertyInfo? property, JsonNamingPolicy? naming, string path, Type[] enclosing)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        var acceptsNull = !type.IsValueType || underlying != type;
        if (ValueKinds.For(underlying, property, path) is { } kind) return new LeafShape(kind.Field(name, acceptsNull), kind, path);

        if (DictionaryTypes(type) is { } kv)
        {
            // Map keys are single values and required in Parquet.
            var keyKind = ValueKinds.For(Nullable.GetUnderlyingType(kv.Key) ?? kv.Key, null, path + ".key")
                          ?? throw new NotSupportedException($"Column '{path}': map keys must be single values, not {kv.Key.Name}.");
            var key = new LeafShape(keyKind.Field("key", false), keyKind, path + ".key");
            return new MapShape(name, key, Of("value", kv.Value, null, naming, path + ".value", enclosing), kv.Key, kv.Value, path);
        }
        if (type != typeof(string) && NestedBuilder.CollectionElement(type) is { } element)
            return new ListShape(name, Of("element", element, null, naming, path + "[]", enclosing));
        if (type.IsClass && type != typeof(object) && type != typeof(string))
        {
            if (enclosing.Contains(type))
                throw new NotSupportedException($"Column '{path}': {type.Name} contains itself, and a Parquet schema can't be recursive. Mark the property [IgnoreDataMember].");
            Type[] inner = [.. enclosing, type];
            var children = ColumnNames.MappedProperties(type)
                .Where(p => p.GetMethod is { IsPublic: true })
                .Select(p => (Getter(p), Of(ColumnNames.Of(p, naming), p.PropertyType, p, naming, $"{path}.{ColumnNames.Of(p, naming)}", inner)))
                .ToArray();
            if (children.Length == 0) throw new NotSupportedException($"Column '{path}': {type.Name} has no public properties to write.");
            return new StructShape(name, children);
        }
        throw new NotSupportedException($"Column '{path}': {type.Name} can't be written to Parquet.");
    }

    private static (Type Key, Type Value)? DictionaryTypes(Type type)
    {
        if (!type.IsGenericType) return null;
        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(Dictionary<,>) || definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>)
            ? (type.GetGenericArguments()[0], type.GetGenericArguments()[1])
            : null;
    }

    private static readonly ConcurrentDictionary<PropertyInfo, Func<object, object?>> Getters = new();

    private static Func<object, object?> Getter(PropertyInfo property) => Getters.GetOrAdd(property, p =>
    {
        var target = Expression.Parameter(typeof(object));
        return Expression.Lambda<Func<object, object?>>(
            Expression.Convert(Expression.Property(Expression.Convert(target, p.DeclaringType!), p), typeof(object)), target).Compile();
    });

    private static Field Find(ParquetSchema schema, Field field)
    {
        foreach (var top in schema.Fields)
            if (Search(top, field) is { } found) return found;
        throw new InvalidOperationException($"Field '{field.Path}' is not in the schema.");

        static Field? Search(Field current, Field wanted)
        {
            if (ReferenceEquals(current, wanted)) return current;
            var children = current switch
            {
                StructField s => s.Fields,
                ListField l => [l.Item],
                MapField m => [m.Key, m.Value],
                _ => (IReadOnlyList<Field>)[],
            };
            foreach (var child in children)
                if (Search(child, wanted) is { } found) return found;
            return null;
        }
    }

    private sealed class LeafShape(DataField data, ValueKind kind, string path) : Shape
    {
        public override Field Field => data;

        public override ShredNode Build(List<Leaf> leaves, ParquetSchema schema)
        {
            var index = SchemaLeaves.IndexOf(schema, data);
            var method = kind.Store == typeof(string) ? typeof(ColumnWrites).GetMethod(nameof(ColumnWrites.StringParts))!
                : kind.Store == typeof(byte[]) ? typeof(ColumnWrites).GetMethod(nameof(ColumnWrites.ByteParts))!
                : typeof(ColumnWrites).GetMethod(nameof(ColumnWrites.Parts))!.MakeGenericMethod(kind.Store);
            var write = method.CreateDelegate<Func<ParquetRowGroupWriter, DataField, List<object>, int[], int[], CancellationToken, Task>>();
            leaves.Add(new Leaf(data, index, kind, write));
            return new LeafNode(leaves.Count - 1, data.MaxDefinitionLevel, data.IsNullable, kind, path);
        }
    }

    private sealed class StructShape(string name, (Func<object, object?> Get, Shape Shape)[] children) : Shape
    {
        private readonly StructField _field = new(name, children.Select(c => c.Shape.Field).ToArray());

        public override Field Field => _field;

        public override ShredNode Build(List<Leaf> leaves, ParquetSchema schema)
        {
            var first = leaves.Count;
            var nodes = children.Select(c => (c.Get, c.Shape.Build(leaves, schema))).ToArray();
            var field = Find(schema, _field);
            return new StructNode(field.MaxDefinitionLevel, nodes, Enumerable.Range(first, leaves.Count - first).ToArray());
        }
    }

    private sealed class ListShape(string name, Shape element) : Shape
    {
        private readonly ListField _field = new(name, element.Field);

        public override Field Field => _field;

        public override ShredNode Build(List<Leaf> leaves, ParquetSchema schema)
        {
            var first = leaves.Count;
            var node = element.Build(leaves, schema);
            var field = Find(schema, _field);
            return new ListNode(field.MaxDefinitionLevel, field.MaxRepetitionLevel, node, Enumerable.Range(first, leaves.Count - first).ToArray());
        }
    }

    private sealed class MapShape : Shape
    {
        private readonly MapField _field;
        private readonly Shape _key;
        private readonly Shape _value;
        private readonly Func<object, object?> _getKey;
        private readonly Func<object, object?> _getValue;
        private readonly string _path;

        public MapShape(string name, Shape key, Shape value, Type keyType, Type valueType, string path)
        {
            _field = new MapField(name, key.Field, value.Field);
            _key = key;
            _value = value;
            _path = path;
            var pair = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
            _getKey = PairGetter(pair, "Key");
            _getValue = PairGetter(pair, "Value");
        }

        public override Field Field => _field;

        public override ShredNode Build(List<Leaf> leaves, ParquetSchema schema)
        {
            var first = leaves.Count;
            var key = _key.Build(leaves, schema);
            var value = _value.Build(leaves, schema);
            var field = Find(schema, _field);
            return new MapNode(field.MaxDefinitionLevel, field.MaxRepetitionLevel, key, value, _getKey, _getValue, _path,
                Enumerable.Range(first, leaves.Count - first).ToArray());
        }

        private static Func<object, object?> PairGetter(Type pair, string member)
        {
            var target = Expression.Parameter(typeof(object));
            return Expression.Lambda<Func<object, object?>>(
                Expression.Convert(Expression.Property(Expression.Convert(target, pair), member), typeof(object)), target).Compile();
        }
    }
}

/// <summary>Writes entries for one value into the sinks of the leaves under it.</summary>
internal abstract class ShredNode(int[] leaves)
{
    /// <param name="value">The value at this node, or null.</param>
    /// <param name="repetition">Repetition level of the first entry this value writes.</param>
    /// <param name="definition">Definition level of the parent: the level a null here is written with.</param>
    /// <param name="sinks">Entries of each leaf column.</param>
    public abstract void Shred(object? value, int repetition, int definition, LeafSink[] sinks);

    protected void Undefined(int repetition, int definition, LeafSink[] sinks)
    {
        foreach (var leaf in leaves)
        {
            sinks[leaf].Repetition.Add(repetition);
            sinks[leaf].Definition.Add(definition);
        }
    }
}

internal sealed class LeafNode(int leaf, int defined, bool optional, ValueKind kind, string path) : ShredNode([leaf])
{
    public override void Shred(object? value, int repetition, int definition, LeafSink[] sinks)
    {
        if (value is null)
        {
            if (!optional) throw new InvalidDataException($"Column '{path}' is required but the value is null.");
            Undefined(repetition, definition, sinks);
            return;
        }
        var sink = sinks[leaf];
        sink.Repetition.Add(repetition);
        sink.Definition.Add(defined);
        sink.Values.Add(kind.Boxed(value));
    }
}

internal sealed class StructNode(int defined, (Func<object, object?> Get, ShredNode Node)[] children, int[] leaves) : ShredNode(leaves)
{
    public override void Shred(object? value, int repetition, int definition, LeafSink[] sinks)
    {
        if (value is null)
        {
            Undefined(repetition, definition, sinks);
            return;
        }
        foreach (var (get, node) in children) node.Shred(get(value), repetition, defined, sinks);
    }
}

/// <summary>A LIST: <c>optional group (LIST) { repeated group list { element } }</c>. <paramref name="defined"/> is the level of a present element slot.</summary>
internal sealed class ListNode(int defined, int repeated, ShredNode element, int[] leaves) : ShredNode(leaves)
{
    public override void Shred(object? value, int repetition, int definition, LeafSink[] sinks)
    {
        if (value is null)
        {
            Undefined(repetition, definition, sinks);
            return;
        }
        var first = true;
        foreach (var item in (IEnumerable)value)
        {
            element.Shred(item, first ? repetition : repeated, defined, sinks);
            first = false;
        }
        if (first) Undefined(repetition, defined - 1, sinks);
    }
}

internal sealed class MapNode(int defined, int repeated, ShredNode key, ShredNode value, Func<object, object?> getKey, Func<object, object?> getValue, string path, int[] leaves)
    : ShredNode(leaves)
{
    public override void Shred(object? map, int repetition, int definition, LeafSink[] sinks)
    {
        if (map is null)
        {
            Undefined(repetition, definition, sinks);
            return;
        }
        var first = true;
        foreach (var pair in (IEnumerable)map)
        {
            var k = getKey(pair) ?? throw new InvalidDataException($"Map '{path}' has a null key.");
            var r = first ? repetition : repeated;
            key.Shred(k, r, defined, sinks);
            value.Shred(getValue(pair), r, defined, sinks);
            first = false;
        }
        if (first) Undefined(repetition, defined - 1, sinks);
    }
}
