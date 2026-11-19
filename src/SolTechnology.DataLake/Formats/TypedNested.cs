using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// Nested columns assembled straight into typed values: each leaf is read once into a typed array of the property's
/// type, and nodes compiled per CLR type (<see cref="StructReader{TClass}"/>, <see cref="ListReader{TElement, TCollection}"/>,
/// <see cref="MapReader{TKey, TValue, TDictionary}"/>) walk the definition and repetition levels with one cursor per leaf.
/// No boxing, no per-value lookups. Layouts this does not cover (legacy two-level lists, repeated fields without a LIST
/// annotation, untyped targets) go through <see cref="NestedBuilder"/>.
/// </summary>
internal static class TypedNested
{
    /// <summary>The leaf columns under a nested column, and for each the conversion of its values (none for skipped leaves).</summary>
    public sealed class Leaves
    {
        public List<DataField> Fields { get; } = [];
        public List<Func<RawColumnData, Array>?> Convert { get; } = [];
        public int Count => Fields.Count;

        public int Add(DataField field, Func<RawColumnData, Array>? convert)
        {
            Fields.Add(field);
            Convert.Add(convert);
            return Fields.Count - 1;
        }
    }

    /// <summary>A reader for <paramref name="field"/> into <paramref name="target"/>, or null to use the general path.</summary>
    public static NestedReader? TryBuild(Field field, Type target, Leaves leaves, JsonNamingPolicy? naming, string path)
    {
        try
        {
            return Build(field, target, leaves, naming, path);
        }
        catch (Unsupported)
        {
            return null;
        }
    }

    private sealed class Unsupported : Exception;

    private static NestedReader Build(Field field, Type target, Leaves leaves, JsonNamingPolicy? naming, string path)
    {
        if (target == typeof(object)) throw new Unsupported();
        switch (field)
        {
            case DataField { IsArray: false } data:
            {
                var logical = ParquetStatistics.LogicalOf(data);
                if (!Conversions.CanConvert(ParquetStatistics.ClrOf(data), target, logical)) throw new Unsupported();
                var convert = (Func<RawColumnData, Array>)typeof(DenseValues).GetMethod(nameof(DenseValues.Converter))!.MakeGenericMethod(target)
                    .Invoke(null, [logical, path])!;
                var leaf = leaves.Add(data, convert);
                return Create(typeof(LeafReader<>).MakeGenericType(target), data.MaxDefinitionLevel, leaf, path);
            }

            case ListField list when list.SchemaElement?.RepetitionType != Parquet.Meta.FieldRepetitionType.REPEATED
                                     && !(list.Item is DataField { IsArray: true } && list.Path[list.Path.Length - 1] == list.Name):
            {
                var element = NestedBuilder.CollectionElement(target) ?? throw new Unsupported();
                var first = leaves.Count;
                var item = Build(list.Item, element, leaves, naming, path + "[]");
                return Create(typeof(ListReader<,>).MakeGenericType(element, target), Levels.Of(list, first, leaves.Count), item);
            }

            case MapField map:
            {
                var definition = target.IsGenericType ? target.GetGenericTypeDefinition() : null;
                if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>) && definition != typeof(IReadOnlyDictionary<,>)) throw new Unsupported();
                var args = target.GetGenericArguments();
                var first = leaves.Count;
                var key = Build(map.Key, args[0], leaves, naming, path + ".key");
                var value = Build(map.Value, args[1], leaves, naming, path + ".value");
                return Create(typeof(MapReader<,,>).MakeGenericType(args[0], args[1], target), Levels.Of(map, first, leaves.Count), key, value, path);
            }

            case StructField structField:
            {
                if (target == typeof(string) || target.IsValueType || !target.IsClass || NestedBuilder.CollectionElement(target) is not null
                    || target.GetConstructor(Type.EmptyTypes) is null || typeof(System.Collections.IDictionary).IsAssignableFrom(target))
                    throw new Unsupported();
                var properties = ColumnNames.MappedProperties(target).ToDictionary(p => ColumnNames.Of(p, naming), StringComparer.OrdinalIgnoreCase);
                var first = leaves.Count;
                var children = new List<StructChild>();
                var bound = new HashSet<PropertyInfo>();
                foreach (var child in structField.Fields)
                {
                    if (properties.TryGetValue(child.Name, out var property))
                    {
                        var node = Build(child, property.PropertyType, leaves, naming, $"{path}.{child.Name}");
                        children.Add((StructChild)Activator.CreateInstance(typeof(StructChild<,>).MakeGenericType(target, property.PropertyType), node, property)!);
                        bound.Add(property);
                    }
                    else
                    {
                        // Not in the class: walked over, its levels still tell whether the struct is null.
                        children.Add(new SkippedChild(Skip(child, leaves)));
                    }
                }
                // A required value-type property without a field: the general path explains it.
                if (properties.Values.Any(p => !bound.Contains(p) && p.PropertyType.IsValueType && Nullable.GetUnderlyingType(p.PropertyType) is null))
                    throw new Unsupported();
                return Create(typeof(StructReader<>).MakeGenericType(target), structField.IsNullable ? structField.MaxDefinitionLevel : -1, children.ToArray(), first, leaves.Count);
            }

            default:
                throw new Unsupported();
        }
    }

    private static NestedReader Skip(Field field, Leaves leaves)
    {
        switch (field)
        {
            case DataField { IsArray: false } data:
                return new SkipLeaf(leaves.Add(data, null));
            case ListField list when list.SchemaElement?.RepetitionType != Parquet.Meta.FieldRepetitionType.REPEATED
                                     && !(list.Item is DataField { IsArray: true } && list.Path[list.Path.Length - 1] == list.Name):
            {
                var first = leaves.Count;
                var item = Skip(list.Item, leaves);
                return new SkipRepeated(Levels.Of(list, first, leaves.Count), [item]);
            }
            case MapField map:
            {
                var first = leaves.Count;
                var key = Skip(map.Key, leaves);
                var value = Skip(map.Value, leaves);
                return new SkipRepeated(Levels.Of(map, first, leaves.Count), [key, value]);
            }
            case StructField structField:
            {
                var first = leaves.Count;
                var children = structField.Fields.Select(f => Skip(f, leaves)).ToArray();
                return new SkipStruct(structField.IsNullable ? structField.MaxDefinitionLevel : -1, first, leaves.Count, children);
            }
            default:
                throw new Unsupported();
        }
    }

    private static NestedReader Create(Type type, params object?[] args) => (NestedReader)Activator.CreateInstance(type, args)!;
}

/// <summary>The leaf columns of one nested column in one row group, with a cursor per leaf.</summary>
internal sealed class LeafCursors
{
    public required int[]?[] Def { get; init; }
    public required int[]?[] Rep { get; init; }
    public required int[] MaxDef { get; init; }
    public required int[] Count { get; init; }
    public required Array[] Values { get; init; }
    public required int[] Position { get; init; }
    public required int[] ValuePosition { get; init; }

    public static LeafCursors Create(int leaves, int[]?[] def, int[]?[] rep, int[] maxDef, int[] count, Array[] values) => new()
    {
        Def = def, Rep = rep, MaxDef = maxDef, Count = count, Values = values,
        Position = new int[leaves], ValuePosition = new int[leaves],
    };

    public int PeekDef(int leaf) => Def[leaf]?[Position[leaf]] ?? MaxDef[leaf];

    /// <summary>Repetition level of the next entry, or -1 at the end of the column.</summary>
    public int PeekRep(int leaf) => Position[leaf] >= Count[leaf] ? -1 : Rep[leaf]?[Position[leaf]] ?? 0;

    /// <summary>Steps over one undefined entry in each of the leaves (a null or empty value above them).</summary>
    public void SkipEntry(int from, int to)
    {
        for (var l = from; l < to; l++) Position[l]++;
    }
}

internal abstract class NestedReader
{
    /// <summary>Steps over one value, consuming its entries.</summary>
    public abstract void Skip(LeafCursors cursors);
}

internal abstract class NestedReader<TOut> : NestedReader
{
    public abstract TOut Read(LeafCursors cursors);

    public override void Skip(LeafCursors cursors) => Read(cursors);
}

/// <summary>Definition and repetition levels of a LIST or MAP, and the leaves under it.</summary>
internal readonly record struct Levels(int NullBelow, int ElementAt, int Repetition, int FirstLeaf, int EndLeaf)
{
    public static Levels Of(Field field, int first, int end) =>
        new(field.IsNullable ? field.MaxDefinitionLevel - 1 : -1, field.MaxDefinitionLevel, field.MaxRepetitionLevel, first, end);
}

internal sealed class LeafReader<TOut>(int defined, int leaf, string path) : NestedReader<TOut>
{
    private static readonly bool AcceptsNull = !typeof(TOut).IsValueType || Nullable.GetUnderlyingType(typeof(TOut)) is not null;

    public override TOut Read(LeafCursors cursors)
    {
        var def = cursors.PeekDef(leaf);
        cursors.Position[leaf]++;
        if (def == defined) return ((TOut[])cursors.Values[leaf])[cursors.ValuePosition[leaf]++];
        return AcceptsNull ? default!
            : throw new InvalidDataException($"Column '{path}' has a null value, but property type {typeof(TOut).Name} does not accept null. Make it {typeof(TOut).Name}?.");
    }
}

internal abstract class StructChild
{
    public abstract void Apply(object target, LeafCursors cursors);
}

internal sealed class StructChild<TClass, TChild>(NestedReader node, PropertyInfo property) : StructChild
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<PropertyInfo, Action<TClass, TChild>> Setters = new();

    private readonly NestedReader<TChild> _node = (NestedReader<TChild>)node;
    private readonly Action<TClass, TChild> _set = Setters.GetOrAdd(property, Compile);

    public override void Apply(object target, LeafCursors cursors) => _set((TClass)target, _node.Read(cursors));

    private static Action<TClass, TChild> Compile(PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(TClass));
        var value = Expression.Parameter(typeof(TChild));
        return Expression.Lambda<Action<TClass, TChild>>(Expression.Assign(Expression.Property(target, property), value), target, value).Compile();
    }
}

internal sealed class SkippedChild(NestedReader node) : StructChild
{
    public override void Apply(object target, LeafCursors cursors) => node.Skip(cursors);
}

internal sealed class StructReader<TClass>(int nullBelow, StructChild[] children, int firstLeaf, int endLeaf) : NestedReader<TClass?> where TClass : class
{
    private static readonly Func<TClass> New = Expression.Lambda<Func<TClass>>(Expression.New(typeof(TClass))).Compile();

    public override TClass? Read(LeafCursors cursors)
    {
        if (cursors.PeekDef(firstLeaf) < nullBelow)
        {
            cursors.SkipEntry(firstLeaf, endLeaf);
            return null;
        }
        var result = New();
        foreach (var child in children) child.Apply(result, cursors);
        return result;
    }
}

internal sealed class ListReader<TElement, TCollection>(Levels levels, NestedReader element) : NestedReader<TCollection?>
{
    private readonly NestedReader<TElement> _element = (NestedReader<TElement>)element;

    public override TCollection? Read(LeafCursors cursors)
    {
        var def = cursors.PeekDef(levels.FirstLeaf);
        if (def < levels.NullBelow)
        {
            cursors.SkipEntry(levels.FirstLeaf, levels.EndLeaf);
            return default;
        }
        var items = new List<TElement>();
        if (def < levels.ElementAt)
        {
            cursors.SkipEntry(levels.FirstLeaf, levels.EndLeaf);
            return Finish(items);
        }
        do items.Add(_element.Read(cursors));
        while (cursors.PeekRep(levels.FirstLeaf) == levels.Repetition);
        return Finish(items);
    }

    private static TCollection Finish(List<TElement> items) =>
        typeof(TCollection).IsArray ? (TCollection)(object)items.ToArray() : (TCollection)(object)items;
}

internal sealed class MapReader<TKey, TValue, TDictionary>(Levels levels, NestedReader key, NestedReader value, string path) : NestedReader<TDictionary?>
    where TKey : notnull
{
    private readonly NestedReader<TKey> _key = (NestedReader<TKey>)key;
    private readonly NestedReader<TValue> _value = (NestedReader<TValue>)value;

    public override TDictionary? Read(LeafCursors cursors)
    {
        var def = cursors.PeekDef(levels.FirstLeaf);
        if (def < levels.NullBelow)
        {
            cursors.SkipEntry(levels.FirstLeaf, levels.EndLeaf);
            return default;
        }
        var result = new Dictionary<TKey, TValue>();
        if (def < levels.ElementAt)
        {
            cursors.SkipEntry(levels.FirstLeaf, levels.EndLeaf);
            return (TDictionary)(object)result;
        }
        do
        {
            var k = _key.Read(cursors) ?? throw new InvalidDataException($"Map '{path}' has a null key.");
            result[k] = _value.Read(cursors);
        }
        while (cursors.PeekRep(levels.FirstLeaf) == levels.Repetition);
        return (TDictionary)(object)result;
    }
}

internal sealed class SkipLeaf(int leaf) : NestedReader
{
    public override void Skip(LeafCursors cursors) => cursors.Position[leaf]++;
}

internal sealed class SkipStruct(int nullBelow, int firstLeaf, int endLeaf, NestedReader[] children) : NestedReader
{
    public override void Skip(LeafCursors cursors)
    {
        if (cursors.PeekDef(firstLeaf) < nullBelow)
        {
            cursors.SkipEntry(firstLeaf, endLeaf);
            return;
        }
        foreach (var child in children) child.Skip(cursors);
    }
}

internal sealed class SkipRepeated(Levels levels, NestedReader[] parts) : NestedReader
{
    public override void Skip(LeafCursors cursors)
    {
        if (cursors.PeekDef(levels.FirstLeaf) < levels.ElementAt)
        {
            cursors.SkipEntry(levels.FirstLeaf, levels.EndLeaf);
            return;
        }
        do
            foreach (var part in parts) part.Skip(cursors);
        while (cursors.PeekRep(levels.FirstLeaf) == levels.Repetition);
    }
}

/// <summary>Defined values of a leaf column, from Parquet.Net's raw column, converted to the target type in one loop.</summary>
internal static class DenseValues
{
    /// <summary>Raw column → <c>TOut[]</c>; the typed loop is made once per raw value type.</summary>
    public static Func<RawColumnData, Array> Converter<TOut>(Logical logical, string path)
    {
        Func<RawColumnData, Logical, string, TOut[]>? typed = null;
        Type? element = null;
        return raw =>
        {
            var current = raw.GetType().GenericTypeArguments[0];
            if (typed is null || current != element)
            {
                element = current;
                typed = typeof(DenseValues).GetMethod(nameof(ConvertTyped), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(current, typeof(TOut))
                    .CreateDelegate<Func<RawColumnData, Logical, string, TOut[]>>();
            }
            return typed(raw, logical, path);
        };
    }

    private static TOut[] ConvertTyped<TValue, TOut>(RawColumnData raw, Logical logical, string path) where TValue : struct
    {
        var values = ((RawColumnData<TValue>)raw).Values;
        var result = new TOut[values.Length];
        if (typeof(TValue) == typeof(ReadOnlyMemory<char>))
        {
            var convert = (Func<string, TOut>)Conversions.Get(typeof(string), typeof(TOut), logical, path);
            for (var i = 0; i < values.Length; i++) result[i] = convert(((ReadOnlyMemory<char>)(object)values[i]).ToString());
        }
        else if (typeof(TValue) == typeof(ReadOnlyMemory<byte>))
        {
            var convert = (Func<byte[], TOut>)Conversions.Get(typeof(byte[]), typeof(TOut), logical, path);
            // A view into the reader's pooled buffer, returned when the column is disposed: copy it.
            for (var i = 0; i < values.Length; i++) result[i] = convert(((ReadOnlyMemory<byte>)(object)values[i]).ToArray());
        }
        else
        {
            var convert = (Func<TValue, TOut>)Conversions.Get(typeof(TValue), typeof(TOut), logical, path);
            for (var i = 0; i < values.Length; i++) result[i] = convert(values[i]);
        }
        return result;
    }
}

/// <summary>A top-level nested column assigned to one property, through <see cref="TypedNested"/>.</summary>
internal sealed class TypedNestedBinding<T, TProperty> : NestedColumnBinding<T> where T : class
{
    private readonly NestedReader<TProperty> _root;
    private readonly TypedNested.Leaves _leaves;
    private readonly Action<T, TProperty> _set;

    public TypedNestedBinding(PropertyMap<T> property, NestedReader root, TypedNested.Leaves leaves) : base(property)
    {
        _root = (NestedReader<TProperty>)root;
        _leaves = leaves;
        _set = (Action<T, TProperty>)property.Setter;
    }

    public override async Task AssignAsync(ParquetRowGroupReader group, T[] rows, CancellationToken ct)
    {
        var count = _leaves.Count;
        var def = new int[]?[count];
        var rep = new int[]?[count];
        var maxDef = new int[count];
        var entries = new int[count];
        var values = new Array[count];
        for (var l = 0; l < count; l++)
        {
            var field = _leaves.Fields[l];
            using var raw = await group.ReadRawColumnDataBaseAsync(field, ct).ConfigureAwait(false);
            // Parquet.Net throws when asked for levels the column does not have.
            def[l] = field.MaxDefinitionLevel > 0 ? raw.DefinitionLevels.ToArray() : null;
            rep[l] = field.MaxRepetitionLevel > 0 ? raw.RepetitionLevels.ToArray() : null;
            maxDef[l] = field.MaxDefinitionLevel;
            values[l] = _leaves.Convert[l]?.Invoke(raw) ?? Array.Empty<object>();
            entries[l] = def[l]?.Length ?? rep[l]?.Length ?? values[l].Length;
            if (entries[l] > 0 && (rep[l]?[0] ?? 0) != 0)
                throw new InvalidDataException($"Column '{field.Path}' starts in the middle of a row (repetition level {rep[l]![0]}); the file is corrupt.");
        }

        var cursors = LeafCursors.Create(count, def, rep, maxDef, entries, values);
        for (var r = 0; r < rows.Length; r++)
        {
            for (var l = 0; l < count; l++)
                if (cursors.Position[l] >= entries[l])
                    throw new InvalidDataException($"Column '{_leaves.Fields[l].Path}' has fewer rows than the row group ({rows.Length}).");
            _set(rows[r], _root.Read(cursors));
        }
        for (var l = 0; l < count; l++)
            if (cursors.Position[l] != entries[l])
                throw new InvalidDataException($"Column '{_leaves.Fields[l].Path}' has more rows than the row group ({rows.Length}).");
    }
}
