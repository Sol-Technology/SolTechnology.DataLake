using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Mapping;
using SolTechnology.DataLake.Writing;
using SolTechnology.Time;
using ArrowTimeUnit = Apache.Arrow.Types.TimeUnit;

namespace SolTechnology.DataLake.Arrow;

/// <summary>
/// Classes to Arrow record batches. Types match what DataLake writes to Parquet: timestamps in microseconds (with time zone
/// UTC for instants, without for <see cref="UnzonedDateTime"/>), decimals as decimal128 (38, 18 unless
/// <see cref="DecimalPrecisionAttribute"/>), enums by name, <see cref="Guid"/> as the arrow.uuid extension, <see cref="TimeSpan"/>
/// as a duration.
/// </summary>
internal sealed class BatchWriter<T> where T : class
{
    private static readonly ConcurrentDictionary<JsonNamingPolicy, BatchWriter<T>> ByPolicy = new();
    private static readonly Lazy<BatchWriter<T>> WithoutPolicy = new(() => new BatchWriter<T>(null));

    private readonly (Func<T, object?> Get, ArrowShape Shape)[] _columns;

    private BatchWriter(JsonNamingPolicy? naming)
    {
        var map = ClassMap<T>.For(naming);
        _columns = map.Properties.Select(p =>
        {
            var row = Expression.Parameter(typeof(T));
            var get = Expression.Lambda<Func<T, object?>>(Expression.Convert(Expression.Property(row, p.Property), typeof(object)), row).Compile();
            return (get, ArrowShape.Of(p.Column, p.Type, p.Property, naming, p.Column, []));
        }).ToArray();
        Schema = new Schema(_columns.Select(c => c.Shape.Field), null);
    }

    public Schema Schema { get; }

    public static BatchWriter<T> For(JsonNamingPolicy? naming) =>
        naming is null ? WithoutPolicy.Value : ByPolicy.GetOrAdd(naming, n => new BatchWriter<T>(n));

    public RecordBatch Write(IReadOnlyList<T> rows)
    {
        var arrays = new IArrowArray[_columns.Length];
        var values = new object?[rows.Count];
        for (var c = 0; c < _columns.Length; c++)
        {
            var (get, shape) = _columns[c];
            for (var r = 0; r < rows.Count; r++) values[r] = get(rows[r] ?? throw new ArgumentException("A row is null.", nameof(rows)));
            arrays[c] = shape.Build(values);
        }
        return new RecordBatch(Schema, arrays, rows.Count);
    }
}

/// <summary>The Arrow type of a CLR type, and how to build an array of it from boxed values.</summary>
internal abstract class ArrowShape
{
    public required Field Field { get; init; }

    public abstract IArrowArray Build(IReadOnlyList<object?> values);

    public static ArrowShape Of(string name, Type type, PropertyInfo? property, JsonNamingPolicy? naming, string path, Type[] enclosing)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        var nullable = !type.IsValueType || underlying != type;

        if (Leaf(underlying, property, path) is { } leaf)
            return new LeafShape(leaf.Build) { Field = new Field(name, leaf.Type, nullable) };

        if (DictionaryTypes(type) is { } kv)
        {
            var key = Of("key", Nullable.GetUnderlyingType(kv.Key) ?? kv.Key, null, naming, path + ".key", enclosing);
            if (key is not LeafShape) throw new NotSupportedException($"Column '{path}': map keys must be single values, not {kv.Key.Name}.");
            var value = Of("value", kv.Value, null, naming, path + ".value", enclosing);
            var pair = typeof(KeyValuePair<,>).MakeGenericType(kv.Key, kv.Value);
            return new MapShape(key, value, Getter(pair, "Key"), Getter(pair, "Value"), path)
            {
                Field = new Field(name, new MapType(key.Field, value.Field), nullable),
            };
        }

        if (type != typeof(string) && NestedBuilder.CollectionElement(type) is { } element)
        {
            var item = Of("item", element, null, naming, path + "[]", enclosing);
            return new ListShape(item) { Field = new Field(name, new ListType(item.Field), nullable) };
        }

        if (type.IsClass && type != typeof(object))
        {
            if (enclosing.Contains(type))
                throw new NotSupportedException($"Column '{path}': {type.Name} contains itself, and an Arrow schema can't be recursive. Mark the property [IgnoreDataMember].");
            Type[] inner = [.. enclosing, type];
            var children = ColumnNames.MappedProperties(type)
                .Where(p => p.GetMethod is { IsPublic: true })
                .Select(p => (Get: Getter(p), Shape: Of(ColumnNames.Of(p, naming), p.PropertyType, p, naming, $"{path}.{ColumnNames.Of(p, naming)}", inner)))
                .ToArray();
            if (children.Length == 0) throw new NotSupportedException($"Column '{path}': {type.Name} has no public properties to write.");
            return new StructShape(children) { Field = new Field(name, new StructType(children.Select(c => c.Shape.Field).ToList()), nullable) };
        }

        throw new NotSupportedException($"Column '{path}': {type.Name} can't be written to Arrow.");
    }

    private sealed record LeafKind(IArrowType Type, Func<IReadOnlyList<object?>, IArrowArray> Build);

    private static LeafKind? Leaf(Type type, PropertyInfo? property, string path)
    {
        if (type == typeof(bool)) return new(BooleanType.Default, v => Primitive(new BooleanArray.Builder(), v, (b, x) => b.Append((bool)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(sbyte)) return new(Int8Type.Default, v => Primitive(new Int8Array.Builder(), v, (b, x) => b.Append((sbyte)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(byte)) return new(UInt8Type.Default, v => Primitive(new UInt8Array.Builder(), v, (b, x) => b.Append((byte)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(short)) return new(Int16Type.Default, v => Primitive(new Int16Array.Builder(), v, (b, x) => b.Append((short)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(ushort)) return new(UInt16Type.Default, v => Primitive(new UInt16Array.Builder(), v, (b, x) => b.Append((ushort)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(int)) return new(Int32Type.Default, v => Primitive(new Int32Array.Builder(), v, (b, x) => b.Append((int)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(uint)) return new(UInt32Type.Default, v => Primitive(new UInt32Array.Builder(), v, (b, x) => b.Append((uint)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(long)) return new(Int64Type.Default, v => Primitive(new Int64Array.Builder(), v, (b, x) => b.Append((long)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(ulong)) return new(UInt64Type.Default, v => Primitive(new UInt64Array.Builder(), v, (b, x) => b.Append((ulong)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(float)) return new(FloatType.Default, v => Primitive(new FloatArray.Builder(), v, (b, x) => b.Append((float)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(double)) return new(DoubleType.Default, v => Primitive(new DoubleArray.Builder(), v, (b, x) => b.Append((double)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(string)) return new(StringType.Default, v => Primitive(new StringArray.Builder(), v, (b, x) => b.Append((string)x), b => b.AppendNull(), b => b.Build()));
        if (type.IsEnum) return new(StringType.Default, v => Primitive(new StringArray.Builder(), v, (b, x) => b.Append(x.ToString()), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(byte[])) return new(BinaryType.Default, v => Primitive(new BinaryArray.Builder(), v, (b, x) => b.Append((byte[])x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(Guid)) return new(GuidType.Default, v => Primitive(new GuidArray.Builder(), v, (b, x) => b.Append((Guid)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(DateOnly)) return new(Date32Type.Default, v => Primitive(new Date32Array.Builder(), v, (b, x) => b.Append((DateOnly)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(TimeOnly))
            return new(new Time64Type(ArrowTimeUnit.Microsecond), v => Primitive(new Time64Array.Builder(ArrowTimeUnit.Microsecond), v, (b, x) => b.Append((TimeOnly)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(TimeSpan))
            return new(DurationType.Microsecond, v => Primitive(new DurationArray.Builder(DurationType.Microsecond), v, (b, x) => b.Append((TimeSpan)x), b => b.AppendNull(), b => b.Build()));
        if (type == typeof(decimal))
        {
            var attribute = property?.GetCustomAttribute<DecimalPrecisionAttribute>();
            var decimalType = new Decimal128Type(attribute?.Precision ?? 38, attribute?.Scale ?? 18);
            return new(decimalType, v => Primitive(new Decimal128Array.Builder(decimalType), v,
                (b, x) => b.Append(ValueKinds.CheckDecimal((decimal)x, decimalType.Precision, decimalType.Scale, path)), b => b.AppendNull(), b => b.Build()));
        }
        if (type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateTimeZone))
        {
            var utcType = new TimestampType(ArrowTimeUnit.Microsecond, "UTC");
            return new(utcType, v => Primitive(new TimestampArray.Builder(utcType), v,
                (b, x) => b.Append(new DateTimeOffset(ValueKinds.UtcOf(x, path), TimeSpan.Zero)), b => b.AppendNull(), b => b.Build()));
        }
        if (type == typeof(UnzonedDateTime))
        {
            var localType = new TimestampType(ArrowTimeUnit.Microsecond, (string?)null);
            return new(localType, v => Primitive(new TimestampArray.Builder(localType), v,
                (b, x) => b.Append(new DateTimeOffset(DateTime.SpecifyKind(((UnzonedDateTime)x).ToDateTime(), DateTimeKind.Unspecified), TimeSpan.Zero)),
                b => b.AppendNull(), b => b.Build()));
        }
        return null;
    }

    private static IArrowArray Primitive<TBuilder>(TBuilder builder, IReadOnlyList<object?> values, Action<TBuilder, object> append,
        Action<TBuilder> appendNull, Func<TBuilder, IArrowArray> build)
    {
        foreach (var value in values)
        {
            if (value is null) appendNull(builder);
            else append(builder, value);
        }
        return build(builder);
    }

    private static (Type Key, Type Value)? DictionaryTypes(Type type)
    {
        if (!type.IsGenericType) return null;
        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(Dictionary<,>) || definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>)
            ? (type.GetGenericArguments()[0], type.GetGenericArguments()[1])
            : null;
    }

    private static readonly ConcurrentDictionary<MemberInfo, Func<object, object?>> Getters = new();

    private static Func<object, object?> Getter(PropertyInfo property) => Getters.GetOrAdd(property, p => Compile(((PropertyInfo)p).DeclaringType!, (PropertyInfo)p));

    private static Func<object, object?> Getter(Type declaring, string name) =>
        Getters.GetOrAdd(declaring.GetProperty(name)!, p => Compile(declaring, (PropertyInfo)p));

    private static Func<object, object?> Compile(Type declaring, PropertyInfo property)
    {
        var target = Expression.Parameter(typeof(object));
        return Expression.Lambda<Func<object, object?>>(
            Expression.Convert(Expression.Property(Expression.Convert(target, declaring), property), typeof(object)), target).Compile();
    }

    protected static (ArrowBuffer Bitmap, int NullCount) Validity(IReadOnlyList<object?> values)
    {
        var nulls = 0;
        foreach (var value in values) if (value is null) nulls++;
        if (nulls == 0) return (ArrowBuffer.Empty, 0);
        var bitmap = new ArrowBuffer.BitmapBuilder(values.Count);
        foreach (var value in values) bitmap.Append(value is not null);
        return (bitmap.Build(), nulls);
    }

    private sealed class LeafShape(Func<IReadOnlyList<object?>, IArrowArray> build) : ArrowShape
    {
        public override IArrowArray Build(IReadOnlyList<object?> values) => build(values);
    }

    private sealed class ListShape(ArrowShape item) : ArrowShape
    {
        public override IArrowArray Build(IReadOnlyList<object?> values)
        {
            var offsets = new ArrowBuffer.Builder<int>(values.Count + 1);
            var items = new List<object?>();
            offsets.Append(0);
            foreach (var value in values)
            {
                if (value is IEnumerable list)
                    foreach (var element in list) items.Add(element);
                offsets.Append(items.Count);
            }
            var (bitmap, nulls) = Validity(values);
            return new ListArray(Field.DataType, values.Count, offsets.Build(), item.Build(items), bitmap, nulls, 0);
        }
    }

    private sealed class StructShape((Func<object, object?> Get, ArrowShape Shape)[] children) : ArrowShape
    {
        public override IArrowArray Build(IReadOnlyList<object?> values)
        {
            var arrays = new IArrowArray[children.Length];
            var childValues = new object?[values.Count];
            for (var c = 0; c < children.Length; c++)
            {
                for (var r = 0; r < values.Count; r++) childValues[r] = values[r] is { } value ? children[c].Get(value) : null;
                arrays[c] = children[c].Shape.Build(childValues);
            }
            var (bitmap, nulls) = Validity(values);
            return new StructArray(Field.DataType, values.Count, arrays, bitmap, nulls, 0);
        }
    }

    private sealed class MapShape(ArrowShape key, ArrowShape value, Func<object, object?> getKey, Func<object, object?> getValue, string path) : ArrowShape
    {
        public override IArrowArray Build(IReadOnlyList<object?> values)
        {
            var offsets = new ArrowBuffer.Builder<int>(values.Count + 1);
            var keys = new List<object?>();
            var items = new List<object?>();
            offsets.Append(0);
            foreach (var map in values)
            {
                if (map is IEnumerable pairs)
                {
                    foreach (var pair in pairs)
                    {
                        keys.Add(getKey(pair!) ?? throw new InvalidDataException($"Map '{path}' has a null key."));
                        items.Add(getValue(pair!));
                    }
                }
                offsets.Append(keys.Count);
            }
            var type = (MapType)Field.DataType;
            var entries = new StructArray(type.KeyValueType, keys.Count, [key.Build(keys), value.Build(items)], ArrowBuffer.Empty, 0, 0);
            var (bitmap, nulls) = Validity(values);
            return new MapArray(type, values.Count, offsets.Build(), entries, bitmap, nulls, 0);
        }
    }
}
