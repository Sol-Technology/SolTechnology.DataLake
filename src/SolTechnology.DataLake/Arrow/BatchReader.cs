using System.Text.Json;
using Apache.Arrow;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Arrow;

/// <summary>Arrow record batches to classes: one typed loop per flat column, plain values for nested ones.</summary>
internal static class BatchReader<T> where T : class
{
    public static T[] Read(RecordBatch batch, JsonNamingPolicy? naming) => Read(batch, ClassMap<T>.For(naming), null, "The Arrow data");

    /// <param name="batch">The batch.</param>
    /// <param name="map">The class mapping.</param>
    /// <param name="filled">Properties filled from elsewhere (partitions): not read, and not missing.</param>
    /// <param name="source">For messages: the file, or "The Arrow data".</param>
    public static T[] Read(RecordBatch batch, ClassMap<T> map, IReadOnlyDictionary<PropertyMap<T>, object?>? filled, string source)
    {
        var naming = map.Naming;
        var rows = new T[batch.Length];
        for (var i = 0; i < rows.Length; i++) rows[i] = map.Create();

        var bound = new HashSet<PropertyMap<T>>();
        for (var c = 0; c < batch.ColumnCount; c++)
        {
            var field = batch.Schema.GetFieldByIndex(c);
            if (map.Find(field.Name) is not { } property || filled?.ContainsKey(property) == true || !bound.Add(property)) continue;
            Bind(batch.Column(c), field.Name, property, naming).Assign(batch.Column(c), rows);
        }

        var missing = map.Properties.FirstOrDefault(p => !bound.Contains(p) && filled?.ContainsKey(p) != true && !p.AcceptsNull);
        if (missing is not null)
            throw new InvalidDataException($"{source} has no column '{missing.Column}' for {typeof(T).Name}.{missing.Property.Name} ({missing.Type.Name}). " +
                                           "Make the property nullable to read data without it, or mark it [IgnoreDataMember].");
        if (filled is not null)
            foreach (var (property, value) in filled)
                for (var i = 0; i < rows.Length; i++) property.BoxedSetter(rows[i], value);
        return rows;
    }

    private static ArrowBinding<T> Bind(IArrowArray array, string column, PropertyMap<T> property, JsonNamingPolicy? naming)
    {
        if (TypedColumns.For(array, column) is { } typed)
        {
            if (!Conversions.CanConvert(typed.Input, property.Type, typed.Logical))
                throw new NotSupportedException($"{Conversions.Mismatch(column, Nullable.GetUnderlyingType(typed.Input) ?? typed.Input, property.Type, typed.Logical).Message} " +
                                                $"Property: {typeof(T).Name}.{property.Property.Name}.");
            var binding = typeof(TypedBinding<,,>).MakeGenericType(typeof(T), typed.Input, property.Type);
            return (ArrowBinding<T>)Activator.CreateInstance(binding, typed.Get, Conversions.Get(typed.Input, property.Type, typed.Logical, column), property.Setter)!;
        }
        return new BoxedBinding<T>(column, property, naming);
    }
}

internal abstract class ArrowBinding<T>
{
    public abstract void Assign(IArrowArray array, T[] rows);
}

internal sealed class TypedBinding<T, TIn, TOut>(Func<IArrowArray, int, TIn> get, Func<TIn, TOut> convert, Action<T, TOut> set) : ArrowBinding<T>
{
    public override void Assign(IArrowArray array, T[] rows)
    {
        for (var i = 0; i < rows.Length; i++) set(rows[i], convert(get(array, i)));
    }
}

internal sealed class BoxedBinding<T>(string column, PropertyMap<T> property, JsonNamingPolicy? naming) : ArrowBinding<T> where T : class
{
    public override void Assign(IArrowArray array, T[] rows)
    {
        for (var i = 0; i < rows.Length; i++)
            property.BoxedSetter(rows[i], ValueAssembler.Convert(ArrowValues.Get(array, i, column), property.Type, naming, column));
    }
}

/// <summary>Flat Arrow arrays read as one CLR type each, without boxing.</summary>
internal static class TypedColumns
{
    public sealed record Typed(Type Input, Delegate Get, Logical Logical);

    public static Typed? For(IArrowArray array, string column) => array switch
    {
        Int8Array => Make((Int8Array a, int i) => a.GetValue(i)),
        Int16Array => Make((Int16Array a, int i) => a.GetValue(i)),
        Int32Array => Make((Int32Array a, int i) => a.GetValue(i)),
        Int64Array => Make((Int64Array a, int i) => a.GetValue(i)),
        UInt8Array => Make((UInt8Array a, int i) => a.GetValue(i)),
        UInt16Array => Make((UInt16Array a, int i) => a.GetValue(i)),
        UInt32Array => Make((UInt32Array a, int i) => a.GetValue(i)),
        UInt64Array => Make((UInt64Array a, int i) => a.GetValue(i)),
        FloatArray => Make((FloatArray a, int i) => a.GetValue(i)),
        DoubleArray => Make((DoubleArray a, int i) => a.GetValue(i)),
        BooleanArray => Make((BooleanArray a, int i) => a.GetValue(i)),
        StringArray => Make((StringArray a, int i) => a.GetString(i)),
        LargeStringArray => Make((LargeStringArray a, int i) => a.GetString(i)),
        StringViewArray => Make((StringViewArray a, int i) => a.GetString(i)),
        BinaryArray => Make((BinaryArray a, int i) => a.IsNull(i) ? null : a.GetBytes(i).ToArray()),
        LargeBinaryArray => Make((LargeBinaryArray a, int i) => a.IsNull(i) ? null : a.GetBytes(i).ToArray()),
        Decimal32Array => Make((Decimal32Array a, int i) => a.GetValue(i)),
        Decimal64Array => Make((Decimal64Array a, int i) => a.GetValue(i)),
        Decimal128Array => Make((Decimal128Array a, int i) => (decimal?)ArrowValues.Get(a, i, column)),
        Decimal256Array => Make((Decimal256Array a, int i) => (decimal?)ArrowValues.Get(a, i, column)),
        GuidArray => Make((GuidArray a, int i) => a.GetGuid(i)),
        Date32Array => Make((Date32Array a, int i) => a.GetDateOnly(i)),
        Date64Array => Make((Date64Array a, int i) => a.GetDateOnly(i)),
        TimestampArray t => Make((TimestampArray a, int i) => ArrowValues.Timestamp(a, i), new Logical(ArrowValues.IsUtc(t.Data.DataType))),
        Time32Array => Make((Time32Array a, int i) => a.GetTime(i)),
        Time64Array => Make((Time64Array a, int i) => a.GetTime(i)),
        DurationArray => Make((DurationArray a, int i) => a.GetTimeSpan(i)),
        _ => null,
    };

    private static Typed Make<TArray, TIn>(Func<TArray, int, TIn> get, Logical? logical = null) where TArray : IArrowArray =>
        new(typeof(TIn), (Func<IArrowArray, int, TIn>)((a, i) => get((TArray)a, i)), logical ?? Logical.Plain);
}
