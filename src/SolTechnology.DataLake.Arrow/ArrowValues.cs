using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace SolTechnology.DataLake.Arrow;

/// <summary>
/// One value of an Arrow array as a plain CLR value: numbers, <see cref="string"/>, <c>byte[]</c>, <see cref="decimal"/>,
/// <see cref="DateOnly"/>, <see cref="TimeOnly"/>, <see cref="TimeSpan"/>, <see cref="Guid"/>, <see cref="DateTime"/> (Kind.Utc for
/// a timestamp with a time zone, Unspecified without), a list as <c>List&lt;object?&gt;</c>, a struct as
/// <c>Dictionary&lt;string, object?&gt;</c>, a map as a list of <c>KeyValuePair&lt;object?, object?&gt;</c>.
/// </summary>
internal static class ArrowValues
{
    public static object? Get(IArrowArray array, int index, string path)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            Int8Array a => a.GetValue(index),
            Int16Array a => a.GetValue(index),
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            UInt8Array a => a.GetValue(index),
            UInt16Array a => a.GetValue(index),
            UInt32Array a => a.GetValue(index),
            UInt64Array a => a.GetValue(index),
            FloatArray a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            HalfFloatArray a => (float?)a.GetValue(index),
            BooleanArray a => a.GetValue(index),
            StringArray a => a.GetString(index),
            LargeStringArray a => a.GetString(index),
            StringViewArray a => a.GetString(index),
            BinaryArray a => a.GetBytes(index).ToArray(),
            LargeBinaryArray a => a.GetBytes(index).ToArray(),
            BinaryViewArray a => a.GetBytes(index).ToArray(),
            Decimal32Array a => a.GetValue(index),
            Decimal64Array a => a.GetValue(index),
            Decimal128Array a => Decimal(() => a.GetValue(index), path),
            Decimal256Array a => Decimal(() => a.GetValue(index), path),
            GuidArray a => a.GetGuid(index),
            FixedSizeBinaryArray a => a.GetBytes(index).ToArray(),
            Date32Array a => a.GetDateOnly(index),
            Date64Array a => a.GetDateOnly(index),
            TimestampArray a => Timestamp(a, index),
            Time32Array a => a.GetTime(index),
            Time64Array a => a.GetTime(index),
            DurationArray a => a.GetTimeSpan(index),
            MapArray a => Map(a, index, path),
            ListArray a => Items(a.Values, a.ValueOffsets[index], a.ValueOffsets[index + 1], path),
            LargeListArray a => Items(a.Values, checked((int)a.ValueOffsets[index]), checked((int)a.ValueOffsets[index + 1]), path),
            FixedSizeListArray a => FixedItems(a, index, path),
            StructArray a => Struct(a, index, path),
            DictionaryArray a => Get(a.Dictionary, DictionaryIndex(a.Indices, index), path),
            NullArray => null,
            _ => throw new NotSupportedException($"Column '{path}': Arrow type {array.Data.DataType.Name} is not supported."),
        };
    }

    /// <summary>Whether timestamps in <paramref name="type"/> are UTC instants (Arrow: a time zone is set).</summary>
    public static bool IsUtc(IArrowType type) => type is TimestampType { Timezone.Length: > 0 };

    public static DateTime? Timestamp(TimestampArray array, int index)
    {
        if (array.GetTimestamp(index) is not { } value) return null;
        return IsUtc(array.Data.DataType)
            ? value.UtcDateTime
            : DateTime.SpecifyKind(value.UtcDateTime, DateTimeKind.Unspecified);
    }

    private static decimal? Decimal(Func<decimal?> get, string path)
    {
        try
        {
            return get();
        }
        catch (OverflowException e)
        {
            throw new InvalidDataException($"Column '{path}' has a decimal that does not fit System.Decimal (28-29 digits).", e);
        }
    }

    private static List<object?> Items(IArrowArray values, int from, int to, string path)
    {
        var items = new List<object?>(to - from);
        for (var i = from; i < to; i++) items.Add(Get(values, i, path + "[]"));
        return items;
    }

    private static List<object?> FixedItems(FixedSizeListArray array, int index, string path)
    {
        var slice = array.GetSlicedValues(index);
        return Items(slice, 0, slice.Length, path);
    }

    private static Dictionary<string, object?> Struct(StructArray array, int index, string path)
    {
        var type = (StructType)array.Data.DataType;
        var result = new Dictionary<string, object?>(type.Fields.Count, StringComparer.OrdinalIgnoreCase);
        for (var f = 0; f < type.Fields.Count; f++)
            result[type.Fields[f].Name] = Get(array.Fields[f], index, $"{path}.{type.Fields[f].Name}");
        return result;
    }

    private static List<KeyValuePair<object?, object?>> Map(MapArray array, int index, string path)
    {
        var from = array.ValueOffsets[index];
        var to = array.ValueOffsets[index + 1];
        var pairs = new List<KeyValuePair<object?, object?>>(to - from);
        for (var i = from; i < to; i++)
            pairs.Add(new(Get(array.Keys, i, path + ".key"), Get(array.Values, i, path + ".value")));
        return pairs;
    }

    private static int DictionaryIndex(IArrowArray indices, int index) => indices switch
    {
        Int8Array a => a.GetValue(index)!.Value,
        Int16Array a => a.GetValue(index)!.Value,
        Int32Array a => a.GetValue(index)!.Value,
        Int64Array a => checked((int)a.GetValue(index)!.Value),
        UInt8Array a => a.GetValue(index)!.Value,
        UInt16Array a => a.GetValue(index)!.Value,
        UInt32Array a => checked((int)a.GetValue(index)!.Value),
        UInt64Array a => checked((int)a.GetValue(index)!.Value),
        _ => throw new NotSupportedException($"Dictionary indices of type {indices.Data.DataType.Name} are not supported."),
    };
}
