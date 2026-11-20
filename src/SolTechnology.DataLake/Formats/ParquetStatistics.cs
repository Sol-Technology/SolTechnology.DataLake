using System.Numerics;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// Parquet.Net returns row group statistics as physical values: a timestamp as an <see cref="long"/> in the column's
/// unit, a date as days since 1970-01-01, a decimal as <see cref="BigDecimal"/>. This turns them into the column's CLR
/// value, or null when unknown. Null means "can't prune", so any doubt ends here.
/// </summary>
internal static class ParquetStatistics
{
    private static readonly int EpochDay = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;

    /// <summary>
    /// Parquet.Net 6.1 writes min/max of MICROS and NANOS timestamps in milliseconds (checked with pyarrow), so those
    /// statistics are wrong in files it wrote. Other writers (Spark, PyIceberg, pyarrow, Trino) are fine.
    /// </summary>
    public static bool TrustsTimestamps(string? createdBy) =>
        createdBy is null || !createdBy.StartsWith("Parquet.Net", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// TIMESTAMP(isAdjustedToUTC=true) and legacy TIMESTAMP_MILLIS/MICROS hold UTC instants; TIME stored as an integer
    /// needs its unit.
    /// </summary>
    public static Logical LogicalOf(DataField field)
    {
        var element = field.SchemaElement;
        if (element is null) return Logical.Plain;
        var utc = element.LogicalType?.TIMESTAMP is { } timestamp
            ? timestamp.IsAdjustedToUTC
            : element.ConvertedType is ConvertedType.TIMESTAMP_MILLIS or ConvertedType.TIMESTAMP_MICROS;
        var time = TimeScale.None;
        if (field.ClrType == typeof(long) || field.ClrType == typeof(int))
        {
            var unit = element.LogicalType?.TIME?.Unit;
            time = unit?.MILLIS is not null || element.ConvertedType == ConvertedType.TIME_MILLIS ? TimeScale.Millis
                : unit?.MICROS is not null || element.ConvertedType == ConvertedType.TIME_MICROS ? TimeScale.Micros
                : unit?.NANOS is not null ? TimeScale.Nanos
                : TimeScale.None;
        }
        return new Logical(utc, time, element.LogicalType?.JSON is not null || element.ConvertedType == ConvertedType.JSON);
    }

    /// <summary>
    /// The CLR type DataLake reads a column as. Parquet.Net 6 reports text and binary as <see cref="ReadOnlyMemory{T}"/>
    /// views into its buffers; DataLake hands out owned <see cref="string"/> and <c>byte[]</c>.
    /// </summary>
    public static System.Type ClrOf(DataField field) =>
        field.ClrType == typeof(ReadOnlyMemory<char>) ? typeof(string)
        : field.ClrType == typeof(ReadOnlyMemory<byte>) ? typeof(byte[])
        : field.ClrType;

    public static object? Decode(object? raw, DataField field, bool trustTimestamps)
    {
        switch (raw)
        {
            case null:
                return null;
            case ReadOnlyMemory<char> chars:
                return Ascii(chars.ToString());
            case string text:
                return Ascii(text);
            case ReadOnlyMemory<byte> bytes:
                return bytes.ToArray();
            case BigDecimal big:
                return ToDecimal(big);
        }

        if (field.ClrType == typeof(DateTime))
            return raw switch
            {
                DateTime dateTime => dateTime,
                long value when TicksPerUnit(field.SchemaElement, trustTimestamps) is { } ticks => DateTime.UnixEpoch.AddTicks(checked(value * ticks)),
                long value when NanosPerUnit(field.SchemaElement, trustTimestamps) => DateTime.UnixEpoch.AddTicks(value / 100),
                _ => null,
            };

        if (field.ClrType == typeof(DateOnly))
            return raw switch
            {
                DateOnly date => date,
                int days => DateOnly.FromDayNumber(EpochDay + days),
                _ => null,
            };

        // DECIMAL stored as INT32 / INT64 (Spark's default up to 18 digits) comes back unscaled.
        if (field.ClrType == typeof(decimal))
            return raw is int or long && ScaleOf(field) is { } scale and <= 28
                ? System.Convert.ToInt64(raw) / Pow10(scale)
                : raw as decimal?;

        // Unsigned integers are stored signed: anything above the signed maximum reads back negative.
        if (IsUnsigned(field.SchemaElement)) return null;

        // TIME columns and anything else not stored as its own CLR type: no pruning.
        return raw.GetType() == field.ClrType || IsNumber(raw) && IsNumber(field.ClrType) ? raw : null;
    }

    // Text statistics are ordered by UTF-8 bytes, and parquet-mr before 1.8 compared them as signed bytes. For ASCII
    // both orders and .NET's ordinal order agree; anything else is not used.
    private static string? Ascii(string text) => System.Text.Ascii.IsValid(text) ? text : null;

    private static int? ScaleOf(DataField field) =>
        field is DecimalDataField d ? d.Scale : field.SchemaElement?.LogicalType?.DECIMAL?.Scale ?? field.SchemaElement?.Scale;

    private static decimal Pow10(int digits)
    {
        var result = 1m;
        for (var i = 0; i < digits; i++) result *= 10;
        return result;
    }

    private static bool IsUnsigned(SchemaElement? element) =>
        element?.LogicalType?.INTEGER is { IsSigned: false }
        || element?.ConvertedType is ConvertedType.UINT_8 or ConvertedType.UINT_16 or ConvertedType.UINT_32 or ConvertedType.UINT_64;

    private static long? TicksPerUnit(SchemaElement? element, bool trust)
    {
        var unit = element?.LogicalType?.TIMESTAMP?.Unit;
        if (unit?.MILLIS is not null || element?.ConvertedType == ConvertedType.TIMESTAMP_MILLIS) return TimeSpan.TicksPerMillisecond;
        if ((unit?.MICROS is not null || element?.ConvertedType == ConvertedType.TIMESTAMP_MICROS) && trust) return TimeSpan.TicksPerMicrosecond;
        return null;
    }

    private static bool NanosPerUnit(SchemaElement? element, bool trust) =>
        trust && element?.LogicalType?.TIMESTAMP?.Unit?.NANOS is not null;

    private static decimal? ToDecimal(BigDecimal big)
    {
        try
        {
            return (decimal)big.UnscaledValue / (decimal)BigInteger.Pow(10, big.Scale);
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    private static bool IsNumber(object o) => IsNumber(o.GetType());

    private static bool IsNumber(System.Type t) =>
        t == typeof(sbyte) || t == typeof(byte) || t == typeof(short) || t == typeof(ushort) || t == typeof(int) || t == typeof(uint)
        || t == typeof(long) || t == typeof(ulong) || t == typeof(float) || t == typeof(double) || t == typeof(decimal);
}
