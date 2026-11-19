using System.Globalization;
using System.Reflection;
using Parquet.Schema;
using SolTechnology.Time;

namespace SolTechnology.DataLake.Writing;

/// <summary>
/// How a property type is stored in Parquet: the column definition and a conversion to the CLR value Parquet.Net
/// takes. The reverse of <see cref="Mapping.Conversions"/>, so a written file reads back into the same class.
/// </summary>
internal sealed class ValueKind
{
    /// <summary>The CLR type Parquet.Net is given: the type itself, <see cref="DateTime"/> for timestamps, <see cref="long"/> for TIME, string for enums.</summary>
    public required Type Store { get; init; }

    /// <summary>(column name, nullable) → column definition.</summary>
    public required Func<string, bool, DataField> Field { get; init; }

    /// <summary><c>Func&lt;TValue, TStore&gt;</c> for the non-null value.</summary>
    public required Delegate Convert { get; init; }

    /// <summary>The same conversion for boxed values, for nested columns.</summary>
    public required Func<object, object> Boxed { get; init; }

    /// <summary>A MICROS timestamp, whose statistics Parquet.Net writes wrong (<see cref="FooterPatch"/>).</summary>
    public bool Timestamp { get; init; }

    /// <summary>A stored timestamp as microseconds since the epoch, the value its statistics hold.</summary>
    public static long Micros(DateTime stored) => (stored.Ticks - DateTime.UnixEpoch.Ticks) / 10;
}

internal static class ValueKinds
{
    private static readonly Type[] SameTypes =
    [
        typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong),
        typeof(float), typeof(double), typeof(string), typeof(byte[]), typeof(Guid), typeof(DateOnly),
    ];

    /// <summary>The kind for a non-nullable <paramref name="type"/>, or null when it is not a single value (class, list, map) or not supported.</summary>
    public static ValueKind? For(Type type, PropertyInfo? property, string column)
    {
        if (SameTypes.Contains(type))
            return (ValueKind)typeof(ValueKinds).GetMethod(nameof(Same), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(type).Invoke(null, null)!;
        if (type.IsEnum)
            return (ValueKind)typeof(ValueKinds).GetMethod(nameof(Enum), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(type).Invoke(null, null)!;

        if (type == typeof(decimal))
        {
            var attribute = property?.GetCustomAttribute<DecimalPrecisionAttribute>();
            var (precision, scale) = (attribute?.Precision ?? 38, attribute?.Scale ?? 18);
            return Make<decimal, decimal>(v => CheckDecimal(v, precision, scale, column),
                (name, nullable) => new DecimalDataField(name, precision, scale, isNullable: nullable));
        }

        // Timestamps in microseconds, like Spark and Iceberg; the last digit of a DateTime (100 ns) is dropped.
        if (type == typeof(DateTime)) return Instant<DateTime>(v => UtcOf(v, column));
        if (type == typeof(DateTimeOffset)) return Instant<DateTimeOffset>(v => v.UtcDateTime);
        if (type == typeof(DateTimeZone)) return Instant<DateTimeZone>(v => v.UtcDateTime);
        if (type == typeof(UnzonedDateTime))
            return Make<UnzonedDateTime, DateTime>(v => Truncate(DateTime.SpecifyKind(v.ToDateTime(), DateTimeKind.Unspecified)),
                (name, nullable) => new DateTimeDataField(name, DateTimeFormat.Timestamp, isAdjustedToUTC: false, DateTimeTimeUnit.Micros, nullable), timestamp: true);

        if (type == typeof(TimeOnly))
            return Make<TimeOnly, long>(v => v.Ticks / 10, Time);
        if (type == typeof(TimeSpan))
            return Make<TimeSpan, long>(v => v >= TimeSpan.Zero && v < TimeSpan.FromDays(1) ? v.Ticks / 10
                    : throw new InvalidDataException(string.Create(CultureInfo.InvariantCulture, $"Column '{column}': {v} is not a time of day, so it can't be written as TIME.")),
                Time);

        return null;
    }

    // A time of day, not an instant: isAdjustedToUTC=false, which Iceberg and Spark expect (DuckDB reads true as TIMETZ).
    private static DataField Time(string name, bool nullable) =>
        new TimeDataField(name, TimeUnitPrecision.Micros, nullable) { IsAdjustedToUtc = false };

    private static ValueKind Same<TValue>() =>
        Make<TValue, TValue>(v => v, (name, nullable) => new DataField(name, typeof(TValue), nullable));

    private static ValueKind Enum<TEnum>() where TEnum : struct, System.Enum =>
        Make<TEnum, string>(v => v.ToString(), (name, nullable) => new DataField(name, typeof(string), nullable));

    private static ValueKind Instant<TValue>(Func<TValue, DateTime> utc) =>
        Make<TValue, DateTime>(v => Truncate(utc(v)),
            (name, nullable) => new DateTimeDataField(name, DateTimeFormat.Timestamp, isAdjustedToUTC: true, DateTimeTimeUnit.Micros, nullable), timestamp: true);

    private static ValueKind Make<TValue, TStore>(Func<TValue, TStore> convert, Func<string, bool, DataField> field, bool timestamp = false) => new()
    {
        Store = typeof(TStore),
        Field = field,
        Convert = convert,
        Boxed = v => convert((TValue)v)!,
        Timestamp = timestamp,
    };

    private static DateTime Truncate(DateTime value) => new(value.Ticks - value.Ticks % 10, value.Kind);

    /// <summary>The UTC instant of a <see cref="DateTime"/> (Kind.Utc or Local), <see cref="DateTimeOffset"/> or <see cref="DateTimeZone"/>.</summary>
    public static DateTime UtcOf(object value, string column) => value switch
    {
        DateTime { Kind: DateTimeKind.Utc } v => v,
        DateTime { Kind: DateTimeKind.Local } v => v.ToUniversalTime(),
        DateTime v => throw new InvalidDataException(string.Create(CultureInfo.InvariantCulture,
            $"Column '{column}': DateTime {v:O} has Kind.Unspecified, so the instant is unknown. Use Kind.Utc, or UnzonedDateTime for wall-clock times.")),
        DateTimeOffset v => v.UtcDateTime,
        DateTimeZone v => v.UtcDateTime,
        _ => throw new ArgumentException($"{value.GetType().Name} is not an instant.", nameof(value)),
    };

    /// <summary>The value unchanged, or an error when it does not fit decimal(<paramref name="precision"/>, <paramref name="scale"/>) without rounding.</summary>
    public static decimal CheckDecimal(decimal value, int precision, int scale, string column)
    {
        if (value.Scale > scale && decimal.Round(value, scale) != value)
            throw new InvalidDataException(string.Create(CultureInfo.InvariantCulture, $"Column '{column}': {value} has more than {scale} digits after the point. Round it, or set [DecimalPrecision] on the property."));
        if (precision - scale < 29 && Math.Abs(decimal.Truncate(value)) >= Pow10(precision - scale))
            throw new InvalidDataException(string.Create(CultureInfo.InvariantCulture, $"Column '{column}': {value} does not fit decimal({precision}, {scale}). Set [DecimalPrecision] on the property."));
        return value;
    }

    private static decimal Pow10(int digits)
    {
        var result = 1m;
        for (var i = 0; i < digits; i++) result *= 10;
        return result;
    }
}
