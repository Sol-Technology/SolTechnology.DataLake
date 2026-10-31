using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using SolTechnology.Time;

namespace SolTechnology.DataLake.Mapping;

/// <summary>
/// Conversions from a column's CLR type to a property type. Only lossless ones: same type, numeric widening,
/// string or integer to enum, <see cref="DateOnly"/> to <see cref="DateTime"/>, <see cref="TimeSpan"/> to <see cref="TimeOnly"/>.
/// Timestamps follow SolTechnology.DateTimeZone: a UTC instant reads into <see cref="DateTimeZone"/> (in UTC),
/// <see cref="DateTimeOffset"/> or <see cref="DateTime"/> (Kind.Utc); a timestamp without a zone into
/// <see cref="UnzonedDateTime"/> or <see cref="DateTime"/> (Kind.Unspecified). Anything else is an error, never a guess.
/// </summary>
/// <summary>Unit of a TIME column stored as an integer.</summary>
internal enum TimeScale { None, Millis, Micros, Nanos }

/// <summary>What the physical value of a column means: a UTC instant, a time of day stored as an integer, or JSON text stored as bytes.</summary>
internal readonly record struct Logical(bool Utc, TimeScale Time = TimeScale.None, bool Json = false)
{
    public static readonly Logical Plain = new(false);
}

internal static class Conversions
{
    private static readonly ConcurrentDictionary<(Type From, Type To, Logical Logical, string Column), Delegate> Cache = new();

    private static readonly Dictionary<Type, Type[]> Widening = new()
    {
        [typeof(sbyte)] = [typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal)],
        [typeof(byte)] = [typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal)],
        [typeof(short)] = [typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal)],
        [typeof(ushort)] = [typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal)],
        [typeof(int)] = [typeof(long), typeof(double), typeof(decimal)],
        [typeof(uint)] = [typeof(long), typeof(ulong), typeof(double), typeof(decimal)],
        [typeof(long)] = [typeof(decimal)],
        [typeof(ulong)] = [typeof(decimal)],
        [typeof(float)] = [typeof(double)],
    };

    /// <summary>
    /// <c>Func&lt;TFrom, TTo&gt;</c>. <paramref name="from"/> is what the reader produces: a non-nullable value type,
    /// <see cref="Nullable{T}"/>, or a reference type. <paramref name="logical"/>: UTC instants, time units.
    /// </summary>
    public static Delegate Get(Type from, Type to, Logical logical, string column) =>
        Cache.GetOrAdd((from, to, logical, column), key => Build(key.From, key.To, key.Logical, key.Column));

    /// <summary>Whether a column of <paramref name="from"/> can be read into <paramref name="to"/>.</summary>
    public static bool CanConvert(Type from, Type to, Logical logical)
    {
        var source = Nullable.GetUnderlyingType(from) ?? from;
        var target = Nullable.GetUnderlyingType(to) ?? to;
        return Core(Expression.Parameter(source), target, logical) is not null;
    }

    /// <summary>Boxed conversion for single values (statistics, partition values).</summary>
    public static object? ConvertValue(object? value, Type to, Logical logical, string column)
    {
        if (value is null) return null;
        var target = Nullable.GetUnderlyingType(to) ?? to;
        var convert = BoxedCache.GetOrAdd((value.GetType(), target, logical), key =>
        {
            var input = Expression.Parameter(typeof(object));
            var body = Core(Expression.Convert(input, key.From), key.To, key.Logical);
            return body is null ? null : Expression.Lambda<Func<object, object>>(Expression.Convert(body, typeof(object)), input).Compile();
        });
        return convert is not null ? convert(value) : throw Mismatch(column, value.GetType(), to, logical);
    }

    private static readonly ConcurrentDictionary<(Type From, Type To, Logical Logical), Func<object, object>?> BoxedCache = new();

    /// <summary>A partition value from a path segment (<c>date=2026-10-29</c>) as the property type.</summary>
    public static object? ParsePartition(string? text, Type to, string column)
    {
        if (text is null) return null;
        var target = Nullable.GetUnderlyingType(to) ?? to;
        var c = CultureInfo.InvariantCulture;
        try
        {
            return target switch
            {
                _ when target == typeof(string) => text,
                _ when target.IsEnum => Enum.Parse(target, text, ignoreCase: true),
                _ when target == typeof(DateOnly) => DateOnly.Parse(text, c),
                _ when target == typeof(DateTime) => DateTime.Parse(text, c, DateTimeStyles.RoundtripKind),
                _ when target == typeof(DateTimeOffset) => DateTimeOffset.Parse(text, c, DateTimeStyles.AssumeUniversal),
                _ when target == typeof(DateTimeZone) => DateTimeZone.Parse(text),
                _ when target == typeof(UnzonedDateTime) => UnzonedDateTime.Parse(text),
                _ when target == typeof(Guid) => Guid.Parse(text),
                _ when target == typeof(bool) => bool.Parse(text),
                _ => System.Convert.ChangeType(text, target, c),
            };
        }
        catch (Exception e) when (e is FormatException or OverflowException or ArgumentException or InvalidCastException or DateTimeZoneException)
        {
            throw new InvalidDataException($"Partition value '{text}' of '{column}' is not a valid {target.Name}.", e);
        }
    }

    private static Delegate Build(Type from, Type to, Logical logical, string column)
    {
        var input = Expression.Parameter(from, "value");
        var source = Nullable.GetUnderlyingType(from);
        var target = Nullable.GetUnderlyingType(to) ?? to;
        var toAcceptsNull = !to.IsValueType || Nullable.GetUnderlyingType(to) is not null;

        Expression Converted(Expression nonNull) =>
            Lift(Core(nonNull, target, logical) ?? throw Mismatch(column, source ?? from, to, logical), to);

        Expression body;
        if (source is not null || !from.IsValueType)
        {
            var hasValue = source is not null ? Expression.Property(input, "HasValue") : (Expression)Expression.NotEqual(input, Expression.Constant(null, from));
            var value = source is not null ? Expression.Property(input, "Value") : (Expression)input;
            var whenNull = toAcceptsNull
                ? (Expression)Expression.Default(to)
                : Expression.Throw(Expression.New(
                    typeof(InvalidDataException).GetConstructor([typeof(string)])!,
                    Expression.Constant($"Column '{column}' has a null value, but property type {to.Name} does not accept null. Make it {to.Name}?.")), to);
            body = Expression.Condition(hasValue, Converted(value), whenNull);
        }
        else
        {
            body = Converted(input);
        }

        return Expression.Lambda(typeof(Func<,>).MakeGenericType(from, to), body, input).Compile();
    }

    // Non-null value of the column's type to the target's underlying type; null when there is no lossless conversion.
    private static Expression? Core(Expression value, Type target, Logical logical)
    {
        var source = value.Type;
        var utc = logical.Utc;
        if (logical.Time != TimeScale.None && (source == typeof(long) || source == typeof(int)))
            return Core(TimeOfDay(value, logical.Time), target, Logical.Plain);
        // Parquet.Net returns timestamps without a zone as Kind.Local; they are wall-clock values, so Unspecified.
        if (source == typeof(DateTime) && target == typeof(DateTime))
            return Expression.Call(typeof(DateTime), nameof(DateTime.SpecifyKind), null, value,
                Expression.Constant(utc ? DateTimeKind.Utc : DateTimeKind.Unspecified));
        if (source == target) return value;
        if (Widening.TryGetValue(source, out var wider) && wider.Contains(target)) return Expression.Convert(value, target);
        if (target.IsEnum && source == typeof(string))
            return Expression.Call(typeof(Enum), nameof(Enum.Parse), [target], value, Expression.Constant(true));
        if (target.IsEnum && (source == Enum.GetUnderlyingType(target) || source == typeof(int) || source == typeof(long)))
            return Expression.Convert(Expression.Convert(value, Enum.GetUnderlyingType(target)), target);
        if (source == typeof(DateTime) && target == typeof(DateTimeOffset) && utc)
            return Expression.New(typeof(DateTimeOffset).GetConstructor([typeof(DateTime)])!,
                Expression.Call(typeof(DateTime), nameof(DateTime.SpecifyKind), null, value, Expression.Constant(DateTimeKind.Utc)));
        if (source == typeof(DateTime) && target == typeof(DateTimeZone) && utc)
            return Expression.Call(typeof(DateTimeZone), nameof(DateTimeZone.FromUtc), null,
                Expression.Call(typeof(DateTime), nameof(DateTime.SpecifyKind), null, value, Expression.Constant(DateTimeKind.Utc)),
                Expression.Constant(TimeZoneInfo.Utc));
        if (source == typeof(DateTime) && target == typeof(UnzonedDateTime) && !utc)
            return Expression.Call(typeof(UnzonedDateTime), nameof(UnzonedDateTime.FromDateTime), null, value);
        if (source == typeof(DateOnly) && target == typeof(DateTime))
            return Expression.Call(value, nameof(DateOnly.ToDateTime), null, Expression.Constant(TimeOnly.MinValue));
        if (source == typeof(TimeSpan) && target == typeof(TimeOnly))
            return Expression.Call(typeof(TimeOnly), nameof(TimeOnly.FromTimeSpan), null, value);
        if (source == typeof(TimeOnly) && target == typeof(TimeSpan))
            return Expression.Call(value, nameof(TimeOnly.ToTimeSpan), null);
        if (source == typeof(byte[]) && target == typeof(Guid))
            return Expression.New(typeof(Guid).GetConstructor([typeof(ReadOnlySpan<byte>), typeof(bool)])!,
                Expression.Convert(value, typeof(ReadOnlySpan<byte>)), Expression.Constant(true));
        if (source == typeof(byte[]) && target == typeof(string) && logical.Json)
            return Expression.Call(Expression.Constant(System.Text.Encoding.UTF8), nameof(System.Text.Encoding.GetString), null, value);
        return null;
    }

    // TIME as an integer count of millis / micros / nanos since midnight.
    private static Expression TimeOfDay(Expression value, TimeScale scale)
    {
        Expression ticks = Expression.Convert(value, typeof(long));
        ticks = scale switch
        {
            TimeScale.Millis => Expression.Multiply(ticks, Expression.Constant(TimeSpan.TicksPerMillisecond)),
            TimeScale.Micros => Expression.Multiply(ticks, Expression.Constant(TimeSpan.TicksPerMicrosecond)),
            _ => Expression.Divide(ticks, Expression.Constant(TimeSpan.NanosecondsPerTick)),
        };
        return Expression.New(typeof(TimeOnly).GetConstructor([typeof(long)])!, ticks);
    }

    private static Expression Lift(Expression value, Type to) => value.Type == to ? value : Expression.Convert(value, to);

    public static NotSupportedException Mismatch(string column, Type from, Type to, Logical logical)
    {
        var utc = logical.Utc;
        var target = Nullable.GetUnderlyingType(to) ?? to;
        if (from == typeof(DateTime) && !utc && (target == typeof(DateTimeOffset) || target == typeof(DateTimeZone)))
            return new($"Column '{column}' is a timestamp without a time zone; read it into UnzonedDateTime (then .InZone(zone) if you know the zone) or DateTime, not {target.Name}.");
        if (from == typeof(DateTime) && utc && target == typeof(UnzonedDateTime))
            return new($"Column '{column}' holds UTC instants; read it into DateTimeZone, DateTimeOffset or DateTime, not UnzonedDateTime.");
        return new($"Column '{column}' of type {from.Name} can't be read into {to.Name} without losing data.");
    }
}
