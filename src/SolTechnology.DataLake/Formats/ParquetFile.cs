using System.Reflection;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Filtering;
using SolTechnology.DataLake.Mapping;

namespace SolTechnology.DataLake.Formats;

/// <summary>Everything one file read needs besides the file: mapping, partition values from the path, filter, metrics.</summary>
internal sealed record FileScan<T>(
    ClassMap<T> Map,
    IReadOnlyDictionary<PropertyMap<T>, object?> Partitions,
    Predicate? Predicate,
    Func<T, bool>? Residual,
    ScanMetrics Metrics) where T : class;

/// <summary>
/// Parquet to classes, column by column: each mapped column is read into a typed array and assigned to all rows of
/// the row group in one loop. Only columns the class has are read. Row groups whose statistics can't match the
/// filter are skipped.
/// </summary>
internal static class ParquetFile<T> where T : class
{
    private static readonly ParquetOptions Options = new() { UseDateOnlyTypeForDates = true };

    public static async IAsyncEnumerable<T> ReadAsync(Stream stream, string location, FileScan<T> scan, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await using var reader = await ParquetReader.CreateAsync(stream, Options, leaveStreamOpen: true, ct).ConfigureAwait(false);
        var bindings = Bind(reader.Schema, location, scan);
        var byProperty = bindings.ToDictionary(b => b.Property.Property.Name);
        var partitionsByProperty = scan.Partitions.ToDictionary(p => p.Key.Property.Name, p => p.Value);

        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            scan.Metrics.RowGroups++;
            var count = (int)group.RowCount;

            if (scan.Predicate?.Evaluate(name => Facts(name, group, count, byProperty, partitionsByProperty)) == Tri.False)
            {
                scan.Metrics.RowGroupsSkippedByStatistics++;
                continue;
            }

            var rows = new T[count];
            for (var i = 0; i < count; i++) rows[i] = scan.Map.Create();
            foreach (var binding in bindings) await binding.AssignAsync(group, rows, ct).ConfigureAwait(false);
            foreach (var (property, value) in scan.Partitions)
                for (var i = 0; i < count; i++) property.BoxedSetter(rows[i], value);

            scan.Metrics.RowsRead += count;
            foreach (var row in rows)
            {
                if (scan.Residual is not null && !scan.Residual(row)) continue;
                scan.Metrics.RowsReturned++;
                yield return row;
            }
        }
    }

    private static List<ColumnBinding<T>> Bind(ParquetSchema schema, string location, FileScan<T> scan)
    {
        var bindings = new List<ColumnBinding<T>>();
        var bound = new HashSet<PropertyMap<T>>();
        foreach (var field in schema.Fields)
        {
            var property = scan.Map.Find(field.Name);
            if (property is null) continue;
            if (field is not DataField { IsArray: false } data)
                throw new NotSupportedException($"{location}: column '{field.Name}' is nested ({field.SchemaType}); nested columns are not supported yet.");
            bindings.Add(ColumnBinding<T>.Create(data, property, IsUtc(data), location));
            bound.Add(property);
        }

        var missing = scan.Map.Properties.FirstOrDefault(p => !bound.Contains(p) && !scan.Partitions.ContainsKey(p) && !p.AcceptsNull);
        if (missing is not null)
            throw new InvalidDataException($"{location} has no column '{missing.Column}' for {typeof(T).Name}.{missing.Property.Name} ({missing.Type.Name}). " +
                                           $"Make the property nullable to read files without it, or mark it [IgnoreDataMember].");
        return bindings;
    }

    private static ColumnFacts? Facts(string property, ParquetRowGroupReader group, int count,
        Dictionary<string, ColumnBinding<T>> byProperty, Dictionary<string, object?> partitions)
    {
        if (partitions.TryGetValue(property, out var value)) return new ColumnFacts.Exact(value);
        if (!byProperty.TryGetValue(property, out var binding)) return null;
        try
        {
            var stats = group.GetStatistics(binding.Field);
            if (stats is null) return null;
            return new ColumnFacts.Range(
                binding.ToPropertyType(stats.MinValue),
                binding.ToPropertyType(stats.MaxValue),
                stats.NullCount,
                count);
        }
        catch (Exception e) when (e is NotSupportedException or InvalidCastException or InvalidDataException)
        {
            return null;
        }
    }

    // TIMESTAMP(isAdjustedToUTC=true) and the legacy TIMESTAMP_MILLIS/MICROS converted types hold UTC instants.
    private static bool IsUtc(DataField field)
    {
        var element = field.SchemaElement;
        if (element is null) return false;
        if (element.LogicalType?.TIMESTAMP is { } timestamp) return timestamp.IsAdjustedToUTC;
        return element.ConvertedType is Parquet.Meta.ConvertedType.TIMESTAMP_MILLIS or Parquet.Meta.ConvertedType.TIMESTAMP_MICROS;
    }
}

internal abstract class ColumnBinding<T> where T : class
{
    public required DataField Field { get; init; }
    public required PropertyMap<T> Property { get; init; }
    public required bool Utc { get; init; }

    public abstract Task AssignAsync(ParquetRowGroupReader group, T[] rows, CancellationToken ct);

    /// <summary>A statistics value as the property type, for comparing with filter constants.</summary>
    public object? ToPropertyType(object? value) => value switch
    {
        null => null,
        ReadOnlyMemory<char> chars => chars.ToString(),
        _ => Conversions.ConvertValue(value, Property.Type, Utc, Field.Name),
    };

    public static ColumnBinding<T> Create(DataField field, PropertyMap<T> property, bool utc, string location)
    {
        var element = field.ClrType == typeof(ReadOnlyMemory<char>) ? typeof(string) : field.ClrType;
        var input = element == typeof(string) || element == typeof(byte[]) ? element
            : field.IsNullable ? typeof(Nullable<>).MakeGenericType(element)
            : element;
        if (!Conversions.CanConvert(input, property.Type, utc))
            throw new NotSupportedException($"{location}: {Conversions.Mismatch(field.Name, element, property.Type, utc).Message} " +
                                            $"Property: {typeof(T).Name}.{property.Property.Name}.");
        var type = typeof(ColumnBinding<,,>).MakeGenericType(typeof(T), input, property.Type);
        var binding = (ColumnBinding<T>)Activator.CreateInstance(type, Conversions.Get(input, property.Type, utc, field.Name), property.Setter)!;
        return binding.With(field, property, utc);
    }

    protected abstract ColumnBinding<T> With(DataField field, PropertyMap<T> property, bool utc);
}

internal sealed class ColumnBinding<T, TIn, TOut>(Func<TIn, TOut> convert, Action<T, TOut> set) : ColumnBinding<T> where T : class
{
    private static readonly Func<ParquetRowGroupReader, DataField, int, CancellationToken, Task<TIn[]>> Read = CreateReader();

    public override async Task AssignAsync(ParquetRowGroupReader group, T[] rows, CancellationToken ct)
    {
        var values = await Read(group, Field, rows.Length, ct).ConfigureAwait(false);
        for (var i = 0; i < rows.Length; i++) set(rows[i], convert(values[i]));
    }

    protected override ColumnBinding<T> With(DataField field, PropertyMap<T> property, bool utc) =>
        new ColumnBinding<T, TIn, TOut>(convert, set) { Field = field, Property = property, Utc = utc };

    private static Func<ParquetRowGroupReader, DataField, int, CancellationToken, Task<TIn[]>> CreateReader()
    {
        var name = typeof(TIn) == typeof(string) ? nameof(ColumnReaders.Strings)
            : typeof(TIn) == typeof(byte[]) ? nameof(ColumnReaders.Bytes)
            : Nullable.GetUnderlyingType(typeof(TIn)) is not null ? nameof(ColumnReaders.Nullable)
            : nameof(ColumnReaders.Required);
        var method = typeof(ColumnReaders).GetMethod(name, BindingFlags.Static | BindingFlags.Public)!;
        if (method.IsGenericMethodDefinition)
            method = method.MakeGenericMethod(Nullable.GetUnderlyingType(typeof(TIn)) ?? typeof(TIn));
        return method.CreateDelegate<Func<ParquetRowGroupReader, DataField, int, CancellationToken, Task<TIn[]>>>();
    }
}

internal static class ColumnReaders
{
    public static async Task<string[]> Strings(ParquetRowGroupReader group, DataField field, int count, CancellationToken ct)
    {
        var values = new string?[count];
        await group.ReadAsync(field, values.AsMemory(), null, ct).ConfigureAwait(false);
        return values!;
    }

    public static async Task<byte[][]> Bytes(ParquetRowGroupReader group, DataField field, int count, CancellationToken ct)
    {
        var values = new byte[]?[count];
        await group.ReadAsync(field, values.AsMemory(), null, ct).ConfigureAwait(false);
        return values!;
    }

    public static async Task<TValue?[]> Nullable<TValue>(ParquetRowGroupReader group, DataField field, int count, CancellationToken ct) where TValue : struct
    {
        var values = new TValue?[count];
        await group.ReadAsync(field, values.AsMemory(), null, ct).ConfigureAwait(false);
        return values;
    }

    public static async Task<TValue[]> Required<TValue>(ParquetRowGroupReader group, DataField field, int count, CancellationToken ct) where TValue : struct
    {
        var values = new TValue[count];
        await group.ReadAsync(field, values.AsMemory(), null, ct).ConfigureAwait(false);
        return values;
    }
}
