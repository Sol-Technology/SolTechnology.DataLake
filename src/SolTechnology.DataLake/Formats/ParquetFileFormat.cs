using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Schema;
using ParquetReadOptions = Parquet.ParquetOptions;

namespace SolTechnology.DataLake.Formats;

/// <summary>Parquet files (<c>.parquet</c>, <c>.parq</c>), the format every folder dataset reads and writes.</summary>
internal sealed class ParquetFileFormat : ILakeFileFormat
{
    public static readonly ParquetFileFormat Instance = new();

    private static readonly ParquetReadOptions Options = new() { UseDateOnlyTypeForDates = true };

    private ParquetFileFormat() { }

    public string Name => "parquet";

    public IReadOnlyList<string> Extensions { get; } = [".parquet", ".parq"];

    public IAsyncEnumerable<T> ReadAsync<T>(Stream stream, string location, FileScan<T> scan, CancellationToken ct) where T : class =>
        ParquetFile<T>.ReadAsync(stream, location, scan, ct);

    public async IAsyncEnumerable<Dictionary<string, object?>> ReadUntypedAsync(Stream stream, string location, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var reader = await ReadFailure.Guard(() => ParquetReader.CreateAsync(stream, Options, leaveStreamOpen: true, ct), location, null).ConfigureAwait(false);
        var columns = ReadFailure.Guard(() => reader.Schema, location).Fields.Select(field =>
        {
            var leaves = new List<DataField>();
            var node = NestedBuilder.Build(field, typeof(object), leaves, null, location, field.Name);
            return (field.Name, Node: node, Leaves: leaves.ToArray());
        }).ToList();

        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            var count = (int)group.RowCount;
            var values = new List<object?[]>();
            foreach (var column in columns)
                values.Add(await ReadFailure.Guard(() => NestedColumn.AssembleAsync(group, column.Node, column.Leaves, count, ct), location, column.Name).ConfigureAwait(false));

            for (var r = 0; r < count; r++)
            {
                var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (var c = 0; c < columns.Count; c++) row[columns[c].Name] = values[c][r];
                yield return row;
            }
        }
    }

    /// <summary>From the footer, no data pages read.</summary>
    public async Task<long> CountAsync(Stream stream, string location, CancellationToken ct)
    {
        await using var reader = await ReadFailure.Guard(() => ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: ct), location, null)
            .ConfigureAwait(false);
        long count = 0;
        for (var g = 0; g < reader.RowGroupCount; g++)
        {
            using var group = reader.OpenRowGroupReader(g);
            count += group.RowCount;
        }
        return count;
    }

    public async Task<IReadOnlyList<string>> DescribeSchemaAsync(Stream stream, string location, CancellationToken ct)
    {
        await using var reader = await ReadFailure.Guard(() => ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: ct), location, null)
            .ConfigureAwait(false);
        var lines = new List<string>();
        foreach (var field in reader.Schema.Fields) Describe(lines, field, "");
        lines.Add($"# {reader.RowGroupCount} row groups, written by {reader.Metadata?.CreatedBy}");
        return lines;
    }

    private static void Describe(List<string> lines, Field field, string indent)
    {
        switch (field)
        {
            case DataField data:
                lines.Add($"{indent}{data.Name}: {TypeName(data)}{(data.IsArray ? "[]" : "")}{(data.IsNullable ? "" : " not null")}");
                break;
            case StructField structField:
                lines.Add($"{indent}{structField.Name}: struct{(structField.IsNullable ? "" : " not null")}");
                foreach (var child in structField.Fields) Describe(lines, child, indent + "  ");
                break;
            case ListField list:
                lines.Add($"{indent}{list.Name}: list{(list.IsNullable ? "" : " not null")}");
                Describe(lines, list.Item, indent + "  ");
                break;
            case MapField map:
                lines.Add($"{indent}{map.Name}: map{(map.IsNullable ? "" : " not null")}");
                Describe(lines, map.Key, indent + "  ");
                Describe(lines, map.Value, indent + "  ");
                break;
        }
    }

    /// <summary>The type of a Parquet column as tools print it: <c>timestamp(micros, UTC)</c>, <c>decimal(18, 2)</c>, <c>string</c>.</summary>
    public static string TypeName(DataField field)
    {
        var logical = field.SchemaElement?.LogicalType;
        if (logical?.TIMESTAMP is { } timestamp)
            return $"timestamp({Unit(timestamp.Unit)}{(timestamp.IsAdjustedToUTC ? ", UTC" : ", no zone")})";
        if (logical?.TIME is { } time) return $"time({Unit(time.Unit)})";
        if (field is DecimalDataField d) return $"decimal({d.Precision}, {d.Scale})";
        if (logical?.UUID is not null || field.ClrType == typeof(Guid)) return "uuid";
        if (field.ClrType == typeof(DateOnly) || logical?.DATE is not null) return "date";
        if (field.ClrType == typeof(ReadOnlyMemory<char>) || field.ClrType == typeof(string)) return "string";
        if (logical?.JSON is not null) return "json";
        if (field.ClrType == typeof(byte[]) || field.ClrType == typeof(ReadOnlyMemory<byte>)) return "binary";
        return field.ClrType.Name.ToLowerInvariant();

        static string Unit(Parquet.Meta.TimeUnit? unit) =>
            unit?.NANOS is not null ? "nanos" : unit?.MICROS is not null ? "micros" : "millis";
    }
}
