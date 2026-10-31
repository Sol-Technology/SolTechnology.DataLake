using System.Dynamic;
using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Schema;
using SolTechnology.Avro;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// Rows as dictionaries, without a class: column name to value, structs as dictionaries, lists as lists, partition
/// values as text. For tools (<c>datalake head</c>), not for application code.
/// </summary>
internal static class UntypedReader
{
    private static readonly ParquetOptions Options = new() { UseDateOnlyTypeForDates = true };

    public static async IAsyncEnumerable<Dictionary<string, object?>> ReadAsync(ILakeStorage storage, string location, [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var file in await DatasetFiles.ListAsync(storage, location, ct).ConfigureAwait(false))
        {
            var partitions = HivePartitions.Parse(file.RelativePath);
            await using var stream = new RangeStream(storage, file);
            var rows = DatasetFiles.FormatOf(file.RelativePath) == FileFormat.Avro ? Avro(stream, ct) : Parquet(stream, file.Location, ct);
            await foreach (var row in rows.ConfigureAwait(false))
            {
                foreach (var (key, value) in partitions) row.TryAdd(key, value);
                yield return row;
            }
        }
    }

    private static async IAsyncEnumerable<Dictionary<string, object?>> Parquet(Stream stream, string location, [EnumeratorCancellation] CancellationToken ct)
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

    private static async IAsyncEnumerable<Dictionary<string, object?>> Avro(Stream stream, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var record in AvroConvert.Deserialize<object>(stream, cancellationToken: ct).ConfigureAwait(false))
            yield return (Dictionary<string, object?>)Plain(record)!;
    }

    // ExpandoObject records into dictionaries, all the way down.
    private static object? Plain(object? value) => value switch
    {
        ExpandoObject record => ((IDictionary<string, object?>)record).ToDictionary(kv => kv.Key, kv => Plain(kv.Value), StringComparer.OrdinalIgnoreCase),
        IDictionary<string, object?> map => map.ToDictionary(kv => kv.Key, kv => Plain(kv.Value)),
        System.Collections.IList list when value is not byte[] => list.Cast<object?>().Select(Plain).ToList(),
        _ => value,
    };
}
