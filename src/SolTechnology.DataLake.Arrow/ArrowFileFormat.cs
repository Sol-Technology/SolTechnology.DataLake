using System.Runtime.CompilerServices;
using SolTechnology.DataLake.Formats;

namespace SolTechnology.DataLake.Arrow;

/// <summary>Arrow IPC files and streams in a folder dataset: <c>.arrow</c>, <c>.feather</c>, <c>.ipc</c>, <c>.arrows</c>.</summary>
internal sealed class ArrowFileFormat : ILakeFileFormat
{
    public static readonly ArrowFileFormat Instance = new();

    private ArrowFileFormat() { }

    public string Name => "arrow";

    public IReadOnlyList<string> Extensions { get; } = [".arrow", ".feather", ".ipc", ".arrows"];

    public IAsyncEnumerable<T> ReadAsync<T>(Stream stream, string location, FileScan<T> scan, CancellationToken ct) where T : class =>
        ArrowFile<T>.ReadAsync(stream, location, scan, ct);

    public async IAsyncEnumerable<Dictionary<string, object?>> ReadUntypedAsync(Stream stream, string location, [EnumeratorCancellation] CancellationToken ct)
    {
        using var reader = await ReadFailure.Guard(() => ArrowConvert.OpenIpcAsync(stream, ct), location, null).ConfigureAwait(false);
        while (await ReadFailure.Guard(() => reader.ReadNextRecordBatchAsync(ct).AsTask(), location, null).ConfigureAwait(false) is { } batch)
        {
            using (batch)
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        var name = batch.Schema.GetFieldByIndex(c).Name;
                        row[name] = FileFormats.Plain(ArrowValues.Get(batch.Column(c), r, name));
                    }
                    yield return row;
                }
            }
        }
    }

    public async Task<IReadOnlyList<string>> DescribeSchemaAsync(Stream stream, string location, CancellationToken ct)
    {
        using var reader = await ReadFailure.Guard(() => ArrowConvert.OpenIpcAsync(stream, ct), location, null).ConfigureAwait(false);
        var schema = await reader.GetSchema(ct).ConfigureAwait(false);
        var lines = schema.FieldsList.Select(field => $"{field.Name}: {field.DataType.Name}{(field.IsNullable ? "" : " not null")}").ToList();
        lines.Add("# Arrow IPC file");
        return lines;
    }
}
