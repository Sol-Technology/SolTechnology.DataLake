using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using SolTechnology.Avro;
using SolTechnology.DataLake.Formats;

namespace SolTechnology.DataLake.Avro;

/// <summary>
/// Avro object container files (<c>.avro</c>) in a folder dataset, decoded by AvroConvert under AvroConvert's own licence:
/// without a licence declared in <c>AvroConvert.License</c>, AvroConvert prints its licence notice.
/// </summary>
internal sealed class AvroFileFormat : ILakeFileFormat
{
    public static readonly AvroFileFormat Instance = new();

    private AvroFileFormat() { }

    public string Name => "avro";

    public IReadOnlyList<string> Extensions { get; } = [".avro"];

    public IAsyncEnumerable<T> ReadAsync<T>(Stream stream, string location, FileScan<T> scan, CancellationToken ct) where T : class =>
        AvroFile<T>.ReadAsync(stream, location, scan, ct);

    public async IAsyncEnumerable<Dictionary<string, object?>> ReadUntypedAsync(Stream stream, string location, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var record in AvroConvert.Deserialize<object>(stream, cancellationToken: ct).ConfigureAwait(false))
            yield return (Dictionary<string, object?>)FileFormats.Plain(record)!;
    }

    public Task<IReadOnlyList<string>> DescribeSchemaAsync(Stream stream, string location, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<string>>([JsonNode.Parse(AvroConvert.GetSchema(stream))!.ToJsonString(new JsonSerializerOptions { WriteIndented = true })]);
}
