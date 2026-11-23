using System.Runtime.CompilerServices;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// Rows as dictionaries, without a class: column name to value, structs as dictionaries, lists as lists, partition
/// values as text. For tools (<c>datalake head</c>), not for application code.
/// </summary>
internal static class UntypedReader
{
    public static async IAsyncEnumerable<Dictionary<string, object?>> ReadAsync(ILakeStorage storage, string location, FileFormats formats,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var (file, format) in await DatasetFiles.ListAsync(storage, location, formats, ct).ConfigureAwait(false))
        {
            var partitions = HivePartitions.Parse(file.RelativePath);
            await using var stream = new RangeStream(storage, file);
            await foreach (var row in format.ReadUntypedAsync(stream, file.Location, ct).ConfigureAwait(false))
            {
                foreach (var (key, value) in partitions) row.TryAdd(key, value);
                yield return row;
            }
        }
    }
}
