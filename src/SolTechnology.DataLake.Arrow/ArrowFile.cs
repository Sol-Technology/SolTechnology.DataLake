using System.Runtime.CompilerServices;
using SolTechnology.DataLake.Formats;

namespace SolTechnology.DataLake.Arrow;

/// <summary>
/// Arrow IPC files (Feather v2) in a dataset, batch by batch, with the same mapping as Parquet. Arrow files have no
/// statistics, so only partitions skip files.
/// </summary>
internal static class ArrowFile<T> where T : class
{
    public static async IAsyncEnumerable<T> ReadAsync(Stream stream, string location, FileScan<T> scan, [EnumeratorCancellation] CancellationToken ct)
    {
        using var reader = await ReadFailure.Guard(() => ArrowConvert.OpenIpcAsync(stream, ct), location, null).ConfigureAwait(false);
        while (await ReadFailure.Guard(() => reader.ReadNextRecordBatchAsync(ct).AsTask(), location, null).ConfigureAwait(false) is { } batch)
        {
            T[] rows;
            using (batch) rows = BatchReader<T>.Read(batch, scan.Map, scan.Partitions, location);
            scan.Metrics.RowGroups++;
            scan.Metrics.RowsRead += rows.Length;
            foreach (var row in rows)
            {
                if (scan.Residual is not null && !scan.Residual(row)) continue;
                scan.Metrics.RowsReturned++;
                yield return row;
            }
        }
    }
}
