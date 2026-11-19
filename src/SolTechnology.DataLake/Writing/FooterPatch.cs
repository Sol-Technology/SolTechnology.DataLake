using System.Buffers.Binary;
using static SolTechnology.DataLake.Writing.ThriftCompact;

namespace SolTechnology.DataLake.Writing;

/// <summary>
/// Parquet.Net 6.1 writes min/max of MICROS timestamps in milliseconds, in the footer and in every data page header, so
/// engines that prune by statistics (DuckDB, Spark, Trino) would skip row groups and pages that match. After the file is
/// written:
/// <list type="bullet">
/// <item>the footer gets the min/max the writer saw, in the column's unit, and <c>created_by</c> naming DataLake, so readers
/// that distrust Parquet.Net's timestamp statistics (DataLake itself) trust these;</item>
/// <item>each data page header of those columns gets the column chunk's min/max, overwritten in place (8 bytes for 8
/// bytes, so no offset moves). Every page lies within its chunk, so the bounds are looser but never wrong.</item>
/// </list>
/// </summary>
internal static class FooterPatch
{
    private static readonly byte[] Magic = "PAR1"u8.ToArray();

    // FileMetaData.row_groups, RowGroup.columns, ColumnChunk.meta_data, ColumnMetaData.statistics,
    // Statistics.max / min (deprecated) / max_value / min_value, ColumnChunk.column_index_offset / length.
    private const short CreatedBy = 6, RowGroups = 4, Columns = 1, MetaData = 3, StatisticsField = 12, Max = 1, Min = 2, MaxValue = 5, MinValue = 6, ColumnIndexOffset = 6, ColumnIndexLength = 7;

    // ColumnMetaData.total_compressed_size / data_page_offset / dictionary_page_offset; PageHeader.compressed_page_size /
    // data_page_header / data_page_header_v2; DataPageHeader.statistics and DataPageHeaderV2.statistics.
    private const short TotalCompressedSize = 7, DataPageOffset = 9, DictionaryPageOffset = 11, CompressedPageSize = 3, DataPage = 5, DataPageV2 = 8, PageStatistics = 5, PageStatisticsV2 = 8;

    /// <param name="file">The whole Parquet file, readable, writable and seekable.</param>
    /// <param name="minMax">Per row group: leaf column index → min and max as the stored INT64.</param>
    /// <param name="ct">Cancellation.</param>
    public static async Task FixAsync(Stream file, IReadOnlyList<Dictionary<int, (long Min, long Max)>> minMax, CancellationToken ct)
    {
        var tail = new byte[8];
        file.Seek(-8, SeekOrigin.End);
        await file.ReadExactlyAsync(tail, ct).ConfigureAwait(false);
        if (!tail.AsSpan(4).SequenceEqual(Magic)) throw new InvalidDataException("Not a Parquet file: no PAR1 at the end.");
        var length = BinaryPrimitives.ReadInt32LittleEndian(tail);
        var footerStart = file.Length - 8 - length;

        var bytes = new byte[length];
        file.Seek(footerStart, SeekOrigin.Begin);
        await file.ReadExactlyAsync(bytes, ct).ConfigureAwait(false);
        var footer = Read(bytes);

        footer.Set(CreatedBy, Binary, System.Text.Encoding.UTF8.GetBytes(WriterName));
        var groups = (ThriftList)footer.Find(RowGroups)!.Value;
        for (var g = 0; g < minMax.Count; g++)
        {
            var columns = (ThriftList)((ThriftStruct)groups.Items[g]).Find(Columns)!.Value;
            foreach (var (column, (min, max)) in minMax[g])
            {
                var chunk = (ThriftStruct)columns.Items[column];
                var meta = (ThriftStruct)chunk.Find(MetaData)!.Value;
                var statistics = meta.Find(StatisticsField)?.Value as ThriftStruct ?? new ThriftStruct();
                statistics.Set(MaxValue, Binary, Int64(max));
                statistics.Set(MinValue, Binary, Int64(min));
                if (statistics.Find(Max) is not null) statistics.Set(Max, Binary, Int64(max));
                if (statistics.Find(Min) is not null) statistics.Set(Min, Binary, Int64(min));
                meta.Set(StatisticsField, Struct, statistics);
                // A page index would carry the same wrong values; without it readers fall back to the fixed statistics.
                chunk.Fields.RemoveAll(f => f.Id is ColumnIndexOffset or ColumnIndexLength);
                await FixPagesAsync(file, meta, min, max, ct).ConfigureAwait(false);
            }
        }

        var fixedFooter = Write(footer);
        file.SetLength(footerStart);
        file.Seek(footerStart, SeekOrigin.Begin);
        await file.WriteAsync(fixedFooter, ct).ConfigureAwait(false);
        BinaryPrimitives.WriteInt32LittleEndian(tail, fixedFooter.Length);
        await file.WriteAsync(tail, ct).ConfigureAwait(false);
        await file.FlushAsync(ct).ConfigureAwait(false);
    }

    private static async Task FixPagesAsync(Stream file, ThriftStruct meta, long min, long max, CancellationToken ct)
    {
        var position = (long)(meta.Find(DictionaryPageOffset)?.Value ?? meta.Find(DataPageOffset)!.Value);
        var end = position + (long)meta.Find(TotalCompressedSize)!.Value;
        var buffer = new byte[4096];
        while (position < end)
        {
            file.Seek(position, SeekOrigin.Begin);
            var read = await file.ReadAtLeastAsync(buffer, buffer.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
            var header = Read(buffer.AsSpan(0, read), out var headerLength);
            var statistics = (header.Find(DataPage)?.Value as ThriftStruct)?.Find(PageStatistics)?.Value as ThriftStruct
                             ?? (header.Find(DataPageV2)?.Value as ThriftStruct)?.Find(PageStatisticsV2)?.Value as ThriftStruct;
            if (statistics is not null && Replace(statistics, min, max))
            {
                var fixedHeader = Write(header);
                if (fixedHeader.Length != headerLength)
                    throw new InvalidOperationException($"A rewritten page header is {fixedHeader.Length} bytes instead of {headerLength}.");
                file.Seek(position, SeekOrigin.Begin);
                await file.WriteAsync(fixedHeader, ct).ConfigureAwait(false);
            }
            position += headerLength + (long)header.Find(CompressedPageSize)!.Value;
        }
    }

    // Only values already there, 8 bytes each: same encoded length.
    private static bool Replace(ThriftStruct statistics, long min, long max)
    {
        var changed = false;
        foreach (var (id, value) in new[] { (Max, max), (Min, min), (MaxValue, max), (MinValue, min) })
        {
            if (statistics.Find(id)?.Value is not byte[] { Length: 8 }) continue;
            statistics.Set(id, Binary, Int64(value));
            changed = true;
        }
        return changed;
    }

    /// <summary><c>created_by</c> in the form the spec suggests: application, version, and the library under it.</summary>
    public static readonly string WriterName =
        $"SolTechnology.DataLake version {Version(typeof(FooterPatch))} (Parquet.Net {Version(typeof(Parquet.ParquetWriter))})";

    private static string Version(Type type)
    {
        var informational = type.Assembly.GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .OfType<System.Reflection.AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion;
        return informational?.Split('+')[0] ?? type.Assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private static byte[] Int64(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }
}
