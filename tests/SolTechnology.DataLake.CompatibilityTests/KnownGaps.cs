namespace SolTechnology.DataLake.CompatibilityTests;

/// <summary>
/// Corpus files DataLake can't read yet, with the reason. All of them fail inside Parquet.Net 6.1; DataLake reports
/// them as <see cref="InvalidDataException"/> with the file and column. A test fails when a file starts to work.
/// </summary>
internal static class KnownGaps
{
    private static readonly Dictionary<string, string> Gaps = new(StringComparer.Ordinal)
    {
        ["parquet-dotnet/all_var1.v2.parquet"] = "Parquet.Net: data page v2 in a list of structs is not implemented",
        ["parquet-dotnet/bigdecimal.parquet"] = "DECIMAL with more than 28 digits does not fit System.Decimal",
        ["parquet-testing/byte_stream_split_extended.gzip.parquet"] = "Parquet.Net: FLOAT16 (System.Half) is not supported",
        ["parquet-testing/datapage_v2.snappy.parquet"] = "Parquet.Net: a boolean column in a v2 data page throws DivideByZeroException",
        ["parquet-testing/datapage_v2_empty_datapage.snappy.parquet"] = "Parquet.Net: an empty v2 data page with Snappy fails to decompress",
        ["parquet-testing/dict-page-offset-zero.parquet"] = "Parquet.Net: the footer of this writer fails to parse",
        ["parquet-testing/float16_nonzeros_and_nans.parquet"] = "Parquet.Net: FLOAT16 (System.Half) is not supported",
        ["parquet-testing/float16_zeros_and_nans.parquet"] = "Parquet.Net: FLOAT16 (System.Half) is not supported",
        ["parquet-testing/floating_orders_nan_count.parquet"] = "Parquet.Net: FLOAT16 (System.Half) is not supported",
        ["parquet-testing/hadoop_lz4_compressed.parquet"] = "Parquet.Net: the Hadoop LZ4 framing of old parquet-mr is not supported",
        ["parquet-testing/hadoop_lz4_compressed_larger.parquet"] = "Parquet.Net: the Hadoop LZ4 framing of old parquet-mr is not supported",
        ["parquet-testing/int96_from_spark.parquet"] = "INT96 values outside the DateTime range, on purpose",
        ["parquet-testing/map_no_value.parquet"] = "Parquet.Net: a MAP without a value field is not supported",
        ["parquet-testing/repeated_no_annotation.parquet"] = "Parquet.Net: a repeated group with several fields and no LIST annotation is not supported",
        ["parquet-testing/rle_boolean_encoding.parquet"] = "Parquet.Net: the GZIP pages of this file fail to inflate",
    };

    // Reading these breaks later reads in the same process, so they are not read at all.
    private static readonly Dictionary<string, string> Unsafe = new(StringComparer.Ordinal)
    {
        ["parquet-testing/large_string_map.brotli.parquet"] = "Parquet.Net: after this file fails, strings of other files come back corrupted (shared buffer pool)",
    };

    public static string? For(string file) => Gaps.GetValueOrDefault(file);

    public static string? NotToRead(string file) => Unsafe.GetValueOrDefault(file);
}
