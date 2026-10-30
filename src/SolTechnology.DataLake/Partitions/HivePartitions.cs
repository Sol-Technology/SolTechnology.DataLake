namespace SolTechnology.DataLake.Partitions;

/// <summary>
/// Hive-style partitions from directory names: <c>orders/date=2026-10-29/country=PL/part-0.parquet</c> gives
/// <c>date = "2026-10-29"</c>, <c>country = "PL"</c>. Values are URL-decoded; Hive's default partition is null.
/// </summary>
internal static class HivePartitions
{
    public const string DefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    public static IReadOnlyDictionary<string, string?> Parse(string relativePath)
    {
        var result = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var segments = relativePath.Split(['/', '\\'], StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments[..^1])
        {
            var eq = segment.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var value = Uri.UnescapeDataString(segment[(eq + 1)..]);
            result[Uri.UnescapeDataString(segment[..eq])] = value == DefaultPartition ? null : value;
        }
        return result;
    }

    /// <summary>Spark, Hive and Hadoop write markers and temporary files starting with <c>_</c> or <c>.</c> (_SUCCESS, .crc, _temporary).</summary>
    public static bool IsHidden(string relativePath) =>
        relativePath.Split(['/', '\\'], StringSplitOptions.RemoveEmptyEntries)
            .Any(s => (s.StartsWith('_') || s.StartsWith('.')) && !s.Contains('=', StringComparison.Ordinal));
}
