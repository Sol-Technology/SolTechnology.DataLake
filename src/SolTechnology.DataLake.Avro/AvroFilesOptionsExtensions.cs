using SolTechnology.DataLake.Avro;

namespace SolTechnology.DataLake;

/// <summary>Avro container files in folder datasets.</summary>
public static class AvroFilesOptionsExtensions
{
    /// <summary>
    /// Reads Avro container files (<c>.avro</c>) next to Parquet, with the same mapping. They have no statistics, so only
    /// partitions skip them. Writes stay Parquet. Avro is decoded by AvroConvert, which has its own licence.
    /// </summary>
    public static FilesOptions Avro(this FilesOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Add(AvroFileFormat.Instance);
    }
}
