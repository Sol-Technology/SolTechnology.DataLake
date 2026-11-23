using SolTechnology.DataLake.Arrow;

namespace SolTechnology.DataLake;

/// <summary>Arrow IPC files in folder datasets.</summary>
public static class ArrowFilesOptionsExtensions
{
    /// <summary>
    /// Reads Arrow IPC files and streams (<c>.arrow</c>, <c>.feather</c>, <c>.ipc</c>, <c>.arrows</c>) next to Parquet, with
    /// the same mapping. They have no statistics, so only partitions skip them. Writes stay Parquet.
    /// </summary>
    public static FilesOptions Arrow(this FilesOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Add(ArrowFileFormat.Instance);
    }
}
