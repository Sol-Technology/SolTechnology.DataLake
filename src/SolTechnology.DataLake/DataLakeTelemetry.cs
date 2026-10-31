using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace SolTechnology.DataLake;

/// <summary>
/// OpenTelemetry-compatible tracing and metrics. Subscribe with
/// <c>.AddSource(DataLakeTelemetry.Name)</c> and <c>.AddMeter(DataLakeTelemetry.Name)</c>.
/// </summary>
public static class DataLakeTelemetry
{
    /// <summary>Name of the <see cref="ActivitySource"/> and the <see cref="Meter"/>.</summary>
    public const string Name = "SolTechnology.DataLake";

    internal static readonly ActivitySource Source = new(Name, typeof(DataLakeTelemetry).Assembly.GetName().Version?.ToString());

    private static readonly Meter Meter = new(Name, typeof(DataLakeTelemetry).Assembly.GetName().Version?.ToString());

    private static readonly Counter<long> Files = Meter.CreateCounter<long>("datalake.scan.files", "{file}", "Data files found by scans.");
    private static readonly Counter<long> FilesSkipped = Meter.CreateCounter<long>("datalake.scan.files.skipped", "{file}", "Files skipped by partition values.");
    private static readonly Counter<long> RowGroupsSkipped = Meter.CreateCounter<long>("datalake.scan.row_groups.skipped", "{row_group}", "Row groups skipped by statistics.");
    private static readonly Counter<long> RowsRead = Meter.CreateCounter<long>("datalake.scan.rows.read", "{row}", "Rows decoded from files.");
    private static readonly Counter<long> BytesRead = Meter.CreateCounter<long>("datalake.scan.bytes.read", "By", "Bytes fetched from storage.");

    internal static void Record(ScanMetrics metrics, Activity? activity)
    {
        Files.Add(metrics.Files);
        FilesSkipped.Add(metrics.FilesSkippedByPartition);
        RowGroupsSkipped.Add(metrics.RowGroupsSkippedByStatistics);
        RowsRead.Add(metrics.RowsRead);
        BytesRead.Add(metrics.BytesRead);

        activity?.SetTag("datalake.files", metrics.Files);
        activity?.SetTag("datalake.files.skipped", metrics.FilesSkippedByPartition);
        activity?.SetTag("datalake.row_groups", metrics.RowGroups);
        activity?.SetTag("datalake.row_groups.skipped", metrics.RowGroupsSkippedByStatistics);
        activity?.SetTag("datalake.rows.read", metrics.RowsRead);
        activity?.SetTag("datalake.rows.returned", metrics.RowsReturned);
        activity?.SetTag("datalake.bytes.read", metrics.BytesRead);
    }
}
