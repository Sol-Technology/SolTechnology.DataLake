using System.Globalization;

namespace IcebergSpike;

public class PruningTests
{
    private static readonly DateTimeOffset Oct2 = new(2026, 10, 2, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task One_country_one_day_reads_one_file_of_nine()
    {
        var table = Oracle.Table("orders_by_day");
        var metrics = new ScanMetrics();
        Predicate[] filter = [Predicate.Eq("country", "PL"), new("created_at", Oct2, Oct2.AddDays(1).AddTicks(-10))];

        var rows = await table.Scan(filter: filter, metrics: metrics);
        TestContext.Out.WriteLine(metrics);

        // 9 appends = 9 manifests; partition summaries in the manifest list already rule out 8 of them.
        metrics.Manifests.Should().Be(9);
        metrics.ManifestsSkipped.Should().Be(8);
        metrics.FilesRead.Should().Be(1);
        var expected = Oracle.Rows("orders_by_day")
            .Where(r => r.GetProperty("country").GetString() == "PL"
                        && DateTimeOffset.Parse(r.GetProperty("created_at").GetString()!, CultureInfo.InvariantCulture).Date == Oct2.Date)
            .ToList();
        Oracle.AssertSame(rows.OrderBy(r => (long)r["id"]!).ToList(), expected);
    }

    [Test]
    public async Task Filter_on_a_non_partition_column_skips_files_by_column_bounds()
    {
        var table = Oracle.Table("orders_by_day");
        var metrics = new ScanMetrics();

        var rows = await table.Scan(filter: [new Predicate("id", 1L, 4L)], metrics: metrics);
        TestContext.Out.WriteLine(metrics);

        metrics.SkippedByStats.Should().Be(8);
        rows.Select(r => (long)r["id"]!).Should().BeEquivalentTo([1L, 2L, 3L, 4L]);
    }

    [Test]
    public async Task Equality_on_bucket_column_reads_one_bucket()
    {
        var table = Oracle.Table("orders_bucket");
        var metrics = new ScanMetrics();

        var rows = await table.Scan(filter: [Predicate.Eq("id", 7L)], metrics: metrics);
        TestContext.Out.WriteLine(metrics);

        metrics.SkippedByPartition.Should().Be(3);
        rows.Should().ContainSingle().Which["id"].Should().Be(7L);
    }
}
