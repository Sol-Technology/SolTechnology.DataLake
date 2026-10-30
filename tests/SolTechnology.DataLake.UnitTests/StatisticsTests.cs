using System.Runtime.Serialization;
using Parquet.Schema;
using SolTechnology.Time;

namespace SolTechnology.DataLake.UnitTests;

// Row group statistics come from Parquet.Net as physical values; pruning must decode them right or not prune at all.
public class StatisticsTests
{
    private TempDataset _lake = null!;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    public sealed class Event
    {
        public DateTimeZone At { get; set; }
    }

    private static readonly DateTime Day1 = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    private async Task WriteTwoDays(DateTimeTimeUnit unit) =>
        await _lake.WriteAsync("x.parquet", [new DateTimeDataField("At", Parquet.Schema.DateTimeFormat.Timestamp, isAdjustedToUTC: true, unit: unit)],
            [Enumerable.Range(0, 24).Select(h => Day1.AddHours(h)).ToArray()],
            [Enumerable.Range(0, 24).Select(h => Day1.AddDays(1).AddHours(h)).ToArray()]);

    [Test]
    public async Task Parquet_Net_micros_timestamps_have_wrong_statistics_so_they_do_not_prune()
    {
        await WriteTwoDays(DateTimeTimeUnit.Micros);
        var metrics = new ScanMetrics();
        var from = DateTimeZone.FromUtc(Day1.AddDays(1), TimeZoneInfo.Utc);

        var rows = await DataLake.Open<Event>(_lake.Root).Where(e => e.At >= from).ReadAsync(metrics).ToListAsync();

        rows.Should().HaveCount(24, "trusting the statistics would skip both row groups");
        metrics.RowGroupsSkippedByStatistics.Should().Be(0);
    }

    [Test]
    public async Task Parquet_Net_millis_timestamps_prune()
    {
        await WriteTwoDays(DateTimeTimeUnit.Millis);
        var metrics = new ScanMetrics();
        var from = DateTimeZone.FromUtc(Day1.AddDays(1), TimeZoneInfo.Utc);

        var rows = await DataLake.Open<Event>(_lake.Root).Where(e => e.At >= from).ReadAsync(metrics).ToListAsync();

        rows.Should().HaveCount(24);
        metrics.RowGroupsSkippedByStatistics.Should().Be(1);
    }

    public sealed class Priced
    {
        public decimal? Amount { get; set; }
        public DateOnly Day { get; set; }
    }

    [Test]
    public async Task Decimal_and_date_statistics_prune()
    {
        var days = Enumerable.Range(0, 3).Select(d => new DateOnly(2026, 10, 1 + d)).ToArray();
        await _lake.WriteAsync("x.parquet", [new DataField<decimal?>("Amount"), new DataField<DateOnly>("Day")],
            days.Select((day, i) => new Array[] { new decimal?[] { 10m * i, 10m * i + 5 }, new[] { day, day } }).ToArray());
        var byAmount = new ScanMetrics();
        var byDay = new ScanMetrics();

        var expensive = await DataLake.Open<Priced>(_lake.Root).Where(p => p.Amount > 16m).ReadAsync(byAmount).ToListAsync();
        var late = await DataLake.Open<Priced>(_lake.Root).Where(p => p.Day >= days[2]).ReadAsync(byDay).ToListAsync();

        byAmount.RowGroupsSkippedByStatistics.Should().Be(2);
        expensive.Select(p => p.Amount).Should().Equal(20m, 25m);
        byDay.RowGroupsSkippedByStatistics.Should().Be(2);
        late.Should().HaveCount(2);
    }

    public sealed class IcebergOrder
    {
        [DataMember(Name = "id")] public long Id { get; set; }
        [DataMember(Name = "created_at")] public DateTimeZone CreatedAt { get; set; }
    }

    [Test]
    public async Task Micros_timestamps_from_PyIceberg_prune()
    {
        var root = Path.Combine(FixturesRoot(), "warehouse", "shop", "orders_by_day", "data");
        var metrics = new ScanMetrics();
        var from = DateTimeZone.FromUtc(new DateTime(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Utc);

        var rows = await DataLake.Open<IcebergOrder>(root).Where(o => o.CreatedAt >= from).ReadAsync(metrics).ToListAsync();

        rows.Should().HaveCount(12).And.OnlyContain(o => o.CreatedAt >= from);
        metrics.RowGroupsSkippedByStatistics.Should().Be(6, "9 files, one per country and day; 6 are before 3 October");
    }

    private static string FixturesRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "spikes", "fixtures");
            if (Directory.Exists(candidate)) return candidate;
        }
        throw new DirectoryNotFoundException("spikes/fixtures");
    }
}
