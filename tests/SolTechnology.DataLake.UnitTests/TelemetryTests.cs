using System.Diagnostics;
using System.Diagnostics.Metrics;
using Parquet.Schema;

namespace SolTechnology.DataLake.UnitTests;

[NonParallelizable]
public class TelemetryTests
{
    public sealed class Row
    {
        public long Id { get; set; }
        public string Country { get; set; } = "";
    }

    [Test]
    public async Task A_read_is_an_activity_and_adds_to_the_counters()
    {
        using var lake = new TempDataset();
        foreach (var country in new[] { "DE", "PL" })
            await lake.WriteAsync($"country={country}/part-0.parquet", [new DataField<long>("Id")], [new long[] { 1, 2, 3 }]);

        var activities = new List<Activity>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == DataLakeTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activities.Add,
        };
        ActivitySource.AddActivityListener(activityListener);

        var counters = new Dictionary<string, long>();
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, l) => { if (instrument.Meter.Name == DataLakeTelemetry.Name) l.EnableMeasurementEvents(instrument); };
        meterListener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            lock (counters) counters[instrument.Name] = counters.GetValueOrDefault(instrument.Name) + value;
        });
        meterListener.Start();

        var rows = await DataLake.Open<Row>(lake.Root).Where(r => r.Country == "PL").ReadAsync().ToListAsync();

        rows.Should().HaveCount(3);
        var scan = activities.Should().ContainSingle(a => a.OperationName == "datalake.scan").Subject;
        scan.Status.Should().Be(ActivityStatusCode.Ok);
        scan.GetTagItem("datalake.location").Should().Be(lake.Root);
        scan.GetTagItem("datalake.files").Should().Be(2);
        scan.GetTagItem("datalake.files.skipped").Should().Be(1);
        scan.GetTagItem("datalake.rows.returned").Should().Be(3L);
        counters["datalake.scan.files"].Should().Be(2);
        counters["datalake.scan.files.skipped"].Should().Be(1);
        counters["datalake.scan.rows.read"].Should().Be(3);
        counters["datalake.scan.bytes.read"].Should().BePositive();
    }
}
