using Parquet;
using Parquet.Schema;

namespace SolTechnology.DataLake.UnitTests;

// Filters must never skip a row group or file that has a matching row. Each case here once did.
public class PruningRegressionTests
{
    private TempDataset _lake = null!;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    public sealed class Priced
    {
        public decimal Amount { get; set; }
    }

    [Test]
    public async Task Decimal_statistics_stored_as_integers_are_scaled()
    {
        // DECIMAL(9, 2) is stored as INT32, like Spark stores decimals up to 18 digits; statistics come back unscaled.
        var amount = new DecimalDataField("Amount", 9, 2);
        await using (var stream = File.Create(_lake.PathOf("prices.parquet")))
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(amount), stream))
        {
            foreach (var group in new[] { new[] { 100.00m, 150.00m, 200.00m }, [300.00m, 400.00m] })
            {
                using var rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync(amount, (ReadOnlyMemory<decimal>)group);
            }
        }
        var metrics = new ScanMetrics();

        var rows = await Dataset.Open<Priced>(_lake.Root).Where(p => p.Amount == 150.00m).ReadAsync(metrics).ToListAsync();

        rows.Should().ContainSingle().Which.Amount.Should().Be(150.00m);
        metrics.RowGroupsSkippedByStatistics.Should().Be(1, "the second row group (300-400) still can't match");
    }

    public sealed class Measured
    {
        public double D { get; set; }
        public float F { get; set; }
    }

    private async Task WriteMeasurements(double[] d, float[] f) =>
        await _lake.WriteAsync("m.parquet", [new DataField<double>("D"), new DataField<float>("F")], [d, f]);

    [Test]
    public async Task NaN_rows_are_not_skipped_by_not_equal_or_negation()
    {
        await WriteMeasurements([2.0, double.NaN], [1f, 1f]);
        var dataset = Dataset.Open<Measured>(_lake.Root);

        (await dataset.Where(m => m.D != 2.0).ReadAsync().CountAsync()).Should().Be(1);
        (await dataset.Where(m => !(m.D < 5)).ReadAsync().CountAsync()).Should().Be(1);
    }

    [Test]
    public async Task Narrowing_casts_and_mixed_numbers_do_not_skip_matching_rows()
    {
        await WriteMeasurements([2.5, 2.5], [0.1f, 0.1f]);
        var dataset = Dataset.Open<Measured>(_lake.Root);

        (await dataset.Where(m => (int)m.D == 2).ReadAsync().CountAsync()).Should().Be(2);
        (await dataset.Where(m => (double)m.F > 0.1).ReadAsync().CountAsync()).Should().Be(2);
        (await dataset.Where(m => (double)m.F < 1e30).ReadAsync().CountAsync()).Should().Be(2);
    }
}
