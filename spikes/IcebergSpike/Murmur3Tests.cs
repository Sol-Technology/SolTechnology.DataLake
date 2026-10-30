using System.Globalization;
using System.Text;

namespace IcebergSpike;

public class Murmur3Tests
{
    // Iceberg spec, Appendix B.
    [Test]
    public void Spec_test_values()
    {
        Murmur3.HashValue(34).Should().Be(2017239379);
        Murmur3.HashValue(34L).Should().Be(2017239379);
        Murmur3.HashValue(14.20m).Should().Be(-500754589);
        Murmur3.HashValue(new DateOnly(2017, 11, 16)).Should().Be(-653330422);
        Murmur3.HashValue(new TimeOnly(22, 31, 8)).Should().Be(-662762989);
        Murmur3.HashValue(new DateTime(2017, 11, 16, 22, 31, 8)).Should().Be(-2047944441);
        Murmur3.HashValue(new DateTime(2017, 11, 16, 22, 31, 8).AddTicks(10)).Should().Be(-1207196810);
        Murmur3.HashValue(new DateTimeOffset(2017, 11, 16, 14, 31, 8, TimeSpan.FromHours(-8))).Should().Be(-2047944441);
        Murmur3.HashValue("iceberg").Should().Be(1210000089);
        Murmur3.HashValue(Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7")).Should().Be(1488055340);
        Murmur3.HashValue(new byte[] { 0, 1, 2, 3 }).Should().Be(-188683207);
    }

    // Every row PyIceberg put into id_bucket=N must hash to N with our Murmur3.
    [Test]
    public async Task Buckets_match_files_written_by_PyIceberg()
    {
        var table = Oracle.Table("orders_bucket");
        var files = await table.PlanFiles(table.Metadata.Snapshot(), [], new ScanMetrics());
        files.Should().HaveCount(4);

        foreach (var file in files)
        {
            var bucket = Convert.ToInt64(((IDictionary<string, object?>)file.Partition!)["id_bucket"], CultureInfo.InvariantCulture);
            var ids = new List<long>();
            await foreach (var row in table.ReadFile(file, table.Metadata.Schema(table.Metadata.CurrentSchemaId))) ids.Add((long)row["id"]!);

            ids.Should().NotBeEmpty();
            ids.Should().AllSatisfy(id => Transforms.Apply("bucket[4]", id).Should().Be(bucket, $"id {id} is in id_bucket={bucket}"));
        }
    }
}
