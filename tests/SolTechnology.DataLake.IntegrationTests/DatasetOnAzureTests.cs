using Azure.Storage.Blobs;
using Parquet;
using Parquet.Schema;
using SolTechnology.DataLake.Azure;
using static SolTechnology.DataLake.IntegrationTests.IntegrationTestsFixture;

namespace SolTechnology.DataLake.IntegrationTests;

// A hive-partitioned Parquet dataset in Azure Blob Storage (Azurite), read through AzureBlobStorage with ranged downloads.
public class DatasetOnAzureTests
{
    public sealed class Order
    {
        public long Id { get; set; }
        public decimal? Amount { get; set; }
        public DateOnly Date { get; set; }
        public string Country { get; set; } = "";
    }

    private BlobServiceClient _blobs = null!;
    private string _location = null!;

    [SetUp]
    public async Task UploadDataset()
    {
        _blobs = Azurite.CreateClient();
        var container = _blobs.GetBlobContainerClient("lake");
        await container.CreateIfNotExistsAsync();
        var prefix = $"datasets/{Guid.NewGuid():N}/orders";
        _location = $"az://lake/{prefix}";

        var id = 0L;
        foreach (var day in new[] { "2026-10-01", "2026-10-02", "2026-10-03" })
        foreach (var country in new[] { "DE", "PL" })
        {
            var ids = Enumerable.Range(0, 1000).Select(_ => id++).ToArray();
            await container.UploadBlobAsync($"{prefix}/date={day}/country={country}/part-00000.snappy.parquet", new BinaryData(await ParquetBytes(ids)));
        }
        await container.UploadBlobAsync($"{prefix}/_SUCCESS", new BinaryData([]));
        // Same name prefix, other folder: must not be listed.
        await container.UploadBlobAsync($"{prefix}_old/part-00000.parquet", new BinaryData(await ParquetBytes([999_999])));
    }

    [Test]
    public async Task Reads_the_whole_dataset()
    {
        var metrics = new ScanMetrics();

        var rows = await Open().ReadAsync(metrics).ToListAsync();

        rows.Should().HaveCount(6000);
        rows.Select(r => r.Id).Should().BeInAscendingOrder();
        rows[3500].Should().BeEquivalentTo(new Order { Id = 3500, Amount = 5250m, Date = new DateOnly(2026, 10, 2), Country = "PL" });
        metrics.Files.Should().Be(6);
    }

    [Test]
    public async Task Skipped_files_are_not_downloaded()
    {
        var all = new ScanMetrics();
        var filtered = new ScanMetrics();
        var day = new DateOnly(2026, 10, 3);

        await Open().ReadAsync(all).ToListAsync();
        var rows = await Open().Where(o => o.Date == day && o.Country == "DE").ReadAsync(filtered).ToListAsync();
        TestContext.Out.WriteLine($"all: {all}\nfiltered: {filtered}");

        rows.Should().HaveCount(1000).And.OnlyContain(r => r.Date == day && r.Country == "DE");
        filtered.FilesSkippedByPartition.Should().Be(5);
        filtered.BytesRead.Should().BeLessThan(all.BytesRead / 5);
    }

    [Test]
    public async Task A_single_blob_is_a_dataset_too()
    {
        var file = $"{_location}/date=2026-10-02/country=PL/part-00000.snappy.parquet";

        var rows = await DataLake.Open<IdOnly>(file, new DataLakeOptions { Storage = new AzureBlobStorage(_blobs) }).ReadAsync().ToListAsync();

        rows.Should().HaveCount(1000);
        rows[0].Id.Should().Be(3000);
    }

    public sealed class IdOnly
    {
        public long Id { get; set; }
    }

    private Dataset<Order> Open() => DataLake.Open<Order>(_location, new DataLakeOptions { Storage = new AzureBlobStorage(_blobs) });

    private static async Task<byte[]> ParquetBytes(long[] ids)
    {
        var id = new DataField<long>("Id");
        var amount = new DataField<decimal?>("Amount");
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(id, amount), stream))
        using (var group = writer.CreateRowGroup())
        {
            await group.WriteAsync(id, (ReadOnlyMemory<long>)ids);
            await group.WriteAsync(amount, (ReadOnlyMemory<decimal?>)ids.Select(i => (decimal?)(i * 1.5m)).ToArray());
        }
        return stream.ToArray();
    }
}
