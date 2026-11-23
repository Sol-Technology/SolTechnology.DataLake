using Azure.Storage.Blobs;
using SolTechnology.DataLake.Azure;

namespace SolTechnology.DataLake.UnitTests;

public class AzureBlobStorageTests
{
    private readonly AzureBlobStorage _storage = new(new BlobServiceClient(new Uri("https://shop.blob.core.windows.net")));

    [TestCase("abfss://lake@shop.dfs.core.windows.net/orders/", true)]
    [TestCase("abfs://lake@shop.dfs.core.windows.net/orders/", true)]
    [TestCase("wasbs://lake@shop.blob.core.windows.net/orders/", true)]
    [TestCase("https://shop.blob.core.windows.net/lake/orders/", true)]
    [TestCase("abfss://Sales@onelake.dfs.fabric.microsoft.com/Orders.Lakehouse/Files/orders/", true)]
    [TestCase("az://lake/orders/", true)]
    [TestCase("https://shop.z12.blob.storage.azure.net/lake/orders/", true)]
    [TestCase("abfss://lake@shop.z12.dfs.storage.azure.net/orders/", true)]
    [TestCase("s3://lake/orders/", false)]
    [TestCase("https://example.com/lake/orders/", false)]
    [TestCase("/data/orders", false)]
    public void Handles_Azure_locations(string location, bool handled) => _storage.CanHandle(location).Should().Be(handled);

    [Test]
    public async Task Location_in_another_account_is_an_error()
    {
        var list = () => _storage.ListAsync("abfss://lake@other.dfs.core.windows.net/orders/").ToListAsync().AsTask();

        (await list.Should().ThrowAsync<ArgumentException>()).WithMessage("*in account 'other'*points to 'shop.blob.core.windows.net'*");
    }
}
