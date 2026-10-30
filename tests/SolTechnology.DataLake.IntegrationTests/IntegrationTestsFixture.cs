using System.Text.Json;
using SolTechnology.Avro;
using SolTechnology.DataLake.IntegrationTests.Containers;

namespace SolTechnology.DataLake.IntegrationTests;

/// <summary>
/// One setup for the whole run: Silo (S3), Lakekeeper (REST catalog) and Trino in Docker, then the test tables,
/// written by PyIceberg and Trino, with the rows PyIceberg reads back as the expected result.
/// </summary>
[SetUpFixture]
public static class IntegrationTestsFixture
{
    public static TestContainersContext Context { get; private set; } = null!;
    public static SiloFixture Silo { get; private set; } = null!;
    public static LakekeeperFixture Lakekeeper { get; private set; } = null!;
    public static TrinoFixture Trino { get; private set; } = null!;
    public static PyIcebergFixture PyIceberg { get; private set; } = null!;

    /// <summary><c>shop.orders</c>: partitioned by country and day, written by PyIceberg.</summary>
    public static JsonElement Orders { get; private set; }

    /// <summary><c>shop.orders_mor</c>: a copy changed by Trino DELETE / UPDATE, so it has position delete files.</summary>
    public static JsonElement OrdersWithDeletes { get; private set; }

    [OneTimeSetUp]
    public static async Task SetUp()
    {
        AvroConvert.License = AvroLicense.NonCommercial;

        Context = new TestContainersContext();
        await Context.InitializeAsync();
        Silo = new SiloFixture(Context);
        Lakekeeper = new LakekeeperFixture(Context);
        Trino = new TrinoFixture(Context);
        PyIceberg = new PyIcebergFixture(Context);

        // Trino and the PyIceberg image take the longest and need nothing else to start.
        var trino = Trino.InitializeAsync();
        var pyIceberg = PyIceberg.InitializeAsync();
        await Silo.InitializeAsync();
        await Lakekeeper.InitializeAsync();
        await pyIceberg;

        Orders = Json(await PyIceberg.RunAsync("orders.json", ["python", "/scripts/seed.py"]));

        await trino;
        await Trino.ExecuteAsync("DROP TABLE IF EXISTS orders_mor");
        await Trino.ExecuteAsync("CREATE TABLE orders_mor WITH (partitioning = ARRAY['country'], format_version = 2) AS SELECT * FROM orders");
        await Trino.ExecuteAsync("DELETE FROM orders_mor WHERE id IN (1, 12)");
        await Trino.ExecuteAsync("UPDATE orders_mor SET amount = 99.99 WHERE id = 4");
        OrdersWithDeletes = Json(await PyIceberg.RunAsync("orders_mor.json", ["python", "/scripts/oracle.py", "orders_mor"]));
    }

    [OneTimeTearDown]
    public static async Task TearDown()
    {
        if (Trino is not null) await Trino.DisposeAsync();
        if (Lakekeeper is not null) await Lakekeeper.DisposeAsync();
        if (Silo is not null) await Silo.DisposeAsync();
        if (Context is not null) await Context.DisposeAsync();
    }

    private static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();
}
