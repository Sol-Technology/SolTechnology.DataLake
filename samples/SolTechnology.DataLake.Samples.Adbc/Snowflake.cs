using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Interop.Snowflake;
using SolTechnology.DataLake.Arrow;

namespace SolTechnology.DataLake.Samples.Adbc;

/// <summary>
/// Snowflake through the ADBC driver (native, shipped in the package): SNOWFLAKE_URI as
/// <c>user:password@account/SNOWFLAKE_SAMPLE_DATA/TPCH_SF1?warehouse=COMPUTE_WH</c>.
/// </summary>
public static class Snowflake
{
    public sealed class Order
    {
        public long O_OrderKey { get; set; }
        public string O_OrderStatus { get; set; } = "";
        public decimal O_TotalPrice { get; set; }
        public DateOnly O_OrderDate { get; set; }
    }

    public static async Task RunAsync()
    {
        using var database = SnowflakeDriverLoader.LoadDriver().Open(new Dictionary<string, string>
        {
            [AdbcOptions.Uri] = Environment.GetEnvironmentVariable("SNOWFLAKE_URI") ?? throw new InvalidOperationException("Set SNOWFLAKE_URI."),
        });
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT O_ORDERKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS LIMIT 1000";

        // Snowflake returns upper-case column names; matching is case-insensitive.
        var result = await statement.ExecuteQueryAsync();
        await foreach (var order in ArrowConvert.DeserializeAsync<Order>(result.Stream!))
            Console.WriteLine($"{order.O_OrderKey} {order.O_OrderDate} {order.O_OrderStatus} {order.O_TotalPrice}");
    }
}
