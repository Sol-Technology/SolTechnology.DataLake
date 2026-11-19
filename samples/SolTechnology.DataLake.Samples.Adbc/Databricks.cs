using Apache.Arrow.Adbc.Drivers.Databricks;
using SolTechnology.DataLake.Arrow;

namespace SolTechnology.DataLake.Samples.Adbc;

/// <summary>
/// A Databricks SQL warehouse: DATABRICKS_HOST (adb-123.azuredatabricks.net), DATABRICKS_HTTP_PATH (/sql/1.0/warehouses/abc)
/// and DATABRICKS_TOKEN (a personal access token).
/// </summary>
public static class Databricks
{
    public static async Task RunAsync()
    {
        using var database = new DatabricksDriver().Open(new Dictionary<string, string>
        {
            [DatabricksParameters.HostName] = Env("DATABRICKS_HOST"),
            [DatabricksParameters.Path] = Env("DATABRICKS_HTTP_PATH"),
            [DatabricksParameters.Token] = Env("DATABRICKS_TOKEN"),
        });
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips LIMIT 1000";

        var result = await statement.ExecuteQueryAsync();
        await foreach (var trip in ArrowConvert.DeserializeAsync<Trip>(result.Stream!))
            Console.WriteLine($"{trip.PickedUp} → {trip.DroppedOff}: {trip.Distance} mi, {trip.Fare:C}");
    }

    private static string Env(string name) =>
        Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Set {name}.");
}
