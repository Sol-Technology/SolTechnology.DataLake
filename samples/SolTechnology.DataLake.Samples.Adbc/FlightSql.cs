using Apache.Arrow.Adbc.Drivers.FlightSql;
using SolTechnology.DataLake.Arrow;

namespace SolTechnology.DataLake.Samples.Adbc;

/// <summary>
/// Any Arrow Flight SQL server (Dremio, InfluxDB 3, DuckDB through sqlflite, Doris, StarRocks): FLIGHT_SQL_URI
/// (http://localhost:31337) and optionally FLIGHT_SQL_AUTHORIZATION (the Authorization header, e.g. "Bearer ...").
/// </summary>
public static class FlightSql
{
    public sealed class Row
    {
        public long Id { get; set; }
        public string? Name { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    public static async Task RunAsync()
    {
        var parameters = new Dictionary<string, string>
        {
            [FlightSqlParameters.ServerAddress] = Environment.GetEnvironmentVariable("FLIGHT_SQL_URI") ?? "http://localhost:31337",
        };
        if (Environment.GetEnvironmentVariable("FLIGHT_SQL_AUTHORIZATION") is { } authorization)
            parameters[FlightSqlParameters.Authorization] = authorization;

        using var database = new FlightSqlDriver().Open(parameters);
        using var connection = database.Connect(parameters);
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1::BIGINT AS id, 'first' AS name, now()::TIMESTAMPTZ AS created_at";

        var result = await statement.ExecuteQueryAsync();
        await foreach (var row in ArrowConvert.DeserializeAsync<Row>(result.Stream!))
            Console.WriteLine($"{row.Id} {row.Name} {row.CreatedAt:O}");
    }
}
