using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Interop.FlightSql;
using SolTechnology.DataLake.Arrow;
using SolTechnology.Time;

namespace SolTechnology.DataLake.Samples.FlightSql;

/// <summary>
/// Any Arrow Flight SQL server (Dremio, InfluxDB 3, DuckDB through GizmoSQL, Doris, StarRocks) through the ADBC Flight SQL
/// driver (native, shipped in the package for x64). FLIGHT_SQL_URI (grpc+tcp://localhost:31337, grpc+tls://... with TLS),
/// FLIGHT_SQL_USERNAME and FLIGHT_SQL_PASSWORD.
/// </summary>
public static class FlightSql
{
    public sealed class Sale
    {
        public long Id { get; set; }
        public string Customer { get; set; } = "";
        public decimal Amount { get; set; }
        public DateOnly Day { get; set; }
        public DateTimeZone CreatedAt { get; set; }
        public List<string> Tags { get; set; } = [];
    }

    /// <summary>The query as DuckDB SQL, so it runs on GizmoSQL without any table; other servers need their own dialect.</summary>
    public const string Query = """
        SELECT * FROM (VALUES
            (1::BIGINT, 'Ann', 19.99::DECIMAL(10, 2), DATE '2026-10-01', TIMESTAMPTZ '2026-10-01 08:30:00.123456+00', ['new', 'web']),
            (2::BIGINT, 'Bob', 5.00::DECIMAL(10, 2), DATE '2026-10-02', TIMESTAMPTZ '2026-10-02 17:00:00+02', [])
        ) AS sales(id, customer, amount, day, created_at, tags)
        ORDER BY id
        """;

    public static async Task RunAsync()
    {
        var uri = Environment.GetEnvironmentVariable("FLIGHT_SQL_URI") ?? "grpc+tcp://localhost:31337";
        await foreach (var sale in QueryAsync(uri, Environment.GetEnvironmentVariable("FLIGHT_SQL_USERNAME"), Environment.GetEnvironmentVariable("FLIGHT_SQL_PASSWORD")))
            Console.WriteLine($"{sale.Id} {sale.Customer} {sale.Amount} {sale.Day} {sale.CreatedAt} [{string.Join(", ", sale.Tags)}]");
    }

    /// <summary>Runs <see cref="Query"/> and maps the Arrow result into <see cref="Sale"/>s: what the integration test checks.</summary>
    public static async IAsyncEnumerable<Sale> QueryAsync(string uri, string? username, string? password)
    {
        var parameters = new Dictionary<string, string> { [AdbcOptions.Uri] = uri };
        if (username is not null) parameters[AdbcOptions.Username] = username;
        if (password is not null) parameters[AdbcOptions.Password] = password;

        using var database = FlightSqlDriverLoader.LoadDriver().Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = Query;

        var result = await statement.ExecuteQueryAsync();
        // DuckDB names columns in snake_case.
        await foreach (var sale in ArrowConvert.DeserializeAsync<Sale>(result.Stream!, new ArrowOptions { ColumnNaming = System.Text.Json.JsonNamingPolicy.SnakeCaseLower }))
            yield return sale;
    }
}
