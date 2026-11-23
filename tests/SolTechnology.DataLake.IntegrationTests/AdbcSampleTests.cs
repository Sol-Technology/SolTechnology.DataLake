using System.Runtime.InteropServices;
using SolTechnology.DataLake.IntegrationTests.Containers;
using SolTechnology.DataLake.Samples.FlightSql;
using SolTechnology.Time;

namespace SolTechnology.DataLake.IntegrationTests;

// The Flight SQL sample, run as it is against a Flight SQL server in a container: ADBC driver, Arrow result, ArrowConvert.
// Databricks and Snowflake have no local server; their samples share the same ArrowConvert path.
public class AdbcSampleTests
{
    private GizmoSqlFixture _server = null!;

    [OneTimeSetUp]
    public async Task StartServer()
    {
        // The ADBC driver packages ship native libraries for x64 only; on Arm (Apple silicon) the driver can't load.
        if (RuntimeInformation.OSArchitecture != Architecture.X64 && Environment.GetEnvironmentVariable("DATALAKE_ADBC_NATIVE") != "1")
            Assert.Ignore("The ADBC drivers ship native libraries for x64 only. Runs in CI (Linux x64).");
        _server = new GizmoSqlFixture();
        await _server.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task StopServer()
    {
        if (_server is not null) await _server.DisposeAsync();
    }

    [Test]
    public async Task Flight_sql_results_land_in_classes()
    {
        var sales = await FlightSql.QueryAsync(_server.Uri, GizmoSqlFixture.User, GizmoSqlFixture.Password).ToListAsync();

        sales.Should().BeEquivalentTo(new[]
        {
            new FlightSql.Sale
            {
                Id = 1, Customer = "Ann", Amount = 19.99m, Day = new DateOnly(2026, 10, 1),
                CreatedAt = DateTimeZone.FromUtc(new DateTime(2026, 10, 1, 8, 30, 0, DateTimeKind.Utc).AddTicks(1_234_560), TimeZoneInfo.Utc),
                Tags = ["new", "web"],
            },
            new FlightSql.Sale
            {
                Id = 2, Customer = "Bob", Amount = 5.00m, Day = new DateOnly(2026, 10, 2),
                CreatedAt = DateTimeZone.FromUtc(new DateTime(2026, 10, 2, 15, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Utc),
                Tags = [],
            },
        }, o => o.WithStrictOrdering());
    }

    [Test]
    public async Task A_wrong_password_is_an_error_from_the_driver()
    {
        var query = () => FlightSql.QueryAsync(_server.Uri, GizmoSqlFixture.User, "wrong").ToListAsync().AsTask();

        await query.Should().ThrowAsync<Apache.Arrow.Adbc.AdbcException>();
    }
}
