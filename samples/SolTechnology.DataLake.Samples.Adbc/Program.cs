using SolTechnology.DataLake.Samples.Adbc;

// dotnet run -- databricks | snowflake | flightsql
switch (args.FirstOrDefault()?.ToLowerInvariant())
{
    case "databricks": await Databricks.RunAsync(); break;
    case "snowflake": await Snowflake.RunAsync(); break;
    case "flightsql": await FlightSql.RunAsync(); break;
    default:
        Console.Error.WriteLine("Usage: dotnet run -- databricks | snowflake | flightsql");
        return 2;
}
return 0;
