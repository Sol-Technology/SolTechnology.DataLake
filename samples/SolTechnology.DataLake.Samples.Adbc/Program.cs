using SolTechnology.DataLake.Samples.Adbc;

// dotnet run -- databricks | snowflake
switch (args.FirstOrDefault()?.ToLowerInvariant())
{
    case "databricks": await Databricks.RunAsync(); break;
    case "snowflake": await Snowflake.RunAsync(); break;
    default:
        Console.Error.WriteLine("Usage: dotnet run -- databricks | snowflake");
        return 2;
}
return 0;
