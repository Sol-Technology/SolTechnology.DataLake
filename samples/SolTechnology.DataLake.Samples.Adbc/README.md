# ADBC samples

[ADBC](https://arrow.apache.org/adbc/) drivers return query results as Arrow record batches. `ArrowConvert.DeserializeAsync<T>` maps them into your classes with the same rules as Parquet datasets: columns by name (case-insensitive, `[DataMember(Name = ...)]`), timestamps with a time zone into `DateTimeZone` / `DateTimeOffset` / UTC `DateTime`, without one into `UnzonedDateTime`, lossless conversions only.

| Sample | Package | Settings |
|---|---|---|
| [Databricks.cs](Databricks.cs) | `Apache.Arrow.Adbc.Drivers.Databricks` | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` |
| [Snowflake.cs](Snowflake.cs) | `Apache.Arrow.Adbc.Drivers.Interop.Snowflake` | `SNOWFLAKE_URI` |
| [FlightSql.cs](FlightSql.cs) | `Apache.Arrow.Adbc.Drivers.FlightSql` | `FLIGHT_SQL_URI`, `FLIGHT_SQL_AUTHORIZATION` |

```
dotnet run -- databricks
```

The samples build in CI but run only against your own accounts; they read the sample tables those services provide.
