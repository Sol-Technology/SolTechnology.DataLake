# ADBC samples

[ADBC](https://arrow.apache.org/adbc/) drivers return query results as Arrow record batches. `ArrowConvert.DeserializeAsync<T>` from the SolTechnology.DataLake.Arrow package maps them into your classes with the same rules as Parquet datasets: columns by name (case-insensitive, `[DataMember(Name = ...)]`), timestamps with a time zone into `DateTimeZone` / `DateTimeOffset` / UTC `DateTime`, without one into `UnzonedDateTime`, lossless conversions only.

| Sample | Package | Settings |
|---|---|---|
| [Databricks.cs](Databricks.cs) | `Apache.Arrow.Adbc.Drivers.Databricks` | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` |
| [Snowflake.cs](Snowflake.cs) | `Apache.Arrow.Adbc.Drivers.Interop.Snowflake` | `SNOWFLAKE_URI` |
| [FlightSql.cs](../SolTechnology.DataLake.Samples.FlightSql/FlightSql.cs) (own project) | `Apache.Arrow.Adbc.Drivers.Interop.FlightSql` | `FLIGHT_SQL_URI`, `FLIGHT_SQL_USERNAME`, `FLIGHT_SQL_PASSWORD` |

```
dotnet run -- databricks     # or snowflake; Flight SQL: cd ../SolTechnology.DataLake.Samples.FlightSql && dotnet run
```

* **Flight SQL** runs in the integration tests against [GizmoSQL](https://github.com/gizmodata/gizmosql) (a Flight SQL server on DuckDB) in a container, so every build checks a real driver, a real server and the mapping of decimals, dates, timestamps with a zone and lists. It is its own project because its driver needs `Apache.Arrow.Adbc` 1.0, while the Databricks and Snowflake drivers (0.23) need 0.23.
* **Databricks and Snowflake** have no server that runs locally; their samples build in CI and run against your own accounts, on the sample tables those services provide. They go through the same `ArrowConvert` path.
* The ADBC driver packages ship native libraries for x64 only (Linux, macOS, Windows). On Apple silicon run them with an x64 .NET under Rosetta.
