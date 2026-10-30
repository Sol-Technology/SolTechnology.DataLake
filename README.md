# SolTechnology.DataLake

**Read and write Apache Iceberg tables and Parquet datasets from .NET. No Spark, no JVM.**

<p>
 <a href="https://github.com/Sol-Technology/SolTechnology.DataLake/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Sol-Technology/SolTechnology.DataLake/build.yml?label=Build&logo=github"></a>
 <a href="./LICENSE.md"><img src="https://img.shields.io/badge/licence-PolyForm%20NC%20%7C%20Small%20Business%20%7C%20Commercial-2F7D7A.svg"></a>
</p>

> **Status: nothing to install yet.** The repository is public from the first commit. The first preview (0.1) will read Parquet datasets from S3 and Azure Data Lake Storage. Watch the repository to keep you updated.

## Why

Data teams keep their tables in a lake: Parquet files on S3 or ADLS, more and more often as Apache Iceberg tables, read by Snowflake, Databricks, Trino, BigQuery or Fabric. Java, Python, Rust, Go and C++ have an official Iceberg library. .NET has none.

So a .NET service that needs yesterday's orders, or has to append events to a table, today starts Spark, calls PyIceberg in a subprocess, or reads Parquet files one by one and guesses which of them are current.

DataLake does it in C#, on your own classes. The API below is a sketch and it will change:

```csharp
// A folder of Parquet files, partitioned by date=…/country=…
var orders = DataLake.Open<Order>("s3://shop/orders/");

await foreach (var order in orders.Where(o => o.Date == yesterday && o.Country == "PL"))
{
    // only the files for that day and country are read
}

// An Iceberg table from a REST catalog
var catalog = new RestCatalog(new Uri("https://catalog.example.com"), credentials);
var table = await catalog.LoadTableAsync<Order>("shop.orders");

await table.AppendAsync(newOrders);
```

## Plan

| Version | What you get |
|---|---|
| 0.1 | Parquet and Avro datasets on local disk, S3 and ADLS Gen2 / OneLake: hive partitions, filters that skip files by their statistics, `IAsyncEnumerable<T>` |
| 0.2 | Partitioned writes. Apache Arrow to C# classes and back, so ADBC results (Databricks SQL, Snowflake, Flight SQL) land in your types |
| 0.3 | Iceberg tables from a REST catalog (Polaris, Lakekeeper, Nessie, Unity Catalog, AWS Glue, S3 Tables): scans, time travel, schema evolution, delete files |
| 1.0 | Iceberg writes: create a table from a class, append, overwrite partitions |
| later | Delta Lake |

Out of scope: SQL engine, joins, DataFrame. For that take a look at DuckDB.NET.

## Packages

| Package | What is inside |
|---|---|
| SolTechnology.DataLake | local files, Parquet, Avro, Arrow, Iceberg, REST catalog |
| SolTechnology.DataLake.Aws | S3 |
| SolTechnology.DataLake.Azure | ADLS Gen2, OneLake |
| SolTechnology.DataLake.Cli | `datalake` command line tool |

Cloud SDKs live in their own packages, so you only get the one you use.

## Design partners

Is your .NET team on Databricks, Snowflake, Fabric, Trino or plain S3, and you would use this? Open an issue or write to contact@soltechnology.dev. Teams that use the previews and give feedback get a free commercial licence until 1.0 and for 12 months after it.

## Licence

Source-available. Free for noncommercial use and for companies under 100 people and 1M USD revenue (PolyForm Noncommercial / Small Business). Other companies need a commercial licence, separate from the AvroConvert one. Details in [LICENSE.md](./LICENSE.md).

Apache Iceberg, Apache Parquet, Apache Arrow and Apache Avro are trademarks of The Apache Software Foundation. DataLake is an independent implementation and is not endorsed by the ASF.
