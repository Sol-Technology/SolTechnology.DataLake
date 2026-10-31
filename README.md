# SolTechnology.DataLake

**Read and write Apache Iceberg tables and Parquet datasets from .NET. No Spark, no JVM.**

<p>
 <a href="https://github.com/Sol-Technology/SolTechnology.DataLake/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Sol-Technology/SolTechnology.DataLake/build.yml?label=Build&logo=github"></a>
 <a href="./LICENSE.md"><img src="https://img.shields.io/badge/licence-PolyForm%20NC%20%7C%20Small%20Business%20%7C%20Commercial-2F7D7A.svg"></a>
</p>

> **Status: 0.1 preview, not on NuGet yet.** It reads Parquet and Avro datasets from local disk, S3 and Azure. Iceberg tables come in 0.3. The API can change until 1.0.

## Why

Data teams keep their tables in a lake: Parquet files on S3 or ADLS, more and more often as Apache Iceberg tables, read by Snowflake, Databricks, Trino, BigQuery or Fabric. Java, Python, Rust, Go and C++ have an official Iceberg library. .NET has none.

So a .NET service that needs yesterday's orders, or has to append events to a table, today starts Spark, calls PyIceberg in a subprocess, or reads Parquet files one by one and guesses which of them are current.

DataLake does it in C#, on your own classes.

## Quick start

```
dotnet add package SolTechnology.DataLake --prerelease
dotnet add package SolTechnology.DataLake.Aws --prerelease     # for s3://
dotnet add package SolTechnology.DataLake.Azure --prerelease   # for abfss://, ADLS Gen2, OneLake
```

```csharp
public class Order
{
    public long Id { get; set; }
    public decimal? Amount { get; set; }
    public DateTimeZone CreatedAt { get; set; }   // SolTechnology.DateTimeZone
    public DateOnly Date { get; set; }            // from the path: date=2026-10-29/
    public string Country { get; set; }           // from the path: country=PL/
}

var orders = DataLake.Open<Order>("s3://shop/orders/", new DataLakeOptions { Storage = new S3Storage(new AmazonS3Client()) })
    .Where(o => o.Date == yesterday && o.Country == "PL");

await foreach (var order in orders.ReadAsync())
{
    // only the files for that day and country were downloaded
}
```

What 0.1 does:
* **Datasets** - a folder of Parquet or Avro files is one table. Hive partitions (`date=…/country=…`) fill properties too
* **Skips what it can** - a LINQ filter skips whole files by partition and whole row groups by min/max statistics; the rest is filtered row by row. One day of 30 reads [33x faster](benchmarks/SolTechnology.DataLake.Benchmarks/README.md) than Parquet.Net file by file
* **Your classes** - columns map to properties by name (`[DataMember]`, snake_case policy, any case). Structs, lists and maps map to nested classes, lists and dictionaries
* **No guessing** - a UTC timestamp reads into `DateTimeZone` or `DateTimeOffset`, one without a zone into `UnzonedDateTime`. A missing column, a null in a non-nullable property or a lossy conversion is an error that says what to change
* **S3 and Azure** - ranged reads, so a skipped file costs nothing. S3-compatible storage (MinIO, R2) and ADLS Gen2 / OneLake through the Blob API
* **`datalake` CLI** - `ls`, `schema`, `head` for any location
* **OpenTelemetry** - every read is a `datalake.scan` activity with files, row groups and bytes skipped and read

Full documentation: [docs/Documentation.md](./docs/Documentation.md).

## Plan

| Version | What you get |
|---|---|
| 0.1 ✔ | Parquet and Avro datasets on local disk, S3 and ADLS Gen2 / OneLake: hive partitions, filters that skip files and row groups, nested columns, CLI |
| 0.2 | Partitioned writes. Apache Arrow to C# classes and back, so ADBC results (Databricks SQL, Snowflake, Flight SQL) land in your types |
| 0.3 | Iceberg tables from a REST catalog (Polaris, Lakekeeper, Nessie, Unity Catalog, AWS Glue, S3 Tables): scans, time travel, schema evolution, delete files |
| 1.0 | Iceberg writes: create a table from a class, append, overwrite partitions |
| later | Delta Lake |

Out of scope: SQL engine, joins, DataFrame. For that take a look at DuckDB.NET.

Step by step: [docs/ROADMAP.md](./docs/ROADMAP.md).

## Packages

| Package | What is inside |
|---|---|
| SolTechnology.DataLake | datasets, Parquet, Avro, local disk |
| SolTechnology.DataLake.Aws | S3 and S3-compatible storage |
| SolTechnology.DataLake.Azure | Azure Blob Storage, ADLS Gen2, OneLake |
| SolTechnology.DataLake.Cli | `datalake` command line tool |

Cloud SDKs live in their own packages, so you only get the one you use.

## Design partners

Is your .NET team on Databricks, Snowflake, Fabric, Trino or plain S3, and you would use this? Open an issue or write to contact@soltechnology.dev. Teams that use the previews and give feedback get a free commercial licence until 1.0 and for 12 months after it.

## Licence

Source-available. Free for noncommercial use and for companies under 100 people and 1M USD revenue (PolyForm Noncommercial / Small Business). Other companies need a commercial licence, separate from the AvroConvert one. Details in [LICENSE.md](./LICENSE.md).

Apache Iceberg, Apache Parquet, Apache Arrow and Apache Avro are trademarks of The Apache Software Foundation. DataLake is an independent implementation and is not endorsed by the ASF.
