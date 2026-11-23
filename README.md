# SolTechnology.DataLake

**Read and write Apache Iceberg tables and Parquet datasets from .NET. No Spark, no JVM.**

<p>
 <a href="https://github.com/Sol-Technology/SolTechnology.DataLake/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Sol-Technology/SolTechnology.DataLake/build.yml?label=Build&logo=github"></a>
 <a href="./LICENSE.md"><img src="https://img.shields.io/badge/licence-MIT-2F7D7A.svg"></a>
</p>

> **Status: 0.2 preview, not on NuGet yet.** It reads and writes Parquet datasets on local disk, S3 and Azure, and maps Apache Arrow (ADBC results, IPC files) to your classes. Iceberg tables come in 0.3. The API can change until 1.0.

## Why

Data teams keep their tables in a lake: Parquet files on S3 or ADLS, more and more often as Apache Iceberg tables, read by Snowflake, Databricks, Trino, BigQuery or Fabric. Java, Python, Rust, Go and C++ have an official Iceberg library. .NET has none.

So a .NET service that needs yesterday's orders, or has to append events to a table, today starts Spark, calls PyIceberg in a subprocess, or reads Parquet files one by one and guesses which of them are current.

DataLake does it in C#, on your own classes.

## Quick start

```
dotnet add package SolTechnology.DataLake --prerelease
dotnet add package SolTechnology.DataLake.Aws --prerelease     # for s3://
dotnet add package SolTechnology.DataLake.Azure --prerelease   # for abfss://, ADLS Gen2, OneLake
dotnet add package SolTechnology.DataLake.Arrow --prerelease   # ArrowConvert, ADBC results, .arrow files
dotnet add package SolTechnology.DataLake.Avro --prerelease    # .avro files
```

```csharp
public class Order
{
    public long Id { get; set; }
    public decimal? Amount { get; set; }
    public DateTimeZone CreatedAt { get; set; }   // SolTechnology.DateTimeZone
    public DateOnly Date { get; set; }            // partition: date=2026-10-29/
    public string Country { get; set; }           // partition: country=PL/
}

public class ShopLake(LakeContextOptions<ShopLake> options) : LakeContext(options), IShopLake
{
    public IDataset<Order> Orders => Dataset<Order>("orders");

    protected override void OnModelCreating(LakeModelBuilder model) =>
        model.Dataset<Order>("orders").PartitionBy(o => o.Date, o => o.Country);
}

// The only place that knows the technology. Tests: o.UseInMemory(). Iceberg in 0.3.
services.AddDataLake<IShopLake, ShopLake>(o => o.UseFiles("s3://shop/", new S3Storage(new AmazonS3Client())));

await foreach (var order in lake.Orders.Where(o => o.Date == yesterday && o.Country == "PL").ReadAsync())
{
    // only the files for that day and country were downloaded
}
await lake.Orders.ReplacePartitionsAsync(todaysOrders);
```

What it does:
* **Datasets** - a folder of files is one table: Parquet built in, Avro and Arrow IPC with their packages (`f => f.Avro().Arrow()`). Hive partitions (`date=…/country=…`) fill properties too
* **Skips what it can** - a LINQ filter skips whole files by partition and whole row groups by min/max statistics; the rest is filtered row by row. One day of 30 reads [33x faster](benchmarks/SolTechnology.DataLake.Benchmarks/README.md) than Parquet.Net file by file
* **Your classes** - columns map to properties by name (`[DataMember]`, snake_case policy, any case). Structs, lists and maps map to nested classes, lists and dictionaries
* **No guessing** - a UTC timestamp reads into `DateTimeZone` or `DateTimeOffset`, one without a zone into `UnzonedDateTime`. A missing column, a null in a non-nullable property or a lossy conversion is an error that says what to change
* **S3 and Azure** - ranged reads, so a skipped file costs nothing. S3-compatible storage (MinIO, R2) and ADLS Gen2 / OneLake through the Blob API
* **One API, the technology in one place** - a `LakeContext` like EF Core's `DbContext`, with `IDataset<T>` to read and write; the provider (folders of files now, Iceberg next) is chosen at registration. Interfaces to mock, `UseInMemory()` for tests
* **Writes** - append, overwrite, overwrite with a filter, replace partitions, delete: the words Spark, delta-rs and PyIceberg use. Parquet files with types and statistics that Spark, Trino, DuckDB and pyarrow read right (checked in CI)
* **Apache Arrow** - record batches, IPC files and ADBC query results (Databricks SQL, Snowflake, Flight SQL) into your classes and back
* **`datalake` CLI** - `ls`, `schema`, `head`, `cat`, `stats` for any location
* **OpenTelemetry** - every read is a `datalake.scan` activity with files, row groups and bytes skipped and read

Full documentation: [docs/Documentation.md](./docs/Documentation.md), and per provider: [Files](./docs/providers/Files.md), [InMemory](./docs/providers/InMemory.md).

## Plan

| Version | What you get |
|---|---|
| 0.1 ✔ | Parquet and Avro datasets on local disk, S3 and ADLS Gen2 / OneLake: hive partitions, filters that skip files and row groups, nested columns, CLI |
| 0.2 ✔ | Partitioned writes. Apache Arrow to C# classes and back, so ADBC results (Databricks SQL, Snowflake, Flight SQL) land in your types |
| 0.3 | Iceberg tables from a REST catalog (Polaris, Lakekeeper, Nessie, Unity Catalog, AWS Glue, S3 Tables): scans, time travel, schema evolution, delete files |
| 1.0 | Iceberg writes: create a table from a class, append, overwrite partitions |
| later | Delta Lake |

Out of scope: SQL engine, joins, DataFrame. For that take a look at DuckDB.NET.

Step by step: [docs/ROADMAP.md](./docs/ROADMAP.md).

## Packages

| Package | What is inside |
|---|---|
| SolTechnology.DataLake | contexts and datasets, Parquet, `UseFiles` and `UseInMemory`, `Dataset.Open`, local disk |
| SolTechnology.DataLake.Aws | S3 and S3-compatible storage |
| SolTechnology.DataLake.Azure | Azure Blob Storage, ADLS Gen2, OneLake |
| SolTechnology.DataLake.Arrow | `ArrowConvert` for record batches, ADBC results and IPC files; Arrow IPC files in datasets |
| SolTechnology.DataLake.Avro | Avro files in datasets, through AvroConvert |
| SolTechnology.DataLake.Cli | `datalake` command line tool, every format |

Cloud SDKs, Apache.Arrow and AvroConvert live in their own packages, so you only get what you use.

## Design partners

Is your .NET team on Databricks, Snowflake, Fabric, Trino or plain S3, and you would use this? Open an issue or write to contact@soltechnology.dev. Teams that use the previews and give feedback get a free licence for the commercial Iceberg package until 1.0 and for 12 months after it.

## Licence

MIT, see [LICENSE.md](./LICENSE.md). SolTechnology.DataLake.Avro is MIT too, but it uses AvroConvert, which has [its own licence](https://github.com/AdrianStrugala/AvroConvert/blob/master/LICENSE.md): free for noncommercial use and small companies, a commercial licence for others. A future SolTechnology.DataLake.Iceberg package will be commercial and will say so in its own licence.

Apache Iceberg, Apache Parquet, Apache Arrow and Apache Avro are trademarks of The Apache Software Foundation. DataLake is an independent implementation and is not endorsed by the ASF.
