<!-- github-only -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./docs/logo-dark.png">
    <img alt="SolTechnology / DataLake" src="./docs/logo.png" width="300">
  </picture>
</p>

<p align="center">
  Iceberg tables and Parquet datasets in .NET. No Spark, no JVM.
</p>

<p align="center">
 <a href="https://github.com/Sol-Technology/SolTechnology.DataLake/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Sol-Technology/SolTechnology.DataLake/build.yml?label=Build&logo=github&labelColor=0B1F33"></a>
 <a href="./LICENSE.md"><img src="https://img.shields.io/badge/licence-MIT-2F7D7A.svg?labelColor=0B1F33"></a>
 <a href="./docs/ROADMAP.md"><img src="https://img.shields.io/badge/status-0.2%20preview-F59E0B.svg?labelColor=0B1F33"></a>
</p>
<!-- /github-only -->

> **Status: 0.2 preview, not on NuGet yet.** It reads and writes Parquet datasets on local disk, S3 and Azure, and maps Apache Arrow (ADBC results, IPC files) to your classes. Iceberg tables come in 0.3. The API can change until 1.0.

## Docs

[DataLake documentation](./docs/Documentation.md)

[Providers: Files](./docs/providers/Files.md), [InMemory](./docs/providers/InMemory.md)

[Roadmap](./docs/ROADMAP.md) and [release notes](./docs/release-notes)


## Why DataLake

**Data teams keep their tables in a lake: Parquet files on S3 or ADLS, more and more often as Apache Iceberg tables.** Snowflake, Databricks, Trino, BigQuery and Fabric read them. Java, Python, Rust, Go and C++ have an official Iceberg library. .NET has none.

So a .NET service that needs yesterday's orders, or has to append events to a table, today starts Spark, calls PyIceberg in a subprocess, or reads Parquet files one by one and guesses which of them are current. DataLake does it in C#, on your own classes.

|                                                                     |    DataLake    |      Parquet.Net, file by file      |
|---------------------------------------------------------------------|:--------------:|:-----------------------------------:|
| A folder of files is one table, `date=…/country=…` fill properties  |       ✔️       |         ❌ one file at a time         |
| LINQ filter skips files and row groups before reading them          |       ✔️       |                  ❌                  |
| Rows into your classes, with nested structs, lists and maps         |       ✔️       |                  ✔️                  |
| Partitioned writes: append, overwrite, replace partitions, delete   |       ✔️       |           ❌ single files            |
| S3 and Azure with ranged reads                                      |       ✔️       |        ❌ bring your own stream       |
| Apache Arrow and ADBC results into your classes                     |       ✔️       |                  ❌                  |
| Apache Iceberg tables                                               |      0.3       |                  ❌                  |
| One day of 30, one country (900 000 rows, local disk)               |   **13 ms**    |        421 ms, 33× slower           |
| All 900 000 rows                                                    |     450 ms     |               474 ms                |

DataLake reads the files with Parquet.Net, so the last two rows are about what it skips, not a faster decoder. Benchmark: [results and how to run it](benchmarks/SolTechnology.DataLake.Benchmarks/README.md), with pyarrow on the same files.

What it means for your services:
* **Less to download** – a filter on a partition or on min/max statistics skips files and row groups; on S3 or ADLS they are never downloaded
* **No guessing** – a UTC timestamp reads into `DateTimeZone` or `DateTimeOffset`, one without a zone into `UnzonedDateTime`. A missing column, a null in a non-nullable property or a lossy conversion is an error that says what to change
* **The technology in one place** – a `LakeContext` like EF Core's `DbContext`; the provider (folders of files now, Iceberg next) is chosen at registration, and `UseInMemory()` replaces it in tests

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

## Features

#### Read with a filter, and see what it skipped
```csharp
var metrics = new ScanMetrics();
var big = await lake.Orders.Where(o => o.Amount > 100m).ReadAsync(metrics).ToListAsync();
```

#### Append, overwrite, replace partitions, delete: the words Spark and PyIceberg use
```csharp
await lake.Orders.OverwriteAsync(fixedOrders, where: o => o.Date == day);
```

#### Any folder or file, without a context
```csharp
var orders = Dataset.Open<Order>("s3://shop/export/", new DataLakeOptions { Storage = new S3Storage(s3) });
```

#### Apache Arrow and ADBC (Databricks SQL, Snowflake, Flight SQL)
```csharp
await foreach (var trip in ArrowConvert.DeserializeAsync<Trip>(result.Stream!)) { }
```

#### Tests without disk
```csharp
var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory()));
```

#### Command line
```bash
datalake stats s3://shop/orders/        # also ls, schema, head, cat
```

#### OpenTelemetry: files, row groups and bytes skipped and read
```csharp
.WithTracing(t => t.AddSource(DataLakeTelemetry.Name))
```

Parquet built in, Avro and Arrow IPC files with their packages (`f => f.Avro().Arrow()`). S3-compatible storage (MinIO, R2) and ADLS Gen2 / OneLake through the Blob API. Parquet files it writes are read right by Spark, Trino, DuckDB and pyarrow (checked in CI).

All features: [documentation](./docs/Documentation.md)


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


## Contribution

If you have any idea, found a bug, a missing format or a better way for integration, leave an issue or a pull request.


## Related Work

- [![AvroConvert](https://img.shields.io/nuget/v/AvroConvert?label=AvroConvert&logo=nuget&labelColor=0B1F33)](https://github.com/AdrianStrugala/AvroConvert) – Apache Avro serializer for .NET, used here for `.avro` files
- [![DateTimeZone](https://img.shields.io/nuget/v/SolTechnology.DateTimeZone?label=SolTechnology.DateTimeZone&logo=nuget&labelColor=0B1F33)](https://github.com/Sol-Technology/SolTechnology.DateTimeZone) – a point in time with its time zone, the type timestamps read into

---

Apache Iceberg, Apache Parquet, Apache Arrow and Apache Avro are trademarks of [The Apache Software Foundation](https://www.apache.org/). DataLake is an independent implementation and is not affiliated with, endorsed by or sponsored by the ASF.
