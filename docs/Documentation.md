# SolTechnology.DataLake documentation

Version 0.2 (preview). The API can change until 1.0.

This page is what every provider shares. What a provider stores, how and at what cost: [Parquet](providers/Parquet.md), [InMemory](providers/InMemory.md). Iceberg comes in 0.3.

* [Install](#install)
* [Contexts](#contexts)
* [Read a dataset](#read-a-dataset)
* [Write a dataset](#write-a-dataset)
* [Tests](#tests)
* [A quick look: Dataset.Open](#a-quick-look-datasetopen)
* [Partitions](#partitions)
* [Columns and properties](#columns-and-properties)
* [Types](#types)
* [Nested columns](#nested-columns)
* [Filters](#filters)
* [Apache Arrow and ADBC](#apache-arrow-and-adbc)
* [Telemetry](#telemetry)
* [Command line](#command-line)
* [Limits of 0.2](#limits-of-02)
* [Compatibility](#compatibility)

## Install

```
dotnet add package SolTechnology.DataLake --prerelease
dotnet add package SolTechnology.DataLake.Aws --prerelease      # s3://
dotnet add package SolTechnology.DataLake.Azure --prerelease    # abfss://, wasbs://, https://, az://
dotnet tool install -g SolTechnology.DataLake.Cli --prerelease  # datalake
```

.NET 10.

## Contexts

A context is a set of datasets with one provider (the storage technology) behind them, like EF Core's `DbContext` without change tracking: reads stream, writes go straight to storage.

```csharp
public interface IShopLake : IDataLake
{
    IDataset<Order> Orders { get; }
}

public class ShopLake(LakeContextOptions<ShopLake> options) : LakeContext(options), IShopLake
{
    public IDataset<Order> Orders => Dataset<Order>("orders");

    protected override void OnModelCreating(LakeModelBuilder model) =>
        model.Dataset<Order>("orders").PartitionBy(o => o.Date, o => o.Country);
}

services.AddDataLake<IShopLake, ShopLake>((sp, o) => o
    .UseParquet("s3://shop/", new S3Storage(sp.GetRequiredService<IAmazonS3>()), p => p.Compression(ParquetCompression.Zstd))
    .ColumnNaming(JsonNamingPolicy.SnakeCaseLower));
```

* **The provider is the only thing that knows the technology.** `UseParquet(root, storage, ...)` or `UseInMemory()`; the code that reads and writes `lake.Orders` does not change with it. Provider settings go in the provider's own callback; what all providers share (`ColumnNaming`) goes on the builder.
* **The model** (`OnModelCreating`) declares the datasets that can be written and how they are partitioned. It is built once per context class, on first use. A dataset not in the model can be read, but a write throws: a typo never creates or overwrites data.
* **Partitioning in the model is for creating a dataset.** An existing dataset partitioned differently is an error on write, never silently rewritten.
* **Registration**: `AddDataLake<TContext>` or `AddDataLake<TService, TContext>` (your interface resolves to the same instance), with `(sp, o) => ...` when the provider needs services. Singleton: a context holds no per-request state. It does not own or dispose the SDK clients you pass in.
* **Two technologies side by side** = two context classes, each registered on its own, injected by type or interface. The same model twice (a production and an archive copy) = a subclass, as in EF:

```csharp
public class ShopLake : LakeContext, IShopLake
{
    public ShopLake(LakeContextOptions<ShopLake> options) : base(options) { }
    protected ShopLake(LakeContextOptions options) : base(options) { }   // for subclasses
    public IDataset<Order> Orders => Dataset<Order>("orders");
}

public class ArchiveLake(LakeContextOptions<ArchiveLake> options) : ShopLake(options), IArchiveLake;

services.AddDataLake<IShopLake, ShopLake>(o => o.UseParquet("s3://shop/", s3));
services.AddDataLake<IArchiveLake, ArchiveLake>(o => o.UseParquet("s3://archive/", s3));
```

* **Without DI**: `new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseParquet("/data")))`. **Without a class**, for scripts: `LakeContext.Create(o => o.UseParquet("/data"), m => m.Dataset<Order>("orders"))`.
* `EnsureCreatedAsync()` creates declared datasets that don't exist yet; folder-based providers have nothing to create.

## Read a dataset

```csharp
await foreach (var order in lake.Orders.Where(o => o.Date == yesterday && o.Country == "PL").ReadAsync(ct)) { }

long count = await lake.Orders.CountAsync();
var metrics = new ScanMetrics();
var rows = await lake.Orders.Where(o => o.Amount > 100m).ReadAsync(metrics).ToListAsync();
// files 90 (skipped by partition 89), row groups 2 (skipped by statistics 0), rows read 10000, returned 10000, bytes read 61442
```

* `Where` returns an `IDatasetQuery<T>`: read and count only, so a write through a filter does not compile. `Where` calls combine with `&&`.
* Nothing is read until you enumerate. There is no cache: reading again reads the storage again.
* A dataset that does not exist yet reads as empty.
* What `CountAsync` costs depends on the provider (see its page).

## Write a dataset

The words are the ones Spark, delta-rs and PyIceberg use; `overwrite` means the whole dataset, as everywhere else.

| Method | What it does | No rows |
|---|---|---|
| `AppendAsync(rows)` | adds rows | nothing |
| `OverwriteAsync(rows)` | replaces the whole dataset | empties it |
| `OverwriteAsync(rows, where: o => ...)` | deletes what the filter matches and adds rows; every row must match the filter too (Delta's `replaceWhere`), else nothing changes | deletes what matches |
| `ReplacePartitionsAsync(rows)` | replaces only the partitions the rows fall into (Spark's dynamic partition overwrite, PyIceberg's `dynamic_partition_overwrite`); an unpartitioned dataset throws | nothing |
| `DeleteAsync(where)` | deletes what the filter matches | |

```csharp
await lake.Orders.AppendAsync(newOrders);
await lake.Orders.ReplacePartitionsAsync(todaysOrders);
await lake.Orders.OverwriteAsync(fixedOrders, where: o => o.Date == day);
```

* `rows` is an `IEnumerable<T>` or an `IAsyncEnumerable<T>`. Every mapped property is a column, named like on read, so the rows read back into the same class.
* Each write returns a `LakeWriteResult`: the files written, the files removed, the rows.
* A failed or cancelled write removes the files it already wrote.
* What a provider can't do throws `NotSupportedException` with what to do instead, never a silent approximation. The Parquet provider, for example, deletes whole files, so the filter of an overwrite or delete must be on partition columns. Each provider's page has the table.

## Tests

* **Your logic**: mock your context's interface (`IShopLake`) or `IDataset<T>`. The interfaces have one method per operation and no optional parameters, so Moq and NSubstitute set them up directly; the shorter calls (`ReadAsync()`, `AppendAsync(IEnumerable<T>)`) are extension methods on top.
* **Real reads and writes, no disk**: `UseInMemory()`. It is the Parquet provider on memory, not a second implementation, so it behaves like `UseParquet` (types, partitions, errors). See [InMemory](providers/InMemory.md).

```csharp
var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory()));
await sut.ImportAsync(lake);
(await lake.Orders.CountAsync()).Should().Be(3);
```

## A quick look: Dataset.Open

For one folder or file, without a context. Read-only: it returns an `IDatasetQuery<T>`.

```csharp
var orders = Dataset.Open<Order>("/data/orders");                                    // local path or file://
var one = Dataset.Open<Order>("s3://shop/export/orders.parquet", new DataLakeOptions { Storage = new S3Storage(s3) });
await foreach (var order in orders.Where(o => o.Country == "PL").ReadAsync()) { }
```

`DataLakeOptions`:

| Option | Default | |
|---|---|---|
| `Storage` | local disk | an `ILakeStorage` for other locations: `S3Storage`, `AzureBlobStorage`, or your own |
| `ColumnNaming` | none | `JsonNamingPolicy` for property names without `[DataMember]`, e.g. `JsonNamingPolicy.SnakeCaseLower` for `created_at` |

Unlike a context, a local folder that does not exist is an error here. Which files are read: [Parquet provider](providers/Parquet.md#files).

## Partitions

Directory names like `date=2026-10-29/country=PL/` are partitions. A property with the same column name gets the value from the path, typed as the property:

```csharp
public class Order
{
    public long Id { get; set; }
    public DateOnly Date { get; set; }      // date=2026-10-29
    public string Country { get; set; }     // country=PL
}
```

* Partition values parse into `string`, numbers, `bool`, enums, `Guid`, `DateOnly`, `DateTime`, `DateTimeOffset`, `DateTimeZone`, `UnzonedDateTime`.
* Values are URL-decoded (`hour=2026-10-29T12%3A00%3A00`).
* Hive's default partition `__HIVE_DEFAULT_PARTITION__` is null. A null into a non-nullable property is an error.
* A partition without a matching property is ignored.

## Columns and properties

A public settable property maps to the column named:

1. `[DataMember(Name = "...")]` if it has one,
2. else `ColumnNaming` applied to the property name,
3. else the property name.

Matching ignores case. `[IgnoreDataMember]` leaves a property out. The class needs a public parameterless constructor.

| Situation | Result |
|---|---|
| Column without a property | not read |
| Property without a column, nullable (`string?`, `int?`, a class) | null |
| Property without a column, non-nullable value type | error naming the file and the column. Make it nullable, or `[IgnoreDataMember]` |
| Null value, non-nullable value type | error |
| Different files with different columns | each file is mapped on its own, so a column added later is null in older files |

## Types

Only lossless conversions. Anything else is an error that names the column and both types.

| Column | Properties |
|---|---|
| boolean, int8-64, uint, float, double | same type or wider (`int` into `long`, `float` into `double`, any integer into `decimal`) |
| decimal | `decimal` |
| string | `string`, enum (by name, any case) |
| JSON | `string` |
| integer | enum (by value) |
| binary | `byte[]` |
| UUID | `Guid` |
| date | `DateOnly`, `DateTime` |
| time | `TimeOnly`, `TimeSpan` |
| timestamp UTC (`isAdjustedToUTC=true`) | `DateTimeZone` (in UTC), `DateTimeOffset`, `DateTime` (`Kind.Utc`) |
| timestamp without a zone | `UnzonedDateTime`, `DateTime` (`Kind.Unspecified`) |

Timestamps follow [SolTechnology.DateTimeZone](https://github.com/Sol-Technology/SolTechnology.DateTimeZone): an instant is never read as a wall-clock time and the other way round. A timestamp without a zone into `DateTimeOffset` is an error; read it into `UnzonedDateTime` and call `.InZone(zone)` if you know the zone.

## Nested columns

| Column | Properties |
|---|---|
| struct | a class (same name rules), `Dictionary<string, object?>`, `object` |
| list | `T[]`, `List<T>`, `IList<T>`, `IReadOnlyList<T>`, `IEnumerable<T>`, `object` |
| map | `Dictionary<K, V>`, `IDictionary<K, V>`, `IReadOnlyDictionary<K, V>`, `object` |

Any depth: a list of structs with lists inside works. Null struct, null list, empty list and null element stay apart. A null element into `List<int>` is an error; use `List<int?>`. Errors name the path inside the column: `lines[].price`, `matrix[][]`.

Older list layouts are read like pyarrow reads them:

* a repeated field without a list group (`repeated int32 ids`) is a list;
* a two-level list from parquet-mr before 1.9 and Spark 1.x (`optional group stats (LIST) { repeated double array; }`) is a list of the repeated type;
* a repeated group without the LIST annotation, as protobuf writers produce it (`repeated group lines { required binary sku; }`), is a list of structs.

## Filters

```csharp
var rows = lake.Orders
    .Where(o => o.Date >= from && o.Country == "PL")
    .Where(o => o.Amount > 100m);
```

`Where` calls are combined with `&&`. Every filter is applied to every row, so results are always right. On top of that, comparisons of a property with a value skip data before reading it:

* **files**, by partition values from the path,
* **row groups** (Parquet), by min/max and null counts in the file. Text statistics are used when both ends are ASCII: their byte order, the one older writers got wrong for other characters, and .NET's ordinal order are then the same.

What skips: `==`, `!=`, `<`, `<=`, `>`, `>=` against a constant or a captured variable, `&&`, `||`, `!`, `list.Contains(x.P)` and `array.Contains(x.P)`, a bool property alone. Anything else (`x.Name.StartsWith("A")`, `x.A + x.B > 5`) still filters, row by row, and does not stop the parts around it from skipping. Some comparisons filter row by row on purpose, because skipping could lose rows: a cast that loses values (`(int)x.Price == 2`), floating-point `!=` and negations (NaN is in no statistic), and enums against row group statistics (enums are stored by name, statistics are ordered by name).

Nulls work like in C#: `x.Amount > 5` is false for null, `x.Amount != 5` is true for null, `x.Amount == null` matches nulls.

What a read skipped and read: `ReadAsync(metrics)`, see [Read a dataset](#read-a-dataset).

## Apache Arrow and ADBC

`ArrowConvert` maps Arrow data to your classes and back, with the rules of this page (names, types, nested columns, no lossy conversions):

```csharp
IReadOnlyList<Order> rows = ArrowConvert.Deserialize<Order>(recordBatch);
RecordBatch batch = ArrowConvert.Serialize(orders);
Schema schema = ArrowConvert.SchemaOf<Order>();

await foreach (var order in ArrowConvert.DeserializeAsync<Order>(arrowArrayStream)) { }      // IArrowArrayStream
await foreach (var order in ArrowConvert.ReadIpcAsync<Order>(File.OpenRead("orders.arrow"))) { }
await ArrowConvert.WriteIpcAsync(stream, orders, ArrowIpcFormat.File);                       // or Stream
```

[ADBC](https://arrow.apache.org/adbc/) drivers return query results as an `IArrowArrayStream`, so Databricks SQL, Snowflake, BigQuery and any Flight SQL server land in your types:

```csharp
using var database = new DatabricksDriver().Open(parameters);
using var connection = database.Connect(new Dictionary<string, string>());
using var statement = connection.CreateStatement();
statement.SqlQuery = "SELECT * FROM samples.nyctaxi.trips";
var result = await statement.ExecuteQueryAsync();
await foreach (var trip in ArrowConvert.DeserializeAsync<Trip>(result.Stream!)) { }
```

Samples for Databricks, Snowflake and Flight SQL: [samples/SolTechnology.DataLake.Samples.Adbc](../samples/SolTechnology.DataLake.Samples.Adbc/README.md) (Flight SQL is tested in CI against a real server).

* Read: integers, floats, half floats, booleans, (large / view) strings and binaries, decimal32 to decimal256 (into `decimal` when it fits), UUID, date32/64, time32/64, timestamps in any unit (with a time zone: an instant; without: wall-clock), durations, lists (also large and fixed size), structs, maps, dictionary-encoded columns.
* Written: the Parquet types above, as Arrow types: timestamp(us, "UTC") / timestamp(us), decimal128, the `arrow.uuid` extension, time64(us), `TimeSpan` as duration(us).
* `ArrowOptions`: `ColumnNaming`, and `BatchSize` (rows per batch when writing IPC, 65 536).

## Telemetry

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(DataLakeTelemetry.Name))
    .WithMetrics(m => m.AddMeter(DataLakeTelemetry.Name));
```

* Activity `datalake.scan` per read, tagged with `datalake.location`, `datalake.row_type`, `datalake.filter`, and what was skipped and read (`datalake.files`, `datalake.files.skipped`, `datalake.row_groups`, `datalake.row_groups.skipped`, `datalake.rows.read`, `datalake.rows.returned`, `datalake.bytes.read`).
* Activity `datalake.write` per write, tagged with `datalake.write.mode`, `datalake.filter`, `datalake.files.written`, `datalake.files.removed`, `datalake.rows.written`, `datalake.bytes.written`.
* Both carry `datalake.context`, `datalake.dataset` and `datalake.provider`.
* Counters: `datalake.write.files`, `datalake.write.files.removed`, `datalake.write.rows`, `datalake.write.bytes`, `datalake.scan.files`, `datalake.scan.files.skipped`, `datalake.scan.row_groups.skipped`, `datalake.scan.rows.read`, `datalake.scan.bytes.read`.

## Command line

```
$ datalake ls s3://shop/orders/
    61.2 KB  parquet  date=2026-10-01/country=DE/part-00000.parquet
    ...
90 files, 5.4 MB

$ datalake schema s3://shop/orders/
# date=2026-10-01/country=DE/part-00000.parquet
id: int64 not null
amount: decimal(10, 2)
created_at: timestamp(micros, UTC)
lines: list
  element: struct
    sku: string
# 2 row groups, written by parquet-mr version 1.13.1
# partitions from the path: date, country

$ datalake head s3://shop/orders/ -n 3
{"id":1,"amount":9.99,"created_at":"2026-10-29T12:30:15.123456Z","lines":[{"sku":"x"}],"date":"2026-10-01","country":"DE"}
```

```
$ datalake cat s3://shop/orders/ --format csv --limit 1000 > orders.csv

$ datalake stats s3://shop/orders/
90 files (90 parquet), 5.4 MB, 900,000 rows, 180 row groups
partition date: 30 values (2026-10-01, 2026-10-02, 2026-10-03, 2026-10-04, 2026-10-05, ...)
partition country: 3 values (DE, PL, US)
column        type                    nulls  min                  max
id            int64                       0  0                    899999
amount        decimal(10, 2)          90000  0.01                 8999.99
lines[].sku   string                      0  sku-000              sku-499
```

`cat` prints every row as JSON lines, or CSV with a header (nested values as JSON). `stats` sums the Parquet footers without reading data pages; Avro and Arrow files are counted row by row. A column whose range is missing in some row group shows `?`.

Credentials come from the environment, like the cloud CLIs: the default AWS chain for `s3://` (`--s3-endpoint` for MinIO), `--azure-connection-string`, `AZURE_STORAGE_CONNECTION_STRING` or `DefaultAzureCredential` for Azure. `DefaultAzureCredential` is only sent to Azure's global storage hosts (`*.blob.core.windows.net`, `*.dfs.core.windows.net`, DNS zone endpoints `*.blob.storage.azure.net`, OneLake), because your token opens every account you can reach; for anything else (Azurite, a custom endpoint, Azure China or US Government) pass a connection string. Exit code 2 on errors.

## Limits of 0.2

* Iceberg tables come in 0.3 (read) and 1.0 (write), Delta Lake later. Until then the only real provider is Parquet folders: no transactions, no schema checks against files already there (see [Parquet](providers/Parquet.md)).
* Free-text SQL over a context's datasets is not there yet; the options are recorded in [design/lake-context.md](design/lake-context.md).
* Nested columns in legacy layouts (repeated fields without a LIST group, two-level lists) and into `object` or `Dictionary<string, object?>` still go value by value, slower than typed ones.
* Parquet.Net 6.1 writes the min/max statistics of microsecond and nanosecond timestamps in milliseconds. Those statistics are not used for files written by Parquet.Net directly, so such filters read the row groups instead of skipping them. Files DataLake writes have them fixed; files from Spark, PyIceberg, pyarrow or Trino are fine.
* Tested against Silo (a MinIO fork) and Azurite in Docker. ADLS Gen2, OneLake and R2 use the same APIs but were not tested against the real services yet.
* ADBC: the Flight SQL sample runs in the integration tests against a Flight SQL server in a container; the Databricks and Snowflake samples run only against your own accounts. The ADBC driver packages ship native libraries for x64 only.

## Compatibility

Every build reads 114 files from [apache/parquet-testing](https://github.com/apache/parquet-testing) and the Parquet.Net test data (Spark, parquet-mr, Impala, Hive, pyarrow, DuckDB, Rust and protobuf writers) and compares row counts and the first 20 rows with pyarrow. 92 match. pyarrow can't read 6 of them either (DataLake reads 3). The other 16:

* FLOAT16 columns, the Hadoop LZ4 framing of old parquet-mr, a MAP without values, a repeated group with several fields and no LIST annotation, and a few page layouts fail inside Parquet.Net 6.1;
* decimals with more than 28 digits don't fit `System.Decimal`;
* `large_string_map.brotli.parquet` (strings over 2 GB): after it fails, Parquet.Net returns corrupted strings for other files in the same process. Don't read files like that next to real work until it is fixed upstream.

The list with reasons is in [KnownGaps.cs](../tests/SolTechnology.DataLake.CompatibilityTests/KnownGaps.cs).
