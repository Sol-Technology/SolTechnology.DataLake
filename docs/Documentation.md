# SolTechnology.DataLake documentation

Version 0.2 (preview). The API can change until 1.0.

* [Install](#install)
* [Open a dataset](#open-a-dataset)
* [Partitions](#partitions)
* [Columns and properties](#columns-and-properties)
* [Types](#types)
* [Nested columns](#nested-columns)
* [Filters](#filters)
* [Avro files](#avro-files)
* [Arrow files](#arrow-files)
* [Write a dataset](#write-a-dataset)
* [Apache Arrow and ADBC](#apache-arrow-and-adbc)
* [Storage: S3](#storage-s3)
* [Storage: Azure, ADLS Gen2, OneLake](#storage-azure-adls-gen2-onelake)
* [Storage: writes](#storage-writes)
* [Telemetry](#telemetry)
* [Command line](#command-line)
* [Limits of 0.2](#limits-of-02)

## Install

```
dotnet add package SolTechnology.DataLake --prerelease
dotnet add package SolTechnology.DataLake.Aws --prerelease      # s3://
dotnet add package SolTechnology.DataLake.Azure --prerelease    # abfss://, wasbs://, https://, az://
dotnet tool install -g SolTechnology.DataLake.Cli --prerelease  # datalake
```

.NET 10.

## Open a dataset

A dataset is every Parquet (`.parquet`, `.parq`), Avro (`.avro`) and Arrow IPC (`.arrow`, `.feather`, `.ipc`) file under one location, read as one table into your class. The location can also be one file.

```csharp
var dataset = DataLake.Open<Order>("/data/orders");            // local path or file://
await foreach (var order in dataset.ReadAsync())
{
}

var one = DataLake.Open<Order>("s3://shop/export/orders.parquet", options);   // a single file
```

* Files are read in path order, row groups in file order.
* Hidden files are left out: names starting with `_` or `.` (`_SUCCESS`, `.part-0.parquet.crc`, `_temporary/`), unless they are a partition (`_x=1/`).
* A folder named like a file (Spark's `df.write.parquet("orders.parquet")`) is still read as a folder. A single file has no partitions, as partitions come from the path under the opened location.
* Nothing is read until you enumerate `ReadAsync()`. Opening the same dataset again reads the files again; there is no cache.
* A file that can't be read (corrupt, or a feature Parquet.Net doesn't support) is an `InvalidDataException` with the file and the column in the message, and the original exception inside.

`DataLakeOptions`:

| Option | Default | |
|---|---|---|
| `Storage` | local disk | an `ILakeStorage` for other locations: `S3Storage`, `AzureBlobStorage`, or your own |
| `ColumnNaming` | none | `JsonNamingPolicy` for property names without `[DataMember]`, e.g. `JsonNamingPolicy.SnakeCaseLower` for `created_at` |

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
var rows = DataLake.Open<Order>(location)
    .Where(o => o.Date >= from && o.Country == "PL")
    .Where(o => o.Amount > 100m);
```

`Where` calls are combined with `&&`. Every filter is applied to every row, so results are always right. On top of that, comparisons of a property with a value skip data before reading it:

* **files**, by partition values from the path,
* **row groups** (Parquet), by min/max and null counts in the file. Text statistics are used when both ends are ASCII: their byte order, the one older writers got wrong for other characters, and .NET's ordinal order are then the same.

What skips: `==`, `!=`, `<`, `<=`, `>`, `>=` against a constant or a captured variable, `&&`, `||`, `!`, `list.Contains(x.P)` and `array.Contains(x.P)`, a bool property alone. Anything else (`x.Name.StartsWith("A")`, `x.A + x.B > 5`) still filters, row by row, and does not stop the parts around it from skipping.

Nulls work like in C#: `x.Amount > 5` is false for null, `x.Amount != 5` is true for null, `x.Amount == null` matches nulls.

What a read did:

```csharp
var metrics = new ScanMetrics();
var rows = await dataset.ReadAsync(metrics).ToListAsync();
// files 90 (skipped by partition 89), row groups 2 (skipped by statistics 0), rows read 10000, returned 10000, bytes read 61442
```

## Avro files

Avro object container files in a dataset are read through AvroConvert and mapped with the same rules as Parquet: names, nullability, partitions, types (`timestamp-*` is UTC, `local-timestamp-*` has no zone, `date`, `time-*`, `decimal`, `uuid`, enums by symbol name, records, arrays, maps).

Avro has no statistics, so only partitions skip files. AvroConvert comes with DataLake under the DataLake licence, so it doesn't write its own licence notice. A licence you declare for AvroConvert itself (`AvroConvert.License`) is still checked by AvroConvert.

## Arrow files

Arrow IPC files (Feather v2) in a dataset are read batch by batch with the same mapping as Parquet. They have no statistics, so only partitions skip files. For a single file or stream outside a dataset, see [Apache Arrow and ADBC](#apache-arrow-and-adbc).

## Write a dataset

```csharp
var orders = DataLake.Open<Order>("s3://shop/orders/", options);

await orders.WriteAsync(rows, new DatasetWriteOptions
{
    PartitionBy = [nameof(Order.Date), nameof(Order.Country)],   // s3://shop/orders/Date=2026-10-29/Country=PL/part-00001-....snappy.parquet
    Mode = DatasetWriteMode.OverwritePartitions,
});
```

`rows` is an `IEnumerable<T>` or `IAsyncEnumerable<T>`. Every mapped property is a column, named like on read (`[DataMember]`, `ColumnNaming`), so `ReadAsync` reads the files back into the same class. The result lists the files written and deleted.

`DatasetWriteOptions`:

| Option | Default | |
|---|---|---|
| `PartitionBy` | none | properties that become folders, in this order. Strings, integers, `bool`, `DateOnly`, `Guid`, enums. Values are in the path, not in the files |
| `Mode` | `ErrorIfExists` | `ErrorIfExists`: fail if the location has data files. `Append`: add files. `OverwritePartitions`: replace the partitions this write has rows for, keep the others (without `PartitionBy`, the whole dataset) |
| `Compression` | `Snappy` | `Snappy`, `Zstd`, `Gzip`, `None` |
| `RowGroupSize` | 131 072 rows | each partition being written keeps this many rows in memory |
| `TargetFileSize` | 512 MiB | a file is closed after the row group that crosses it |

Types written, chosen so Spark, Trino, DuckDB, pyarrow and Iceberg read them as what they are:

| Property | Column |
|---|---|
| `bool`, integers, `float`, `double` | the same type (`ushort` as UINT_16, ...) |
| `decimal` | decimal(38, 18), or `[DecimalPrecision(10, 2)]`. A value with more fractional digits or too many integer digits is an error, never rounded |
| `string`, enum | string (enums by name) |
| `byte[]` | binary |
| `Guid` | UUID |
| `DateOnly` | date |
| `TimeOnly`, `TimeSpan` (a time of day) | time(micros), not adjusted to UTC |
| `DateTimeZone`, `DateTimeOffset`, `DateTime` (`Kind.Utc` or `Local`) | timestamp(micros, UTC). A `DateTime` with `Kind.Unspecified` is an error: the instant is unknown |
| `UnzonedDateTime` | timestamp(micros) without a zone |
| class, list, array, dictionary | struct, list, map, any depth. A class can't contain itself |

* Files are written to a local temporary file and uploaded with `WriteMode.CreateNew`, so a write never replaces a file it did not create, and readers never see half a file.
* Partition values are escaped like Hive does (`a/b` is `a%2Fb`); null and the empty string are `__HIVE_DEFAULT_PARTITION__`, which reads back as null.
* `OverwritePartitions` deletes the old files after the new ones are written, so a reader in between sees both. A table format makes that atomic; that is what Iceberg writes in 1.0 are for.
* Timestamps are stored in microseconds, so the last digit of a .NET tick (100 ns) is dropped.
* Parquet.Net 6.1 writes min/max of microsecond timestamps in milliseconds, which makes engines that prune by statistics skip matching rows. DataLake fixes them in the footer and the page headers of every file it writes, and checks that DuckDB and pyarrow filter them correctly.

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

Samples for Databricks, Snowflake and Flight SQL: [samples/SolTechnology.DataLake.Samples.Adbc](../samples/SolTechnology.DataLake.Samples.Adbc/README.md).

* Read: integers, floats, half floats, booleans, (large / view) strings and binaries, decimal32 to decimal256 (into `decimal` when it fits), UUID, date32/64, time32/64, timestamps in any unit (with a time zone: an instant; without: wall-clock), durations, lists (also large and fixed size), structs, maps, dictionary-encoded columns.
* Written: the Parquet types above, as Arrow types: timestamp(us, "UTC") / timestamp(us), decimal128, the `arrow.uuid` extension, time64(us), `TimeSpan` as duration(us).
* `ArrowOptions`: `ColumnNaming`, and `BatchSize` (rows per batch when writing IPC, 65 536).

## Storage: S3

```csharp
var s3 = new AmazonS3Client();   // credentials and region from the default AWS chain
var dataset = DataLake.Open<Order>("s3://shop/orders/", new DataLakeOptions { Storage = new S3Storage(s3) });
```

* `s3://` and `s3a://` locations.
* The location is a folder: `s3://shop/orders` does not include `s3://shop/orders_old/`.
* Files are read with ranged GETs: the Parquet footer, then only the column chunks of row groups that are not skipped.
* S3-compatible storage works through the client: `new AmazonS3Client(new AmazonS3Config { ServiceURL = "http://localhost:9000", ForcePathStyle = true })` for MinIO, the account endpoint for Cloudflare R2.

## Storage: Azure, ADLS Gen2, OneLake

```csharp
var blobs = new BlobServiceClient(new Uri("https://shop.blob.core.windows.net"), new DefaultAzureCredential());
var dataset = DataLake.Open<Order>("abfss://lake@shop.dfs.core.windows.net/orders/", new DataLakeOptions { Storage = new AzureBlobStorage(blobs) });
```

`AzureBlobStorage` uses the Blob API, which ADLS Gen2 and OneLake also serve.

| Location | |
|---|---|
| `abfss://container@account.dfs.core.windows.net/path` | ADLS Gen2 (also `abfs://`) |
| `wasbs://container@account.blob.core.windows.net/path` | Blob Storage (also `wasb://`) |
| `https://account.blob.core.windows.net/container/path` | either |
| `abfss://workspace@onelake.dfs.fabric.microsoft.com/item.Lakehouse/Files/path` | OneLake; the client points to `https://onelake.blob.fabric.microsoft.com` |
| `az://container/path` | the account the client points to, e.g. Azurite |

A location in another account than the client is an error. ADLS directory markers are skipped.

## Storage: writes

Every storage writes and deletes single files. [Dataset writes](#write-a-dataset) use this, and Iceberg tables will; it is also the place to put your own files.

```csharp
ILakeStorage storage = new S3Storage(s3);
await using var parquet = File.OpenRead("part-00000.parquet");
var file = await storage.WriteAsync("s3://shop/orders/date=2026-10-01/part-00000.parquet", parquet);

await storage.WriteAsync("s3://shop/tables/orders/v2.metadata.json", metadata, WriteMode.CreateNew); // LakeFileExistsException if it exists
await storage.DeleteAsync("s3://shop/orders/date=2026-10-01/part-00000.parquet");                 // no error if it doesn't
```

* The stream is read from its current position to the end. It does not have to be seekable and is not disposed.
* Readers see the old file or the whole new one, never a part. Local disk writes a hidden temporary file next to the target (datasets skip it) and renames it; S3 and Azure commit the upload at the end.
* `WriteMode.CreateNew` checks and writes in one step, so of two writers creating the same file exactly one wins and the other gets `LakeFileExistsException`. S3 sends `If-None-Match: *` (Amazon S3, MinIO, R2; a store that ignores the header overwrites), Azure the same condition, local disk a hard link (a file system without hard links, such as FAT, falls back to a check and a move).
* S3 sends up to 8 MiB in one PUT and more as a multipart upload in 8 MiB parts, so files up to 80 GB never sit in memory whole. A failed upload is aborted.
* Azure creates the missing directories of ADLS Gen2 and OneLake.
* Your own `ILakeStorage` can stay read-only: `WriteAsync` and `DeleteAsync` have default implementations that throw `NotSupportedException`.

## Telemetry

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(DataLakeTelemetry.Name))
    .WithMetrics(m => m.AddMeter(DataLakeTelemetry.Name));
```

* Activity `datalake.scan` per read, tagged with `datalake.location`, `datalake.row_type`, `datalake.filter`, and what was skipped and read (`datalake.files`, `datalake.files.skipped`, `datalake.row_groups`, `datalake.row_groups.skipped`, `datalake.rows.read`, `datalake.rows.returned`, `datalake.bytes.read`).
* Counters: `datalake.scan.files`, `datalake.scan.files.skipped`, `datalake.scan.row_groups.skipped`, `datalake.scan.rows.read`, `datalake.scan.bytes.read`.

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

Credentials come from the environment, like the cloud CLIs: the default AWS chain for `s3://` (`--s3-endpoint` for MinIO), `--azure-connection-string`, `AZURE_STORAGE_CONNECTION_STRING` or `DefaultAzureCredential` for Azure. Exit code 2 on errors.

## Limits of 0.2

* Iceberg tables come in 0.3 (read) and 1.0 (write), Delta Lake later. Dataset writes are plain Parquet files: no transactions, no schema checks against files already there.
* Writes produce Parquet only; Avro and Arrow files are read, not written into datasets (`ArrowConvert.WriteIpcAsync` writes single Arrow files).
* Nested columns in legacy layouts (repeated fields without a LIST group, two-level lists) and into `object` or `Dictionary<string, object?>` still go value by value, slower than typed ones.
* Parquet.Net 6.1 writes the min/max statistics of microsecond and nanosecond timestamps in milliseconds. Those statistics are not used for files written by Parquet.Net directly, so such filters read the row groups instead of skipping them. Files DataLake writes have them fixed; files from Spark, PyIceberg, pyarrow or Trino are fine.
* A location without data files reads zero rows. On local disk a folder that does not exist is an error.
* Tested against Silo (a MinIO fork) and Azurite in Docker. ADLS Gen2, OneLake and R2 use the same APIs but were not tested against the real services yet.
* The ADBC samples compile in CI but run only against your own accounts.

## Compatibility

Every build reads 114 files from [apache/parquet-testing](https://github.com/apache/parquet-testing) and the Parquet.Net test data (Spark, parquet-mr, Impala, Hive, pyarrow, DuckDB, Rust and protobuf writers) and compares row counts and the first 20 rows with pyarrow. 92 match. pyarrow can't read 6 of them either (DataLake reads 3). The other 16:

* FLOAT16 columns, the Hadoop LZ4 framing of old parquet-mr, a MAP without values, a repeated group with several fields and no LIST annotation, and a few page layouts fail inside Parquet.Net 6.1;
* decimals with more than 28 digits don't fit `System.Decimal`;
* `large_string_map.brotli.parquet` (strings over 2 GB): after it fails, Parquet.Net returns corrupted strings for other files in the same process. Don't read files like that next to real work until it is fixed upstream.

The list with reasons is in [KnownGaps.cs](../tests/SolTechnology.DataLake.CompatibilityTests/KnownGaps.cs).
