# Files provider

A folder of files per dataset under a root, partitioned hive-style (`root/orders/date=2026-10-29/country=PL/`), without transactions. Writes are Parquet. Reads take Parquet, and Avro and Arrow IPC files when their package is added. What Spark, Trino, DuckDB, pyarrow and Databricks read and write as a "Parquet table" without a table format.

```csharp
services.AddDataLake<IShopLake, ShopLake>((sp, o) => o.UseFiles("s3://shop/", new S3Storage(sp.GetRequiredService<IAmazonS3>()), f => f
    .Parquet(p => p.Compression(ParquetCompression.Zstd).RowGroupSize(128 * 1024).TargetFileSize(512L << 20))
    .Avro()));
```

* [Options](#options)
* [Formats](#formats)
* [What it can do](#what-it-can-do)
* [Files](#files)
* [Writes](#writes)
* [Storage: S3](#storage-s3)
* [Storage: Azure, ADLS Gen2, OneLake](#storage-azure-adls-gen2-onelake)
* [Storage: writes](#storage-writes)

## Options

`UseFiles(root, storage, configure)`: `root` is a local path, `file://` or a URI the storage handles; `storage` is needed for anything but a local path. `configure` gets a `FilesOptions`.

| `FilesOptions` | |
|---|---|
| `Parquet(p => ...)` | how Parquet files are written, below |
| `Avro()` | read `.avro` files; from SolTechnology.DataLake.Avro |
| `Arrow()` | read `.arrow`, `.feather`, `.ipc`, `.arrows` files; from SolTechnology.DataLake.Arrow |

| `ParquetOptions` | Default | |
|---|---|---|
| `Compression` | `Snappy` | `Snappy`, `Zstd`, `Gzip`, `None` |
| `RowGroupSize` | 131 072 rows | each partition being written keeps this many rows in memory |
| `TargetFileSize` | 512 MiB | a file is closed after the row group that crosses it |

## Formats

| Format | Files | Package | In `UseFiles` |
|---|---|---|---|
| Parquet | `.parquet`, `.parq` | SolTechnology.DataLake | built in |
| Avro | `.avro` | SolTechnology.DataLake.Avro | `f => f.Avro()` |
| Arrow IPC | `.arrow`, `.feather`, `.ipc`, `.arrows` | SolTechnology.DataLake.Arrow | `f => f.Arrow()` |

* A `.avro` or Arrow file in a dataset without its format added is a `NotSupportedException` that names the package and the method to call. It is not skipped, so rows are never missing without a word.
* Other files (`README.md`, `.csv`) are left out.
* `Dataset.Open` takes the same: `new DataLakeOptions { Files = f => f.Avro() }`.

## What it can do

| | |
|---|---|
| `ReadAsync`, `Where` | yes; filters skip files by partition and row groups by statistics |
| `CountAsync` | without a filter, sums the row counts in Parquet footers (no data read; Avro and Arrow files are counted row by row); with a filter, reads |
| `AppendAsync` | yes |
| `OverwriteAsync(rows)`, `ReplacePartitionsAsync` | yes, not atomic: old files are deleted after the new ones are written |
| `OverwriteAsync(rows, where)`, `DeleteAsync(where)` | only filters on partition columns: whole files are deleted, rows inside files are never rewritten. Anything else (a non-partition column, `StartsWith`) throws `NotSupportedException` |
| time travel | no |

* Before writing, the provider checks the folders it finds against the declared partitions (names case-insensitive, so Spark's `date=` is `Date`). Files partitioned differently stop the write.
* A dataset that does not exist reads as empty, on every storage.

## Files

A dataset is every file under its folder in one of its [formats](#formats).

* Files are read in path order, row groups in file order.
* Hidden files are left out: names starting with `_` or `.` (`_SUCCESS`, `.part-0.parquet.crc`, `_temporary/`), unless they are a partition (`_x=1/`).
* A folder named like a file (Spark's `df.write.parquet("orders.parquet")`) is still read as a folder. A single file has no partitions, as partitions come from the path under the opened location.
* Nothing is read until you enumerate `ReadAsync()`. Opening the same dataset again reads the files again; there is no cache.
* A file that can't be read (corrupt, or a feature Parquet.Net doesn't support) is an `InvalidDataException` with the file and the column in the message, and the original exception inside.

Avro files are read through AvroConvert and mapped with the same rules as Parquet: names, nullability, partitions, types (`timestamp-*` is UTC, `local-timestamp-*` has no zone, `date`, `time-*`, `decimal`, `uuid`, enums by symbol name, records, arrays, maps). Avro has no statistics, so only partitions skip files. AvroConvert has its own licence, and without `AvroConvert.License` declared it prints its licence notice, as for any AvroConvert user.

Arrow IPC files (Feather v2) and streams are read batch by batch with the same mapping. They have no statistics either. Writes produce Parquet only.

## Writes

Files are named `part-00001-<write id>.snappy.parquet`, in a folder per partition.

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

* Files are written to a temporary file (memory for `UseInMemory`) and uploaded with `WriteMode.CreateNew`, so a write never replaces a file it did not create, and readers never see half a file.
* Partition values are escaped like Hive does (`a/b` is `a%2Fb`); null and the empty string are `__HIVE_DEFAULT_PARTITION__`, which reads back as null.
* Overwrites delete the old files after the new ones are written, so a reader in between sees both. A table format makes that atomic; that is what Iceberg writes in 1.0 are for.
* Timestamps are stored in microseconds, so the last digit of a .NET tick (100 ns) is dropped.
* Parquet.Net 6.1 writes min/max of microsecond timestamps in milliseconds, which makes engines that prune by statistics skip matching rows. DataLake fixes them in the footer and the page headers of every file it writes, and checks that DuckDB and pyarrow filter them correctly.


## Storage: S3

```csharp
var s3 = new AmazonS3Client();   // credentials and region from the default AWS chain
services.AddDataLake<ShopLake>(o => o.UseFiles("s3://shop/", new S3Storage(s3)));
```

* `s3://` and `s3a://` locations.
* The location is a folder: `s3://shop/orders` does not include `s3://shop/orders_old/`.
* Files are read with ranged GETs: the Parquet footer, then only the column chunks of row groups that are not skipped.
* S3-compatible storage works through the client: `new AmazonS3Client(new AmazonS3Config { ServiceURL = "http://localhost:9000", ForcePathStyle = true })` for MinIO, the account endpoint for Cloudflare R2.

## Storage: Azure, ADLS Gen2, OneLake

```csharp
var blobs = new BlobServiceClient(new Uri("https://shop.blob.core.windows.net"), new DefaultAzureCredential());
services.AddDataLake<ShopLake>(o => o.UseFiles("abfss://lake@shop.dfs.core.windows.net/", new AzureBlobStorage(blobs)));
```

`AzureBlobStorage` uses the Blob API, which ADLS Gen2 and OneLake also serve.

| Location | |
|---|---|
| `abfss://container@account.dfs.core.windows.net/path` | ADLS Gen2 (also `abfs://`) |
| `wasbs://container@account.blob.core.windows.net/path` | Blob Storage (also `wasb://`) |
| `https://account.blob.core.windows.net/container/path` | either; also Azure DNS zone endpoints, `https://account.z12.blob.storage.azure.net/container/path` |
| `abfss://workspace@onelake.dfs.fabric.microsoft.com/item.Lakehouse/Files/path` | OneLake; the client points to `https://onelake.blob.fabric.microsoft.com` |
| `az://container/path` | the account the client points to, e.g. Azurite |

A location in another account than the client is an error. ADLS directory markers are skipped.

## Storage: writes

Every storage writes and deletes single files. [Dataset writes](#writes) use this, and Iceberg tables will; it is also the place to put your own files.

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

