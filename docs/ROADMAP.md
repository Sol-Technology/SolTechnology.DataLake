# Roadmap

Milestones in order. No dates: this is a side project and AvroConvert 4.0 comes first. Each step will be a GitHub issue in its milestone.

## Spike

A throwaway prototype that answers the hard questions before the real code. Lives in `spikes/`, does not ship.

- [x] Local test environment: MinIO + Lakekeeper (REST catalog) in Docker, PyIceberg or Spark to create test tables
- [x] Test tables: partitioned, after `MERGE` (with delete files), after a schema change. Delete files from Trino `DELETE` / `UPDATE`
- [x] Read `metadata.json` → snapshot → manifest list → manifests (Avro, through AvroConvert)
- [x] Read data files with Parquet.Net: column projection, skipping files by manifest statistics
- [x] REST catalog: `loadTable`, OAuth2 client credentials, credentials vended by the catalog (Lakekeeper with Keycloak)
- [x] Measure the column layer: one mapper over Parquet.Net columns and Arrow vs everything through Arrow. Through Arrow is 1.8× slower and allocates 1.48× more (`spikes/IcebergSpike.Benchmarks`)
- [x] Writing Parquet with `field_id`: Parquet.Net change, ParquetSharp or own writer. Answer: Parquet.Net 6.1 writes it (`DataField.FieldId`), checked with pyarrow
- [x] Which managed catalogs accept writes from an outside client (Unity, Snowflake, Glue, S3 Tables, Polaris). All of them, with limits: see the spike report
- [x] Bucket transform (Murmur3) against the test values from the Iceberg spec
- [x] Spike report: [spikes/REPORT.md](../spikes/REPORT.md)

## 0.1 Dataset ✔

A folder of Parquet or Avro files read as one table.

- [x] Storage abstraction: list and range read; local disk
- [x] `SolTechnology.DataLake.Aws`: S3 and S3-compatible (MinIO, R2)
- [x] `SolTechnology.DataLake.Azure`: Blob Storage, ADLS Gen2 and OneLake through the Blob API
- [x] Parquet read: projection, row group statistics decoded by logical type, nested types (struct, list, map, any depth)
- [x] Avro container read through AvroConvert, same mapping rules as Parquet
- [x] Column layer and compiled mapper from columns to C# classes
- [x] Type mapping: decimals, `DateOnly`, `TimeOnly`, timestamps through SolTechnology.DateTimeZone, `Guid`, nullability, widening, enums
- [x] Discovery: hive partitions (`date=…/country=…`), partition types from the class
- [x] Schema merge across files, clear error on a type conflict
- [x] Filters: LINQ to predicate, pruning by partition, by row group statistics, then by row
- [x] Metrics: `ScanMetrics` per read, `Meter` and `ActivitySource` for OpenTelemetry
- [x] CLI `datalake`: `ls`, `schema`, `head`
- [x] Benchmark vs Parquet.Net file by file
- [x] Docs: [Documentation.md](Documentation.md), S3 and ADLS examples
- [x] Publish workflow to nuget.org

## 0.2 Arrow ✔

Writes, and Arrow results in your classes.

- [x] Storage writes: write, conditional write, delete (local, S3, Azure)
- [x] Parquet write: row group and file size, Snappy/Zstd, statistics
- [x] Partitioned dataset write: `PartitionBy`, modes append / overwrite partitions / error if exists
- [x] Public API: Arrow `RecordBatch` to C# classes and back
- [x] Arrow IPC file and stream
- [x] ADBC samples: Databricks SQL, Snowflake, Flight SQL
- [x] Interop tests: DuckDB reads what we write
- [x] CLI: `cat --format json`, `stats`
- [x] Benchmark vs `pyarrow.dataset` on the same data
- [x] Faster nested columns (compiled per type, like flat ones)
- [x] `LakeContext` with a provider per context, `IDataset<T>`, `UseInMemory()` for tests ([design](design/lake-context.md))
- [x] Packages split: core with Parquet, `UseFiles` and `UseInMemory`; Arrow and Avro as format plug-ins in their own packages (`f.Arrow()`, `f.Avro()`); MIT for all

## 0.3 Iceberg read

Design and steps: [design/iceberg-read.md](design/iceberg-read.md) (draft, to be reviewed).

- [ ] REST catalog client: namespaces, tables, `loadTable`, OAuth2, SigV4 (AWS Glue, S3 Tables), vended credentials
- [ ] Table straight from a `metadata.json` path
- [ ] Metadata model for spec v1 and v2
- [ ] Scan planning: filters on manifest statistics, partition transforms (identity, bucket, truncate, year/month/day/hour)
- [ ] Projection by field id, schema evolution, partition evolution
- [ ] Time travel: snapshot id, timestamp, tag, branch
- [ ] Positional delete files
- [ ] Equality delete files
- [ ] Clear error for what is not supported yet (spec v3 features, encryption)
- [ ] Interop fixtures from Spark / PyIceberg in CI
- [ ] CLI: `table show`, `snapshots`, `files`, `history`

## 1.0 Iceberg write

- [ ] Parquet with `field_id` and column statistics for manifests
- [ ] `CreateTable<T>` with a partition spec from the class
- [ ] Append: data files, manifest and manifest list (Avro), new snapshot
- [ ] Commit with requirements, retry on conflict
- [ ] Overwrite partitions, overwrite by filter
- [ ] Schema evolution from the class: new columns, wider types
- [ ] Bucket transform matching Java bit for bit
- [ ] Batching small writes; sample: Kafka to Iceberg with SolTechnology.Avro.Kafka
- [ ] Interop: Spark, Trino, Snowflake and DuckDB read our tables
- [ ] Docs: moving from PyIceberg in a subprocess

## Later

Depends on what users ask for.

- [ ] Iceberg v3: deletion vectors, `timestamp_ns`, default values
- [ ] Upsert and delete through equality delete files
- [ ] Name mapping for files without field ids (tables migrated from Hive)
- [ ] Delta Lake read
- [ ] Delta Lake write
- [ ] Google Cloud Storage
- [ ] SQL over a context's datasets through DuckDB, `SqlQueryAsync<T>(FormattableString)` (design: [design/lake-context.md](design/lake-context.md), "Deferred: SQL queries")
