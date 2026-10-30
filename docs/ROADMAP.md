# Roadmap

Milestones in order. No dates: this is a side project and AvroConvert 4.0 comes first. Each step will be a GitHub issue in its milestone.

## Spike

A throwaway prototype that answers the hard questions before the real code. Lives in `spikes/`, does not ship.

- [x] Local test environment: MinIO + Lakekeeper (REST catalog) in Docker, PyIceberg or Spark to create test tables
- [ ] Test tables: partitioned, after `MERGE` (with delete files), after a schema change
- [x] Read `metadata.json` → snapshot → manifest list → manifests (Avro, through AvroConvert)
- [x] Read data files with Parquet.Net: column projection, skipping files by manifest statistics
- [ ] REST catalog: `loadTable`, OAuth2 client credentials, credentials vended by the catalog. Done: `loadTable` and vended S3 credentials (Lakekeeper). Left: OAuth2
- [ ] Measure the column layer: one mapper over Parquet.Net columns and Arrow vs everything through Arrow
- [x] Writing Parquet with `field_id`: Parquet.Net change, ParquetSharp or own writer. Answer: Parquet.Net 6.1 writes it (`DataField.FieldId`), checked with pyarrow
- [ ] Which managed catalogs accept writes from an outside client (Unity, Snowflake, Glue, S3 Tables, Polaris)
- [x] Bucket transform (Murmur3) against the test values from the Iceberg spec
- [ ] Spike report

## 0.1 Dataset

A folder of Parquet or Avro files read as one table.

- [ ] Storage abstraction: list, range read, write, conditional write, delete; local file system
- [ ] `SolTechnology.DataLake.Aws`: S3 and S3-compatible (MinIO, R2)
- [ ] `SolTechnology.DataLake.Azure`: ADLS Gen2 and OneLake
- [ ] Parquet read: projection, row group statistics, logical types, nested types
- [ ] Avro container read through AvroConvert
- [ ] Column layer and compiled mapper from columns to C# classes
- [ ] Type mapping: decimals, `DateOnly`, `TimeOnly`, timestamps through SolTechnology.DateTimeZone, `Guid`, nullability
- [ ] Discovery: hive partitions (`date=…/country=…`), partition types from the class
- [ ] Schema merge across files, clear error on a type conflict
- [ ] Filters: LINQ to predicate, pruning by partition, by file and row group statistics, then by row
- [ ] Metrics: files and bytes read and skipped (`Meter`, `ActivitySource`)
- [ ] CLI `datalake`: `ls`, `schema`, `head`
- [ ] Benchmarks: vs Parquet.Net file by file, vs `pyarrow.dataset`
- [ ] Docs and sample: read a partitioned dataset from S3 and ADLS
- [ ] Publish workflow to nuget.org

## 0.2 Arrow

Writes, and Arrow results in your classes.

- [ ] Parquet write: row group and file size, Snappy/Zstd, statistics
- [ ] Partitioned dataset write: `PartitionBy`, modes append / overwrite partitions / error if exists
- [ ] Public API: Arrow `RecordBatch` to C# classes and back
- [ ] Arrow IPC file and stream
- [ ] ADBC samples: Databricks SQL, Snowflake, Flight SQL
- [ ] Interop tests: DuckDB reads what we write
- [ ] CLI: `cat --format json`, `stats`

## 0.3 Iceberg read

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
