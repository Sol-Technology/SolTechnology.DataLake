# Spike report

30 October 2026. The questions come from the Spike milestone in [docs/ROADMAP.md](../docs/ROADMAP.md). Code: `spikes/IcebergSpike` (reader), `spikes/IcebergSpike.Tests` (file fixtures), `tests/SolTechnology.DataLake.IntegrationTests` (Docker), `spikes/IcebergSpike.Benchmarks`.

## Answers

| Question | Answer | Evidence |
|---|---|---|
| Can we read Iceberg tables in .NET with what we have? | Yes. `metadata.json` → manifest list → manifests through AvroConvert, data files through Parquet.Net, columns matched by field id. | Every snapshot of 4 tables written by PyIceberg reads row for row like PyIceberg reads it: time travel, renamed, added and dropped columns |
| Can filters skip data before reading it? | Yes, on three levels. | Country + one day: 8 of 9 manifests skipped from the manifest list alone, 1 of 9 files read. Filter on a non-partition column: 8 of 9 files skipped by column bounds. Bucket column: 1 of 4 files read |
| REST catalog, OAuth2, vended credentials? | Works with Lakekeeper behind Keycloak: client credentials token, `loadTable`, short-lived S3 credentials from STS, files read from S3. A request without a token gets 401. | Integration tests, also on CI |
| Delete files? | Position deletes work, applied by sequence number. Equality deletes fail with a clear error for now. | Table changed by Trino `DELETE` / `UPDATE`: 3 position deletes, rows match PyIceberg |
| Can Parquet.Net write the field ids Iceberg needs (D1)? | Yes. `DataField.FieldId` is written; pyarrow sees `PARQUET:field_id`. No need for ParquetSharp or our own writer. | `WriteFieldIdTests` + pyarrow check |
| Parquet to classes: column layer or Arrow (D2)? | Column layer. Through Arrow is 1.8× slower and allocates 1.48× more, because with Parquet.Net Arrow is always an extra copy. | [Benchmark](IcebergSpike.Benchmarks/README.md): 415 ms vs 747 ms for 1M rows |
| Bucket transform compatible with Java? | Yes. | All Murmur3 test values from the spec; every row in PyIceberg's `id_bucket=N` files hashes to N |
| Do managed catalogs accept writes from an outside client? | Yes, from all big ones, with limits. See below. | Vendor docs, 30 October 2026 |

## Managed catalogs, writes from outside

| Catalog | Writes | Auth | Note |
|---|---|---|---|
| Databricks Unity | managed Iceberg tables; Delta tables only read through Iceberg | OAuth2 M2M or PAT | needs "External data access" and `EXTERNAL USE SCHEMA` |
| Snowflake Horizon | Snowflake-managed Iceberg v2 and v3 tables | Snowflake | |
| AWS Glue | yes | SigV4, or OAuth2 through token exchange | covers S3 Tables too |
| S3 Tables | yes | SigV4 | AWS recommends Glue instead |
| Microsoft OneLake | native Iceberg tables; Delta tables only read through Iceberg | Entra ID | |
| Google Lakehouse (BigLake) | docs say "read, manage"; to confirm | Google OAuth | v2 tables only |
| Polaris, Lakekeeper | yes | OAuth2 | Lakekeeper tested here |

What it changes in the plan:
- teams on Databricks or Fabric with **Delta** tables can't write through Iceberg; for them Delta support is what makes writes possible
- `SolTechnology.DataLake.Aws` has to sign catalog requests with SigV4 (Glue, S3 Tables), on top of S3 requests
- Snowflake writes v3 tables, so deletion vectors are needed earlier than planned

## Found on the way

AvroConvert (4.0.0-preview.3):
- an Iceberg manifest needs keys like `schema`, `partition-spec` and `format-version` in the Avro file header; AvroConvert can't write custom header metadata yet, so writing tables waits for [AvroConvert#196](https://github.com/AdrianStrugala/AvroConvert/issues/196)
- a record can't be read into `IDictionary<string, object>`, `dynamic` works: [AvroConvert#197](https://github.com/AdrianStrugala/AvroConvert/issues/197)
- used inside DataLake, AvroConvert prints its own licence notice to the DataLake user; to fix in AvroConvert before DataLake 0.1
- everything else worked as is: `IAsyncEnumerable<T>` from a stream, `[DataMember(Name = "manifest_path")]`, unions with null, `Dictionary<int, byte[]>` from key-value arrays (`lower_bounds`), `DateOnly` from `date`

Parquet.Net 6.1:
- on read `DataField.FieldId` is -1; the value is in `SchemaElement.FieldId`
- `DateTime` comes back with `Kind.Unspecified`, also for UTC timestamps
- decimals are written as `decimal(38, 18)` unless the precision is set; Iceberg tables need the table's precision
- **min/max statistics of `MICROS` timestamps are written in milliseconds** (the column says microseconds; pyarrow reads the max of a 2026 column as 1970-01-21). Any engine that prunes by statistics can drop matching rows from such files. DataLake does not trust timestamp statistics in micros or nanos from files written by Parquet.Net, and has to write millis or its own statistics in 0.2. To report upstream
- row group statistics come back as physical values: a timestamp as `long` in the column's unit, a date as days since 1970-01-01, a decimal as `BigDecimal`
- timestamps without a zone (`isAdjustedToUTC=false`) come back as `Kind.Local`, which they are not; DataLake returns them as `Unspecified`
- a required column read into `T?[]` sometimes came back with nulls (about 1 scan in 50 in the integration tests, never in a standalone loop over the same file). Reading required columns into `T[]` made it go away (0 in 600 scans). DataLake does that; worth reporting upstream once it reproduces alone

Catalogs:
- Lakekeeper writes gzip-compressed metadata (`*.gz.metadata.json`)
- vended credentials come in `config` (older) and in `storage-credentials` with a path prefix (newer); clients have to handle both
- the S3 endpoint a catalog hands out is the one the catalog sees (`http://silo:9000` in Docker); clients need a way to override it

## Not covered

- equality deletes (Flink writes them, Trino and PyIceberg don't)
- spec v3: deletion vectors, `timestamp_ns`, default values
- writing tables: only field ids in Parquet were checked; manifests, commits and concurrency are milestone 1.0
- real clouds: everything ran on Silo and Lakekeeper in Docker
- Spark as a writer (Trino and PyIceberg were enough for the cases above)

## Next

Milestone 0.1 Dataset: storage abstraction (local, S3, ADLS), Parquet and Avro reading, column layer with a compiled mapper, hive partitions, filters. The spike code stays in `spikes/` as a reference and is not moved to `src/` as it is.
