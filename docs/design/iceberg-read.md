# Iceberg read: `UseIceberg` for milestone 0.3

Status: reviewed on 24 November 2026 in a separate session, verdict **Go with changes**; every change is applied below (review log at the end). Next: step 1.

Sources: [ROADMAP.md](../ROADMAP.md) milestone 0.3, the [spike report](../../spikes/REPORT.md), [lake-context.md](lake-context.md) (D3 `InNamespace`, D5 storage per table, D6 capability matrix, D7 missing table reads as empty), the product plan in the licensing repo (`ops/docs/DataLakePlan.md` F6, D3, D6, D7), the [Iceberg table spec](https://iceberg.apache.org/spec/) and the [REST catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

## Goal

An Iceberg table is one more provider of the same `IDataset<T>`. Code that reads `lake.Orders` does not change when the context moves from `UseFiles` to `UseIceberg`.

```csharp
services.AddDataLake<IShopLake, ShopLake>((sp, o) => o
    .UseIceberg(IcebergCatalog.Rest(new Uri("https://catalog.example.com/catalog"), c => c
            .ClientCredentials("datalake", secret, tokenEndpoint: new Uri("https://login.example.com/token"))
            .Warehouse("shop")),
        i => i.Namespace("sales")
              .Storage(properties => S3Storage.FromProperties(properties))));   // credentials vended by the catalog

await foreach (var order in lake.Orders.Where(o => o.Date == yesterday).ReadAsync()) { }
await foreach (var order in lake.Orders.AsOf(LakeVersion.Time(lastMonday)).ReadAsync()) { }

// Without a catalog, read-only, like Dataset.Open:
var orders = IcebergTable.Open<Order>("s3://shop/sales/orders/metadata/00042-....metadata.json", new DataLakeOptions { Storage = s3 });
```

Demo for 0.3 (scenario S4 of the product plan): a table after a Spark `MERGE` (merge-on-read) reads row for row like Spark reads it, a filter skips manifests and files, time travel returns an older snapshot, and a table with equality deletes from Flink reads correctly.

Not in 0.3: writes (1.0), spec v3 and later (refused by name), name mapping for files without field ids, ORC and Avro data files, remote signing, read restrictions, views, `reportMetrics`.

## Design

### I1. Package, licence, CLI

- New package `SolTechnology.DataLake.Iceberg`. Licence as decided on 23 November: PolyForm Noncommercial and Small Business free, commercial through Polar. Iceberg code is never MIT.
- **Packaging.** `src/Directory.Build.props` sets MIT and packs the MIT `LICENSE.md` for every package today. It gets a property per package (`DataLakeLicence`, default `MIT`); `.Iceberg` and `.Cli` set `Iceberg`, which packs their own `LICENSE.md` (PolyForm + commercial, like AvroConvert's) through `PackageLicenseFile`, with no licence expression and no MIT file. A build target fails when a package that references `.Iceberg` resolves to MIT.
- **CLI.** One tool package, one licence (Adrian, 24 November): `SolTechnology.DataLake.Cli` moves to the Iceberg licence, file commands included, because a tool package carries every DLL it depends on.
- **Repository licence.** The root `LICENSE.md` is scoped: MIT for everything except `src/SolTechnology.DataLake.Iceberg` and `src/SolTechnology.DataLake.Cli`, which carry their own `LICENSE.md`.
- **The spike.** `spikes/IcebergSpike*` is public under MIT and stays MIT in the git history. It is removed from `master` in step 1 (Adrian, 24 November); the package code is written from the spec, with the spike as a reference read in history only. Its fixtures move to `tests/`.
- **`IcebergLicense`** follows `AvroLicense`: `NonCommercial`, `SmallBusiness`, `Commercial(key)`, environment variable `DATALAKE_ICEBERG_LICENSE`, the same signed key format, product `datalake-iceberg` in `products`. Informational only: never restricts.
- **One notice** (Adrian, 24 November): a user of the Iceberg package sees only the Iceberg notice, never AvroConvert's.
  - A declared Iceberg licence is passed to AvroConvert: the free licences as they are, a commercial key when it lists `avroconvert`. It happens in the `IcebergLicense` setter and, for the environment variable, before the first manifest is deserialized, because AvroConvert decides about its notice at its first call. An application that declared its own AvroConvert licence keeps it.
  - With nothing declared, the Iceberg package prints its notice and tells AvroConvert that the notice is handled. **AvroConvert 4.0.1 needs a public call for this** (proposal: `AvroLicense.NoticeHandledBy(string product)`); `LicenseCoveredBy` was removed on 23 November and is not coming back.
  - Key issuing (Worker): every Iceberg key also lists `avroconvert`.
- **Core access.** `InternalsVisibleTo("SolTechnology.DataLake.Iceberg")`, `UsesCoreInternals` (exact core version, as `.Arrow` and `.Avro`), `LakeContextOptionsBuilder.Use` becomes `internal`. `LakeProvider` stays internal.

### I2. Catalogs

```csharp
IcebergCatalog.Rest(Uri uri, Action<RestCatalogOptions>? configure = null)
IcebergCatalog.Folder(string root, ILakeStorage? storage = null, Action<FolderCatalogOptions>? configure = null)
```

REST, from the OpenAPI:

| Call | Used for |
|---|---|
| `GET /v1/config?warehouse=` | `prefix`, `defaults`, `overrides`, `namespace-separator` |
| `GET /v1/{prefix}/namespaces`, `.../tables` | CLI `ls`; not needed to read |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` | `loadTable`, with `X-Iceberg-Access-Delegation: vended-credentials` |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{table}/credentials` | fresh vended credentials, when the catalog has it |

- **Auth.** `ClientCredentials(id, secret, scope = "catalog", tokenEndpoint)`: `/v1/config` itself needs a token, so the endpoint comes from the client; the deprecated `/v1/oauth/tokens` only when none is given. `Token(Func<CancellationToken, ValueTask<string>>)` for Entra ID, Databricks PAT and the like. `Handler(HttpMessageHandler)` for anything else.
- **Refresh.** Before `expires_in` when it is given; one refresh at a time under concurrent calls; on 401 or 419 refresh and retry once.
- **SigV4** for AWS Glue and S3 Tables is a `DelegatingHandler` in `SolTechnology.DataLake.Aws` (MIT, it signs HTTP requests and knows nothing about Iceberg).
- **Missing tables.** A 404 with error type `NoSuchTableException` or `NoSuchNamespaceException` reads as empty (D7 of lake-context.md). Any other 404 (wrong prefix, warehouse or URI) throws with the URL.
- **Refused by name**: `read-restrictions` in `loadTable` (row filters, column masks must not be bypassed) and `s3.remote-signing-enabled=true`.
- **Namespaces.** The model's `InNamespace(...)` wins over `i.Namespace(...)`, which is the default for tables without one. Multi-level namespaces are joined with `namespace-separator` from config, else `%1F`.
- **Cache.** `loadTable` is cached per table for a short time (default 30 s, configurable); `AsOf` and a new context always see the latest metadata.
- **Folder catalog** (tests, local experiments, PyIceberg `SqlCatalog` on a folder): `version-hint.text` when present, else the metadata file with the highest numeric prefix, in either naming (`v12.metadata.json`, `00012-<uuid>.metadata.json`). `RemapLocation(from, to)` on the folder catalog and on `IcebergTable.Open` rewrites absolute locations (fixtures point to `file:///warehouse/...`).

### I3. Storage per table and per file

Iceberg paths are absolute (`s3://bucket/...`), data files can live under other prefixes than the table location, and a catalog may vend credentials per table (older catalogs in `config`, newer in `storage-credentials` with a path prefix).

```csharp
i.Storage(ILakeStorage storage)                                             // one storage for everything
i.Storage(Func<IReadOnlyDictionary<string, string>, ILakeStorage> create)   // from vended properties
i.Storage(Func<IcebergStorageRequest, ILakeStorage> resolve)                // full control: prefix + properties
```

- The storage is chosen **per file**, by the longest `storage-credentials` prefix matching the file's location, else from `config`.
- `S3Storage.FromProperties` in `.Aws` and `AzureBlobStorage.FromProperties` in `.Azure` read the standard keys (`s3.access-key-id`, `s3.session-token`, `s3.endpoint`, `s3.region`, `adls.sas-token.<account>`, ...). They know property names, not Iceberg, so they stay MIT.
- `i.EndpointOverride(Uri)`: the catalog hands out the endpoint it sees (`http://silo:9000` in Docker).
- **Expiry.** Vended credentials are refreshed before their expiry property (`s3.session-token-expires-at-ms` and the like) through the `credentials` endpoint, else through `loadTable`. Storages are cached per credential; an old one is disposed when no scan uses it any more.
- **Core change: open by path.** `ILakeStorage` lists and reads ranges; reading one known file today costs a LIST and a HEAD and needs list permission, which vended credentials may lack. Storage gets an operation that opens a path with a known length (`manifest_length`, `file_size_in_bytes`) and one that gets the length of a single path (`metadata.json`, manifest lists).

### I4. Table metadata

- `metadata.json` format versions 1 and 2, plain or gzip, detected by magic bytes, not by name. Schemas, partition specs, snapshots, `refs`, `snapshot-log`. Sort orders and statistics files are read and ignored.
- v1 legacy fields: a single `schema` and `partition-spec` instead of the lists, and a snapshot with `manifests` (a list of manifest paths) instead of `manifest-list`.
- `current-snapshot-id` missing or `-1` means no snapshot: the table reads as empty.
- Manifest lists and manifests through AvroConvert, v1 and v2 field sets (v1 has no `content` and no sequence numbers; v1 reads as data sequence number 0).
- Refused with a message that names the feature: any `format-version` above 2 (v3: deletion vectors, `timestamp_ns`, default values, row lineage), encryption (`encryption-keys`, `key_metadata`), ORC and Avro data files, files without field ids (name mapping, `schema.name-mapping.default`).

### I5. From a class to field ids

- A property maps to a column name as today (`[DataMember]`, `ColumnNaming`, case-insensitive).
- **Which schema.** The name is looked up in the table's **current** schema to get the field id, for the main branch, branches and time travel alike (Iceberg Java does the same for main and branches). For time travel to a snapshot id, a time or a tag, a name not found in the current schema is looked up in the snapshot's schema, so a column dropped since then still reads (Adrian, 24 November: a renamed column keeps its data; Spark would return null there). A snapshot without `schema-id` uses the current schema.
- Reading a file matches by **Parquet field id** (`SchemaElement.FieldId`; Parquet.Net's `DataField.FieldId` is -1 on read, spike report), never by name, so renamed columns keep working and an added column reads as null in old files.
- **Nested types** match by field id too: struct fields, list elements, map keys and values. Today they match by name (`ParquetFile.Bind`, `NestedColumns.cs` `NestedBuilder`, `TypedNested.cs`), so the resolver reaches both nested builders.
- A property with no field in either schema: null if it accepts null, else `InvalidOperationException` naming the table, the property and the schema id. A required table column the class does not have is not read.
- Type promotion allowed by the spec (`int` to `long`, `float` to `double`, wider decimal precision) goes through the existing widening conversions.
- Identity-partitioned columns missing from a data file (imported Hive tables) are filled from the file's partition tuple, as hive folder values are today.
- **Core change, made once in step 4:** `FileScan<T>` gets (1) a column resolver from property to Parquet field id, used by flat and nested bindings, and (2) a row-group hook that receives the position of the group's first row, can read extra columns by field id (equality delete keys the class has no property for), and returns which rows to keep. Position and equality deletes both use the hook. Without them the Parquet reader works as today, so `UseFiles` does not change.

### I6. Scan planning

Three levels, as in the spike: manifest list (partition field summaries), manifest entries (partition tuple), column metrics (`lower_bounds`, `upper_bounds`, `null_value_counts`, `value_counts`).

- The core does not project filters; it evaluates the LINQ filter (`Predicate`) to `Tri` over facts per property (`ColumnFacts`), which handles `Not` correctly. Each transform turns its partition values or summaries into facts about the source column:
  - `identity`: `Exact` or `Range`; `year`/`month`/`day`/`hour`: `Range` of the source column, with Java's correction for values before 1970 (`fixInclusiveTimeProjection`); `truncate` on numbers: `Range`.
  - New fact kinds: `Bucket(n, values)` answers `==` and `In` and is `Unknown` otherwise; `Prefix(text)` for `truncate` on strings, truncated by code points, not UTF-16 units.
  - `void` and unknown transforms give no facts (`Unknown`, never `False`).
- Bounds are decoded by the spec's single-value serialization and by length: a 4-byte bound of a promoted `long` column is an `int`, of a promoted `double` a `float`.
- Row groups are still skipped by Parquet statistics inside each file, and the residual filter runs on every row, so a fact that is too wide costs time, never rows.
- **Order and memory.** Delete manifests are read first (their entries are needed to scope deletes); data manifests are then streamed, one manifest at a time, and data files are read as they are planned, not after the whole plan.
- `ScanMetrics` gains `Manifests`, `ManifestsSkipped`, `FilesSkippedByStatistics`, `DeleteFiles` and `RowsDeleted`; `Files` and `FilesSkippedByPartition` keep their meaning.
- `CountAsync` without a filter and without delete files is the sum of `record_count` from the manifests: no data file is opened.

### I7. Delete files

All comparisons use the **data sequence number** (inherited from the manifest when the entry has none).

- **Position deletes** apply to a data file when the delete file's partition spec id and partition values equal the data file's (or its `referenced_data_file` names it), the path matches, and the data file's data sequence number is lower than or equal to the delete file's. Equal numbers happen when both were written in one commit. The `file_path` bounds in a delete file's metrics skip delete files that can't name the data file. The positions of one data file are a sorted array; skipped row groups still advance the position.
- **Equality deletes** apply when the data file's data sequence number is strictly lower, and the partition spec id and partition values are equal, or when the delete file was written with an unpartitioned spec (global). Key columns (`equality_ids`) are compared even when they were dropped from the current schema since; `null` equals `null`. Keys of all applicable delete files are held in memory; the docs give the cost and `RowsDeleted` shows it.
- A read never returns a row a delete file removed. Until step 8 is done, a table with equality delete files throws `NotSupportedException` naming the file.

### I8. Time travel

`AsOf` reads the table as it was at an earlier moment: one of its snapshots.

```csharp
IDatasetQuery<T> AsOf(LakeVersion version);

LakeVersion.Snapshot(long id)
LakeVersion.Time(DateTimeOffset at)      // the snapshot that was current at that moment, from snapshot-log
LakeVersion.Tag(string name)
LakeVersion.Branch(string name)
```

- On `IDatasetQuery<T>` (Adrian, 24 November), so `AsOf` and `Where` chain in any order and Delta can use the same call. The files providers throw `NotSupportedException` naming the provider (D6 of lake-context.md). Nothing is published yet, so the interface change is free; mocks with Moq or NSubstitute are not affected.
- Schema: I5.
- A time before the first snapshot, an unknown id, tag or branch: `InvalidOperationException` with the table and the earliest snapshot time.

### I9. Writes, errors, telemetry

- Every write method throws `NotSupportedException`: "Iceberg writes come in SolTechnology.DataLake 1.0".
- `EnsureCreatedAsync` does nothing when every declared table exists, so a startup call keeps working; it throws the same message when one is missing.
- `IcebergTable.Open<T>(path, DataLakeOptions)` takes the same options as `Dataset.Open<T>` (storage, `ColumnNaming`), plus `RemapLocation`.
- A catalog that answers 401 or 403 after the retry throws, with the catalog URI and the table.
- The `datalake.scan` activity gets `datalake.iceberg.snapshot_id` and `datalake.iceberg.schema_id`; catalog calls are `datalake.catalog` activities.

### I10. CLI

`datalake table show|snapshots|files|history <table> --catalog <uri>`, and the same for a `metadata.json` path, in `SolTechnology.DataLake.Cli`, which is under the Iceberg licence (I1).

## Risks

- **Parquet.Net.** Timestamp statistics in `MICROS` are written in milliseconds (spike): manifest bounds are used as written by the table's writer and never recomputed, and the existing `TrustsTimestamps` check stays for row groups. Decimals above 28 digits fail inside Parquet.Net, and Spark writes `decimal(38, x)`: such columns throw with the column name, and the docs say so. After a failed read its buffer pool can return damaged strings in the same process (compatibility corpus, 31 October): a failed file read is reported, and the upstream report is sent before 0.3 ships.
- **Memory.** Equality delete keys and position arrays of a large table. Measured on a fixture with 1M equality deletes before 0.3 ships; above a limit the read throws with the table and the count instead of running out of memory.
- **v3 sooner than planned.** Snowflake Horizon writes v3 (product plan D6). 0.3 refuses it by name; the refusal is a signal to count in conversations.
- **Fixtures.** Spark merge-on-read, Flink equality deletes and nested types need writers we don't run today: step 2.

## Steps

| # | Step | Done when |
|---|---|---|
| 1 | Licence and packaging (I1): `DataLakeLicence` property, `.Iceberg` and `.Cli` licence files, build check, scoped root `LICENSE.md`; spike removed from `master` (projects, CI step, solution, the integration tests that used it), fixtures moved to `tests/`; `IcebergLicense` and the notice; `InternalsVisibleTo`, internal `Use`, `UseIceberg` with a provider whose writes throw | a test opens the packed `.nupkg` of `.Iceberg` and `.Cli`: own licence file, no MIT file or expression, exact core dependency; `.Arrow` still MIT; a context with `UseIceberg(IcebergCatalog.Folder(...))` builds; licence tests copied from AvroConvert's pass |
| 2 | Fixtures in Docker, generated once and committed: Spark merge-on-read `MERGE`/`DELETE`/`UPDATE`, Trino `DELETE`, Flink equality deletes (global and partitioned), nested struct/list/map with a renamed nested field, a rename without a later commit, type promotion, a legacy v1 table, two branches and a tag, an empty table (`current-snapshot-id` -1), Spark's expected rows as JSON | each fixture has its generator script and expected rows, like `spikes/fixtures` |
| 3 | Metadata model (I4); folder catalog with both namings and `RemapLocation` (I2); open by path in storage (I3) | every fixture loads; each refusal has a test with the feature in its message; reading a manifest does no LIST on S3 (request count on Silo) |
| 4 | Read path (I5): field-id resolver for flat and nested bindings, row-group hook, schema choice, identity partition fill, `CountAsync` from manifests | every fixture without deletes reads like its expected rows, renamed nested field included; `UseFiles` tests unchanged |
| 5 | Scan planning (I6): facts per transform, `Bucket` and `Prefix`, bounds decoding, streaming plan, metrics | the spike's pruning numbers (8 of 9 manifests, 8 of 9 files, 1 of 4 bucket files); a property test over random filters with `Not`, `!=`, nulls and pre-1970 times never loses a row |
| 6 | Time travel (I8) | snapshot, time, tag and branch on the branch fixture; renamed and dropped columns under `AsOf` |
| 7 | Position deletes (I7) | Spark and Trino delete fixtures read like Spark, equal sequence numbers included |
| 8 | Equality deletes (I7) | Flink fixtures, global and partitioned, read like Spark; 1M deletes measured, the limit tested |
| 9 | REST catalog (I2, I3): config, `loadTable`, auth and refresh, 404 rules, refusals, vended credentials in both shapes, per-file storage, expiry, endpoint override, cache | Lakekeeper + Keycloak integration tests through `UseIceberg`; 401/419 retry, missing table, wrong prefix and expired-credentials tests |
| 10 | `.Aws` SigV4 handler, `FromProperties` in `.Aws` and `.Azure` | AWS's published SigV4 test vectors; property parsing tests |
| 11 | CLI (I10) | each command on a folder fixture and on Lakekeeper |
| 12 | Docs: `docs/providers/Iceberg.md`, capability matrix, README, release notes `v0.3.0-preview.1`; AvroConvert 4.0.1 on NuGet with `NoticeHandledBy` before the package is published | docs examples compile in the samples project; one notice with nothing declared, none with a declared licence |

Steps 3 to 8 run on file fixtures only; the catalog (9) comes after the read path is right, so a reading bug is never mixed up with an auth bug.

## Decisions

Decided by Adrian on 24 November 2026:

1. **Time travel on the interface.** `AsOf` is on `IDatasetQuery<T>` (I8).
2. **0.3 starts before the go/no-go conversations.** Recorded in the product plan; the conversations decide what comes after 0.3.
3. **Equality deletes in 0.3** (step 8).
4. **AvroConvert 4.0.1 first**, with the notice hand-over (I1).
5. **The spike leaves `master`** (I1).
6. **One CLI package under the Iceberg licence** (I1).
7. **One notice, Iceberg's** (I1).
8. **`AsOf` keeps renamed columns' data**: current schema first, snapshot schema for the rest (I5).

## Review log

Reviewed 24 November 2026 in a separate session against the code on `master` (b5041f3), the spike, the spec and the OpenAPI. Verdict: **Go with changes**. All changes below are applied above.

| # | Finding | Decision |
|---|---|---|
| 1 | `.Iceberg` would ship as MIT: `src/Directory.Build.props` sets the expression and packs the MIT file for every package | `DataLakeLicence` property, own licence file, build check, `.nupkg` test in step 1 |
| 2 | The CLI would carry the Iceberg DLL inside an MIT tool package | the whole CLI under the Iceberg licence (decision 6) |
| 3 | The spike's reader is already public under MIT | spike removed from `master`, package written from the spec, root licence scoped (decision 5) |
| 4 | Licence hand-over: only the commercial key, and too late for AvroConvert's first-call notice | free licences too, in the setter and before the first manifest; `NoticeHandledBy` in AvroConvert 4.0.1; Iceberg keys list `avroconvert` |
| 5 | The snapshot's schema for main and branches reads a column renamed after the last commit as null | current schema first (decision 8) |
| 6 | Equality-delete scoping missing; "sequence number" instead of data sequence number; `referenced_data_file` | I7 rewritten with the spec's rules; fixtures for global deletes and equal sequence numbers |
| 7 | Field ids don't reach nested types; `DataField.FieldId` is -1 on read | resolver on `SchemaElement.FieldId` for flat and nested bindings; nested rename fixture |
| 8 | Deleted positions can't carry equality deletes: the reader binds only the class's columns | one row-group hook in `FileScan<T>` for both delete kinds |
| 9 | v1 legacy metadata, `current-snapshot-id` -1, v4, gzip by name | I4: legacy fields, empty table, refuse above 2, magic bytes |
| 10 | The folder catalog can't find PyIceberg's `00000-<uuid>.metadata.json`; fixtures use `file:///warehouse` | both namings, `RemapLocation` |
| 11 | Pruning described as projection, but the core evaluates facts; bucket and string truncate don't fit `Range`; unknown transforms | facts per transform, `Bucket` and `Prefix`, code points, pre-1970 correction, bounds by length |
| 12 | Opening one file costs LIST + HEAD and list permission | open by path with a known length in storage |
| 13 | Any REST 404 would read as empty | only `NoSuchTableException` / `NoSuchNamespaceException` |
| 14 | OAuth: `/v1/config` needs a token first; optional `expires_in`; 419; concurrent refresh; retry | I2 auth and refresh |
| 15 | `read-restrictions` and remote signing ignored | refused by name |
| 16 | Credentials expire during a scan; S3 clients never disposed; data under other prefixes; example didn't compile | expiry refresh, per-credential cache with disposal, storage per file, properties overload |
| 17 | Missing risks (decimals above 28 digits, buffer pool, memory) and a fixtures step | risks added; step 2; delete manifests first, data manifests streamed |
| 18 | `EnsureCreatedAsync` always throwing; `Open` without options; dangling reference; namespace precedence and encoding | no-op when tables exist; `DataLakeOptions`; precedence and `namespace-separator` |
