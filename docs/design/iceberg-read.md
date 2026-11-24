# Iceberg read: `UseIceberg` for milestone 0.3

Status: draft, 24 November 2026, decisions below taken the same day. To be reviewed in a separate session before step 1, like [lake-context.md](lake-context.md).

Sources: [ROADMAP.md](../ROADMAP.md) milestone 0.3, the [spike report](../../spikes/REPORT.md) and its reader in `spikes/IcebergSpike`, [lake-context.md](lake-context.md) (D3 `InNamespace`, D5 storage per table, D6 capability matrix), the product plan in the licensing repo (`ops/docs/DataLakePlan.md` F6, D3, D6, D7).

## Goal

An Iceberg table is one more provider of the same `IDataset<T>`. Code that reads `lake.Orders` does not change when the context moves from `UseFiles` to `UseIceberg`.

```csharp
services.AddDataLake<IShopLake, ShopLake>((sp, o) => o
    .UseIceberg(IcebergCatalog.Rest(new Uri("https://catalog.example.com/catalog"), c => c
            .ClientCredentials("datalake", secret)
            .Warehouse("shop")),
        i => i.Namespace("sales")
              .Storage(S3Storage.FromProperties)));   // credentials vended by the catalog, per table

await foreach (var order in lake.Orders.Where(o => o.Date == yesterday).ReadAsync()) { }
await foreach (var order in lake.Orders.AsOf(LakeVersion.Time(lastMonday)).ReadAsync()) { }

// Without a catalog, read-only, like Dataset.Open:
var orders = IcebergTable.Open<Order>("s3://shop/sales/orders/metadata/00042-....metadata.json", s3);
```

Demo for 0.3 (scenario S4 of the product plan): a table after a Spark `MERGE` with position deletes reads row for row like Spark reads it, a filter skips manifests and files, and time travel returns an older snapshot.

Not in 0.3: writes (1.0), spec v3 (refused by name), name mapping for files without field ids, ORC and Avro data files, remote signing, views, `reportMetrics`.

## Design

### I1. Package and licence

- New package `SolTechnology.DataLake.Iceberg`. Licence as decided on 23 November: PolyForm Noncommercial and Small Business free, commercial through Polar; the code is never MIT.
- `IcebergLicense` follows `AvroLicense`: `NonCommercial`, `SmallBusiness`, `Commercial(key)`, environment variable `DATALAKE_ICEBERG_LICENSE`, same signed key format, product `datalake-iceberg` in `products`. Informational only: never restricts, one notice on standard error.
- When the key also lists `avroconvert` and the application has not declared its own AvroConvert licence, the package passes it on with `AvroConvert.License = AvroLicense.Commercial(key)`. Needs AvroConvert 4.0.1 (the `products` check is on AvroConvert `master` since 23 November, not released).
- Core: `InternalsVisibleTo("SolTechnology.DataLake.Iceberg")`, `UsesCoreInternals` so the package depends on exactly its core version, and `LakeContextOptionsBuilder.Use` becomes `internal` so another package can add a provider. `LakeProvider` stays internal (product plan D3: no third-party Iceberg provider without a fork).

### I2. Catalogs

```csharp
IcebergCatalog.Rest(Uri uri, Action<RestCatalogOptions>? configure = null)
IcebergCatalog.Folder(string root, ILakeStorage? storage = null)   // <root>/<namespace>/<table>/metadata/, version-hint.text
```

REST, from the Iceberg REST OpenAPI:

| Call | Used for |
|---|---|
| `GET /v1/config?warehouse=` | `prefix`, `defaults` and `overrides` |
| `GET /v1/{prefix}/namespaces`, `.../tables` | CLI `ls`; not needed to read |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` | `loadTable`, with `X-Iceberg-Access-Delegation: vended-credentials` |

- Auth: `ClientCredentials(id, secret, scope)` (token endpoint from `oauth2-server-uri`, else the deprecated `/v1/oauth/tokens`), `Token(Func<CancellationToken, ValueTask<string>>)` for Entra ID, Databricks PAT and the like, and `Handler(HttpMessageHandler)` for anything else. Tokens refresh before `expires_in`.
- SigV4 for AWS Glue and S3 Tables is a `DelegatingHandler` in `SolTechnology.DataLake.Aws` (MIT, it signs HTTP requests and knows nothing about Iceberg), passed through `Handler(...)`.
- `loadTable` is cached per table for a short time (default 30 s, configurable) so a burst of reads does not call the catalog each time; `AsOf` and a new context always see the latest metadata.
- The folder catalog is for tests, local experiments and tables written by PyIceberg's `SqlCatalog` to a folder. It reads `version-hint.text`, else the highest `vN.metadata.json`.

### I3. Storage per table

Iceberg paths are absolute (`s3://bucket/...`), and a catalog may vend credentials per table (spike: `config` in older catalogs, `storage-credentials` with a path prefix in newer ones). The provider asks a resolver for the storage of each table:

```csharp
i.Storage(ILakeStorage storage)                                          // one storage for every table
i.Storage(Func<IcebergStorageRequest, ILakeStorage> resolve)             // per table: location + vended properties
```

- `IcebergStorageRequest` carries the table location and the merged properties (`config`, then the longest matching `storage-credentials` prefix).
- `S3Storage.FromProperties(IReadOnlyDictionary<string, string>)` in `.Aws` and `AzureBlobStorage.FromProperties(...)` in `.Azure` read the standard keys (`s3.access-key-id`, `s3.session-token`, `s3.endpoint`, `s3.region`, `adls.sas-token.<account>`, ...). They know property names, not Iceberg, so they stay MIT.
- `i.EndpointOverride(Uri)`: the catalog hands out the endpoint it sees (`http://silo:9000` in Docker).
- Credentials expire: the storage is resolved again when `loadTable` is called again (I2 cache).

### I4. Table metadata

- `metadata.json` format versions 1 and 2, plain or gzip (`*.gz.metadata.json`, Lakekeeper). Schemas, partition specs, snapshots, `refs` (branches and tags), `snapshot-log`. Sort orders and statistics files are read and ignored.
- Manifest lists and manifests through AvroConvert, v1 and v2 field sets (v1 has no `content` and no sequence numbers; a v1 table reads as sequence number 0 everywhere).
- Refused with a message that names the feature: `format-version` 3 (deletion vectors, `timestamp_ns`, default values, row lineage), encryption (`encryption-keys`, `key_metadata` on a file), ORC and Avro data files, files without field ids (name mapping, `schema.name-mapping.default`).

### I5. From a class to field ids

- A property maps to a column name as today (`[DataMember]`, `ColumnNaming`, case-insensitive), and the name is looked up **in the schema of the snapshot being read** to get the field id. Reading a file matches by field id, never by name, so renamed columns keep working and an added column reads as null in old files.
- A property with no field in the schema: null if it accepts null, else `InvalidOperationException` naming the table, the property and the schema id. A required table column the class does not have is simply not read.
- Type promotion allowed by the spec (`int` to `long`, `float` to `double`, wider decimal precision) goes through the existing widening conversions.
- Identity-partitioned columns missing from a data file (imported Hive tables) are filled from the file's partition tuple, the way hive folder values are today.
- Core change: `FileScan<T>` gets an optional column resolver (property to Parquet field, by field id) and a set of deleted row positions. Without them the Parquet reader works as it does today, so `UseFiles` does not change.

### I6. Scan planning

Same three levels as the spike, with the filter the LINQ translator already produces (`Predicate`, `Tri`, `ColumnFacts`):

1. **Manifest list**: partition field summaries (`lower_bound`, `upper_bound`, `contains_null`) against the filter projected through the partition transform.
2. **Manifest entries**: the partition tuple of each data file against the projected filter.
3. **Column metrics**: `lower_bounds`, `upper_bounds`, `null_value_counts`, `value_counts` of each data file, decoded by the spec's single-value binary serialization.

Projection through transforms is inclusive: `identity` all comparisons, `year`/`month`/`day`/`hour` and `truncate` ranges, `bucket` only `==` and `In`. `void` matches everything. Row groups are still skipped by Parquet statistics inside each file, and the residual filter runs on every row, so a projection that is too wide costs time, never rows.

`ScanMetrics` gains `Manifests`, `ManifestsSkipped`, `DeleteFiles` and `RowsDeleted`; `Files` and `FilesSkippedByPartition` keep their meaning, plus `FilesSkippedByStatistics`.

`CountAsync` without a filter and without delete files is the sum of `record_count` from the manifests: no data file is opened.

### I7. Delete files

- **Position deletes**: a delete file applies to a data file when the paths match and the data file's sequence number is lower than or equal to the delete file's. Delete files are scoped by partition and by the `file_path` bounds in their metrics before they are read. The positions of one data file are a sorted array; the reader skips them, counting positions also through row groups it skips.
- **Equality deletes**: apply when the data file's sequence number is strictly lower. The key columns (`equality_ids`) are read from the data file even when the class has no property for them. Keys of all applicable delete files are held in memory, so the docs give the limit and the metric. Fixtures need a writer that produces them (Flink, or Iceberg Java in a test container): see open question 3.
- Until step 7 is done, a table with equality delete files throws `NotSupportedException` naming the file, as in the spike. A read never returns rows a delete file removed.

### I8. Time travel

```csharp
IDatasetQuery<T> AsOf(LakeVersion version);

LakeVersion.Snapshot(long id)
LakeVersion.Time(DateTimeOffset at)      // the snapshot current at that moment, from snapshot-log
LakeVersion.Tag(string name)
LakeVersion.Branch(string name)
```

- On `IDatasetQuery<T>`, so `AsOf` and `Where` chain in any order. The files providers throw `NotSupportedException` naming the provider (D6 of lake-context.md already says so). Changing the interface is free now: nothing is published yet.
- The schema used is the snapshot's schema (`schema-id` on the snapshot), as Spark and PyIceberg do, so a renamed column in an old snapshot still maps by field id.
- A time before the first snapshot, an unknown id, tag or branch: `InvalidOperationException` with the table and the earliest snapshot time.

### I9. Writes, errors, telemetry

- Every write method throws `NotSupportedException`: "Iceberg writes come in SolTechnology.DataLake 1.0". `EnsureCreatedAsync` throws the same.
- A table that does not exist reads as empty, as on every provider (D7 of lake-context.md); a namespace that does not exist too. A catalog that answers 401 or 403 throws, with the catalog URI and the table.
- The `datalake.scan` activity gets `datalake.iceberg.snapshot_id` and `datalake.iceberg.schema_id`; catalog calls are `datalake.catalog` activities.

### I10. CLI

`datalake table show|snapshots|files|history <table> --catalog <uri>` and the same for a `metadata.json` path, in `SolTechnology.DataLake.Cli`, which references `.Iceberg` (product plan: Iceberg commands under the Iceberg licence).

## Risks

- **Parquet.Net timestamp statistics.** Its min/max for `MICROS` are in milliseconds (spike). Iceberg tables written by Spark or PyIceberg are not affected, but a file written by Parquet.Net can be; the existing `TrustsTimestamps` check stays, and manifest bounds are trusted only from the manifest, not recomputed.
- **Delete file memory.** A table with many equality deletes needs all keys in memory. Measure on a fixture with 1M deletes before 0.3 ships; if too much, refuse above a limit with a message.
- **v3 sooner than planned.** Snowflake Horizon writes v3 (product plan D6). 0.3 refuses it by name; the refusal message is the signal to count in conversations.
- **Fixtures.** Spark `MERGE` (merge-on-read) and equality deletes need writers we don't run today. Generated once in Docker and committed, like `spikes/fixtures`.

## Steps

| # | Step | Done when |
|---|---|---|
| 1 | Package `SolTechnology.DataLake.Iceberg`, licence files, `IcebergLicense` and the notice, `InternalsVisibleTo`, internal `Use`, `UseIceberg` with a provider whose writes throw | a context with `UseIceberg(IcebergCatalog.Folder(...))` builds; writes throw the I9 message; licence tests copied from AvroConvert's pass |
| 2 | Metadata model (I4): `metadata.json` v1/v2 and gzip, manifest list and manifests v1/v2, refusals; folder catalog; `IcebergTable.Open<T>` | the spike's 4 tables and a new v1 table load; each refusal has a test with the feature in the message |
| 3 | Read path (I5): field-id resolver and deleted positions in `FileScan<T>`, schema evolution, identity partition fill, `CountAsync` from manifests | every snapshot of the spike tables reads like `spikes/fixtures/expected`; `UseFiles` tests unchanged |
| 4 | Scan planning (I6): transforms projection, partition summaries, partition tuples, column bounds, metrics | the spike's pruning numbers (8 of 9 manifests, 8 of 9 files, 1 of 4 bucket files) and no lost rows on a property test over random filters |
| 5 | Time travel (I8): `LakeVersion`, `AsOf` on the interface, files providers throw | snapshot, time, tag and branch on a fixture with two branches |
| 6 | Position deletes (I7) | the Trino `DELETE`/`UPDATE` table and a Spark `MERGE` table read like Spark |
| 7 | Equality deletes (I7) | a Flink- or Java-written fixture reads like Spark; memory measured on 1M deletes |
| 8 | REST catalog (I2, I3): config, `loadTable`, OAuth2, token, handler, refresh, vended credentials in both shapes, endpoint override, cache | the spike's Lakekeeper + Keycloak integration tests run through `UseIceberg`; 401 test |
| 9 | `.Aws` SigV4 handler and `FromProperties` in `.Aws` and `.Azure` | signing test against AWS's published SigV4 test vectors; property parsing tests |
| 10 | CLI (I10) | each command on the folder fixture and on Lakekeeper |
| 11 | Docs: provider page `docs/providers/Iceberg.md`, capability matrix, README, release notes `v0.3.0-preview.1` | docs build, examples compile in the samples project |

Steps 2 to 7 run on file fixtures only; the catalog (8) comes after the read path is right, so a reading bug is never mixed up with an auth bug.

## Decisions

Decided by Adrian on 24 November 2026, before the review:

1. **Time travel on the interface.** `AsOf` is on `IDatasetQuery<T>` (I8); Delta will use the same call.
2. **0.3 starts before the go/no-go conversations.** Recorded in the product plan (`ops/docs/DataLakePlan.md`); the conversations still decide what comes after 0.3.
3. **Equality deletes in 0.3.** Step 7 is part of the milestone, so 0.3 needs an equality-delete fixture (Flink or Iceberg Java in a test container).
4. **AvroConvert 4.0.1 first.** Released before the Iceberg package is published, for the licence hand-over in I1.

## Open questions

None before the review.
