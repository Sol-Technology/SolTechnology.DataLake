# LakeContext: one API, provider per context

Status: implemented on 20 November 2026 (steps 1-8), before `0.2.0-preview.1` was published. Current behaviour: [Documentation.md](../Documentation.md).

## Goal

Code that reads and writes tables does not change when the table technology changes. You pick the technology once, where the context is configured. Like EF Core, where swapping SQL Server for PostgreSQL is a change in `AddDbContext`, not in queries.

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

// The only place that knows the technology:
services.AddDataLake<IShopLake, ShopLake>((sp, o) => o
    .UseParquet("s3://shop/", new S3Storage(sp.GetRequiredService<IAmazonS3>()), p => p.Compression(ParquetCompression.Zstd))
    .ColumnNaming(JsonNamingPolicy.SnakeCaseLower));
// 0.3:   o.UseIceberg(catalog, i => i.Namespace("shop"))
// tests: o.UseInMemory()

// Same in every case:
await foreach (var order in lake.Orders.Where(o => o.Date == yesterday).ReadAsync()) { }
await lake.Orders.AppendAsync(newOrders);
await lake.Orders.ReplacePartitionsAsync(todaysOrders);                   // replaces the partitions the rows fall into
await lake.Orders.OverwriteAsync(fixedOrders, where: o => o.Date == day);  // replaces what the filter matches
```

Not a goal: change tracking, `SaveChanges`, migrations, `IQueryable`. A lake table is a stream of reads and writes; a unit of work would get in the way.

## What users are used to

Research, 20 November 2026 (sources at the end):

- **Testing.** Microsoft discourages the EF InMemory provider ("no advantages" over SQLite in memory, behaves differently) and recommends mocking a narrow interface that returns materialized results, never `IQueryable`. Mocking `DbSet` is "not possible" properly. So: small interfaces to mock, plus a fake that runs the real engine in memory, not a second implementation of the semantics.
- **Several instances side by side.** EF's idiom is one class per context, `AddDbContext<TContext>` with `DbContextOptions<TContext>`. Keyed services have open bugs (validation depends on registration order, `AnyKey` creating new singletons) and the runtime team was cautious about moving named `HttpClient`s onto them. One class per context, no keys.
- **Write vocabulary.** Spark `mode("append" | "overwrite" | "errorifexists" | "ignore")` plus dynamic partition overwrite; delta-rs `write_deltalake(mode, partition_by, predicate)`; PyIceberg `append`, `overwrite(overwrite_filter)`, `dynamic_partition_overwrite`, `delete(filter)`; pyarrow `existing_data_behavior`. Append, overwrite, overwrite with a filter, partition-scoped overwrite, error if exists. `AppendAsync` / `OverwriteAsync` / `OverwriteAsync(where)` / `ReplacePartitionsAsync` / `DeleteAsync(where)` match, with `overwrite` meaning the whole table as everywhere else.
- **Many providers.** Abstractions decay into the lowest common denominator when differences are hidden. What works: a closed, small surface, and a loud, typed error when a provider can't do something (EF throws instead of evaluating on the client since 3.0).
- **.NET today.** No EF-like lake API exists; .NET for Spark is dead; delta-dotnet is low-level bindings; EngineeredWood is file/format level. Nobody asked for `IQueryable` over Parquet; .NET sentiment favours explicit, non-leaky methods.

## Design

Revised after the review below; the first draft's decisions that changed are marked in the review log.

### D1. `LakeContext`, `IDataLake`, `IDataset<T>`, `IDatasetQuery<T>`

Naming: `Dataset`, not `Table` (decided 20 November). Spark's `Dataset[T]` is the same idea: a typed view over any source, plain files or an Iceberg table. The docs say "an Iceberg table, used as a dataset". Generic `IDataset<T>` does not clash with ADO.NET's `DataSet`.

```csharp
public interface IDataLake
{
    IDataset<T> Dataset<T>(string name) where T : class;
}

public interface IDatasetQuery<T> where T : class
{
    IAsyncEnumerable<T> ReadAsync(CancellationToken ct);
    Task<long> CountAsync(CancellationToken ct);
}

public interface IDataset<T> : IDatasetQuery<T> where T : class
{
    string Name { get; }
    IDatasetQuery<T> Where(Expression<Func<T, bool>> predicate);
    Task<LakeWriteResult> AppendAsync(IAsyncEnumerable<T> rows, CancellationToken ct);
    Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, CancellationToken ct);                       // the whole table
    Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, Expression<Func<T, bool>> where, CancellationToken ct);
    Task<LakeWriteResult> ReplacePartitionsAsync(IAsyncEnumerable<T> rows, CancellationToken ct);              // dynamic overwrite
    Task<LakeWriteResult> DeleteAsync(Expression<Func<T, bool>> where, CancellationToken ct);
}
```

- **`Where` returns `IDatasetQuery<T>`**: read and count only, so writing through a filter does not compile. `IDatasetQuery<T>.Where` chains (`&&`).
- **No optional parameters on the interfaces** (Moq can't put them in expression trees; NSubstitute stubs silently miss). Convenience overloads are extension methods: `ReadAsync()`, `ReadAsync(ScanMetrics)`, `AppendAsync(IEnumerable<T>)`, and so on. Metrics come in through an extension that passes them in an internal scan context.
- **`CountAsync` now**: Parquet footers and Iceberg manifests answer it without reading rows, and adding it later would break implementers. No `AnyAsync`.
- `LakeWriteResult` is a class with init properties (files written, files removed, rows).

### D2. Write semantics, matching Spark, delta-rs and PyIceberg

| Method | Meaning | Empty input |
|---|---|---|
| `AppendAsync(rows)` | add rows | no-op |
| `OverwriteAsync(rows)` | replace the whole table (Spark static overwrite, PyIceberg `overwrite`) | truncates the table |
| `OverwriteAsync(rows, where)` | delete what `where` matches, add rows; every row must match `where`, else throw before writing (Delta `replaceWhere`) | deletes what matches |
| `ReplacePartitionsAsync(rows)` | replace only the partitions the rows fall into (Spark dynamic, PyIceberg `dynamic_partition_overwrite`) | no-op; throws on an unpartitioned table |
| `DeleteAsync(where)` | delete what `where` matches | |

Writes go only to **tables declared in the model**. `Dataset<T>("typo")` can read (and reads nothing), but a write throws `InvalidOperationException: table 'typo' is not in ShopLake's model`. No silent new folders, no surprise deletes.

A failed or cancelled write deletes, best effort, the files it already uploaded.

### D3. Model in `OnModelCreating`

```csharp
model.Dataset<Order>("orders")
    .InNamespace("sales", "eu")          // Iceberg; ignored by Parquet, which uses the name as a folder
    .PartitionBy(o => o.Date, o => o.Country);
```

- **The partition spec in the model is for creating a table.** On an existing table the table wins: Iceberg uses the spec in its metadata (and throws if a declared spec disagrees); Parquet checks the folders it finds and throws when their hive keys don't match the declared ones (case-insensitive, so Spark's `date=` matches `Date`).
- Partitioning is an expression per column, so Iceberg transforms come later (`LakePartition.Day(o.CreatedAt)`, `Bucket(16, o.Id)`); Parquet accepts plain properties only.
- `[DataMember]`, `[IgnoreDataMember]`, `[DecimalPrecision]`, `ColumnNaming` keep working.
- **Model caching**: only the provider-free part (properties, expressions) is cached per context type. Column names and provider checks are resolved per options instance, keyed on (context type, provider, naming), like EF's `IModelCacheKeyFactory`. So production on Iceberg and tests on `UseInMemory()` use the same class safely.

### D4. Filters that decide what gets deleted (Parquet)

`OverwriteAsync(rows, where)`, `DeleteAsync(where)` and `ReplacePartitionsAsync` delete whole files only, never rewrite them:

- `where` must translate completely (no opaque parts, e.g. `StartsWith`) and use partition columns only; otherwise `NotSupportedException` naming the column and pointing to Iceberg.
- Each existing file is evaluated against its partition values; an `Unknown` result throws instead of being skipped.
- Incoming rows are checked against the compiled `where` before anything is written.

### D5. One provider per context, provider-specific options

```csharp
services.AddDataLake<IShopLake, ShopLake>((sp, o) => o
    .UseParquet("s3://shop/", new S3Storage(sp.GetRequiredService<IAmazonS3>()), p => p
        .Compression(ParquetCompression.Zstd)
        .RowGroupSize(128 * 1024)
        .TargetFileSize(512L << 20))
    .ColumnNaming(JsonNamingPolicy.SnakeCaseLower));       // common to every provider
```

- Two technologies at once = two context classes, each with its own `AddDataLake`. No named or keyed registration (EF's idiom; keyed services have open bugs).
- Providers get storage per table (Iceberg vends credentials per table); Parquet uses the one it was given.

### D6. Capabilities: loud errors, documented matrix

| | Parquet (folders) | InMemory | Iceberg 0.3 (read) / 1.0 (write) |
|---|---|---|---|
| `ReadAsync`, `Where`, `CountAsync` | yes | yes | 0.3 |
| `AppendAsync` | yes | yes | 1.0 |
| `OverwriteAsync(rows)` | yes, not atomic | yes | 1.0, atomic |
| `ReplacePartitionsAsync` | yes, not atomic | yes | 1.0, atomic |
| `OverwriteAsync(rows, where)`, `DeleteAsync(where)` | partition filters only | same | 1.0: whole files; row-level later (delete files) |
| time travel `AsOf(...)` | no | no | 0.3 |

A call the provider can't honour throws `NotSupportedException` naming the provider, the table and what to do instead.

### D7. `UseInMemory()` is the Parquet provider on memory

- `UseInMemory(MemoryStorage? storage = null)` = the Parquet provider over `MemoryStorage : ILakeStorage`. Same writer, reader, type rules. Pass one `MemoryStorage` to seed from one context and assert from another.
- Truly no disk: the dataset writer gets a temp-stream factory (memory for this provider, temp files for the others).
- `MemoryStorage`: `CreateNew` as an atomic add, stored bytes never mutated (readers survive overwrites).
- **Missing table reads as empty on every provider.** Today local disk throws `DirectoryNotFoundException` while S3 and Azure list nothing; that is unified.
- Docs: mock `IShopLake` / `IDataset<T>` for logic, `UseInMemory()` for real read/write behaviour, our integration tests for engine compatibility.

### D8. Registration, lifetime, creation

```csharp
services.AddDataLake<ShopLake>(o => ...);
services.AddDataLake<IShopLake, ShopLake>((sp, o) => ...);
var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseParquet("/data")));
```

- **Two contexts** = two classes, two registrations, injected by type or by their interfaces:

  ```csharp
  services.AddDataLake<ISalesLake, SalesLake>((sp, o) => o.UseIceberg(catalog));
  services.AddDataLake<ILandingLake, LandingLake>((sp, o) => o.UseParquet("s3://landing/", s3));

  public class ImportJob(ILandingLake landing, ISalesLake sales) { }
  ```

- **The same model twice** (e.g. a production and an archive copy) = a subclass, as in EF. The base class takes the non-generic options in a protected constructor:

  ```csharp
  public class SalesLake : LakeContext, ISalesLake
  {
      public SalesLake(LakeContextOptions<SalesLake> options) : base(options) { }
      protected SalesLake(LakeContextOptions options) : base(options) { }
      public IDataset<Order> Orders => Dataset<Order>("orders");
  }

  public class ArchiveLake(LakeContextOptions<ArchiveLake> options) : SalesLake(options), IArchiveLake;
  ```

- Singleton; no scoped. The context does not own or dispose `IAmazonS3` / `BlobServiceClient`. Per-tenant credentials = construct contexts yourself.
- `IDataLake` is not registered on its own (ambiguous with two contexts).
- `EnsureCreatedAsync()` on the context creates declared tables that don't exist (Iceberg 1.0; Parquet: nothing to do). Writing to a declared table that doesn't exist creates it from the model. Spark's `errorifexists` is not a mode; check `CountAsync()` if you need it.
- Core package gains `Microsoft.Extensions.DependencyInjection.Abstractions` (decided 20 November).

### D9. Telemetry

`datalake.write` activity and counters next to `datalake.scan`; both tagged `datalake.context`, `datalake.table`, `datalake.provider`.

### D10. What happens to today's API

- `Dataset<T>` → internal `ParquetDataset<T>`; `DatasetWriteOptions`, `DatasetWriteMode`, `DatasetWriteResult` removed (never published).
- `DataLake.Open<T>` becomes `Dataset.Open<T>(location, options)` (a static class next to `IDataset<T>`, so it doesn't clash with `namespace X.DataLake` in user code). It stays for a quick look at one folder or file, **read-only**: it returns `IDatasetQuery<T>`. Writes always go through a context, so there is one write path with one set of safety rules (decided 20 November). A script that does not want a class builds a context inline:

  ```csharp
  var lake = LakeContext.Create(
      o => o.UseParquet("s3://shop/", s3),
      model => model.Dataset<Order>("orders").PartitionBy(o => o.Date));
  await lake.Dataset<Order>("orders").AppendAsync(rows);
  ```
- `ArrowConvert` stays a converter, like ADO.NET next to EF.
- The CLI uses `DatasetFiles` and the format readers only: unaffected.
- Providers stay internal until Iceberg proves the shape.

## Deferred: SQL queries

Raised 20 November, decision postponed. Free-text SQL (`SELECT ... FROM orders JOIN ... GROUP BY ...`) over the datasets of a context.

- Not our own SQL engine or parser: one dataset, no joins or aggregates, and typing lost against LINQ `Where`. Out of scope, as the README says.
- Preferred route: a separate package (`SolTechnology.DataLake.DuckDb`, DuckDB.NET is native, tens of MB) that registers the model's datasets as DuckDB views over the same storage and credentials, and maps results through `ArrowConvert`. API like EF Core 8: `lake.SqlQueryAsync<T>(FormattableString sql)`, interpolated values as parameters. Iceberg through DuckDB's `iceberg` extension.
- Limits: `UseInMemory()` can't serve it (DuckDB doesn't read `MemoryStorage`), so tests mock it; remote engines (Databricks, Snowflake, Trino) stay with ADBC and `ArrowConvert`.
- Open: `SqlQueryAsync<T>` on `IDataLake` now (providers without SQL throw `NotSupportedException`; mockable from day one) or a separate `ISqlLake` added by `UseSql()`. Adding it to `IDataLake` later breaks implementers of the interface, so decide before 1.0.

## Steps

| # | Step | Done when |
|---|---|---|
| 1 | Interfaces, `LakeWriteResult`, extensions; internal `DatasetSpec` (name, partition keys); `Dataset<T>` → `ParquetDataset<T>` taking a `DatasetSpec`; write tests made storage-neutral (no `Directory.GetFiles`) | existing tests pass on the new names |
| 2 | `LakeModelBuilder`, model cache split (provider-free per type, resolved per options) | wrong expression, unknown property, unsupported partition type fail at build; same class with two namings works |
| 3 | Write semantics on `ParquetDataset<T>`: append, overwrite, replace partitions, overwrite/delete with a filter, layout check, rows-match-filter check, cleanup on failure, missing table = empty | a test per row of the D2 table, per rule in D4, and for a Spark-written `date=` folder |
| 4 | `LakeContext`, `LakeContextOptions<T>`, `UseParquet` options, declared-tables-only writes, `EnsureCreatedAsync` | two contexts on two roots don't see each other; typo table write throws |
| 5 | `MemoryStorage`, temp-stream factory, `UseInMemory()` | the write test suite runs on local disk and on `UseInMemory()` with the same results; no temp files created |
| 6 | `AddDataLake` overloads | singleton, interface resolves, two contexts side by side, `(sp, o)` overload |
| 7 | Telemetry for writes | activity and tags in `TelemetryTests` |
| 8 | Docs split per provider: a common page (context, model, datasets, write semantics) and one page per provider (Parquet, InMemory, later Iceberg) with its options, capability table and costs (e.g. Parquet `CountAsync` without a filter sums footers, with a filter reads the data). README, release notes, ROADMAP; benchmarks and samples on the new API | docs examples compile (copied into a test) |

## Open questions

None. All decided on 20 November (see the end of the review log).

## Review log

Reviewed 20 November 2026 in a separate session against the code on `storage-writes`. Verdict: **Go with changes**. All changes below are applied above.

| # | Finding | Decision |
|---|---|---|
| 1 | Overwriting a table not in the model treats it as unpartitioned and deletes every file, including Spark's `date=` folders; a typo creates a folder | writes only to declared tables; layout check before any delete |
| 2 | `OverwriteAsync(rows)` meant dynamic partition overwrite, but Spark, delta-rs and PyIceberg mean the whole table; empty input undefined | `OverwriteAsync` = whole table, `ReplacePartitionsAsync` = dynamic; empty input defined |
| 3 | Writing through `Where` compiles and fails at runtime | `Where` returns `IDatasetQuery<T>` |
| 4 | Model cached per type, but partition columns depend on `ColumnNaming` and checks on the provider | provider-free part cached per type, the rest per (type, provider, naming) |
| 5 | Unclear who owns the partition spec; Parquet overwrite compared folder names case-sensitively | model spec is for creation; existing table wins; case-insensitive layout check |
| 6 | Filter overwrite: opaque parts slip through the partition-only check, `Unknown` would skip files, rows outside the filter, leftovers on failure | D4 rules and cleanup on failure |
| 7 | Iceberg needs namespaces, per-table credentials, `(sp, o)` registration | `InNamespace`, storage per table, `(sp, o)` overload |
| 8 | Optional parameters break Moq setups and silently miss in NSubstitute; no count | no optional parameters on interfaces, extensions instead; `CountAsync` |
| 9 | `UseInMemory` still wrote temp files; write tests use `Directory.*`; missing-location behaviour differs per storage | temp-stream factory, storage-neutral tests, missing table = empty everywhere, shared `MemoryStorage` |
| 10 | Step order: filter overwrite needs the model | model before write semantics |
| 11 | Capability matrix disagreed with ROADMAP (time travel 0.3, delete files later, create in 1.0) | matrix fixed; `EnsureCreatedAsync`, create on first write; `DeleteAsync` now |
| 12 | Writes had no telemetry | D9 |
| 13 | Lifetime and ownership of SDK clients | singleton, context does not own clients |
| 14 | `DataLake.Open` vs `namespace X.DataLake` | open question 3 |

Decided after the review (20 November): `Dataset.Open<T>` instead of `DataLake.Open<T>`; `Dataset` naming instead of `Table`; DI abstractions in the core package; `DataLake.Open` read-only, writes only through a context (`LakeContext.Create` inline); documentation per provider, costs such as `CountAsync` described there.

## Sources

- EF Core InMemory provider: https://learn.microsoft.com/ef/core/providers/in-memory/
- Choosing a testing strategy: https://learn.microsoft.com/ef/core/testing/choosing-a-testing-strategy
- Testing without the database (repository, no `IQueryable`): https://learn.microsoft.com/ef/core/testing/testing-without-the-database
- Mocking DbSet: https://stackoverflow.com/questions/71228582
- Multiple DbContexts: https://codewithmukesh.com/blog/multiple-dbcontext-efcore/
- Keyed services issues: https://github.com/dotnet/runtime/issues/113472, https://github.com/dotnet/runtime/issues/89755
- Azure SDK clients in DI (`WithName`, `IAzureClientFactory<T>`): https://learn.microsoft.com/dotnet/azure/sdk/dependency-injection
- Spark write modes and partition overwrite: https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
- delta-rs writing: https://delta-io.github.io/delta-rs/usage/writing/
- pyarrow `write_dataset`: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
- DuckDB partitioned writes: https://duckdb.org/docs/data/partitioning/partitioned_writes
- EF client evaluation (throw instead of silent fallback): https://learn.microsoft.com/ef/core/querying/client-eval
- .NET for Apache Spark retired: https://github.com/dotnet/spark/discussions/1160
