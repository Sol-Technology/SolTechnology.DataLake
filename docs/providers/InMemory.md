# InMemory provider

The [Files provider](Files.md) on memory instead of storage. For tests that need real reads and writes without a disk.

```csharp
var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory()));

services.AddDataLake<IShopLake, ShopLake>(o => o.UseInMemory());   // in a test host
```

* **Not a second implementation.** The same writer, reader, type rules, partition folders, statistics and errors as `UseFiles`, so a test sees what production would. That is the difference to EF Core's InMemory provider, which Microsoft discourages because it behaves differently from a database.
* **No disk at all**: files live in a `MemoryStorage`, and the temporary file a Parquet file is built in is a memory stream.
* **Lifetime**: the data lives as long as the `MemoryStorage`. Each `UseInMemory()` makes a new one; pass your own to share data between contexts, to seed files, or to look at what was written:

```csharp
var memory = new MemoryStorage();
var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory(memory)));
await lake.Orders.AppendAsync(orders);
memory.Locations.Should().Contain(l => l.Contains("/orders/Date=2026-10-29/"));
```

* `UseInMemory(storage, configure)` takes the same `FilesOptions` as `UseFiles`: Parquet write settings, `Avro()`, `Arrow()`.
* What it can do, and what it costs: the same table as [Files](Files.md#what-it-can-do).
* For unit tests of your own logic, mocking your context's interface is simpler still; see [Tests](../Documentation.md#tests).
