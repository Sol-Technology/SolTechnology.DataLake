# Spikes

Throwaway code that answers questions before the real implementation. Not shipped, not an API.

## IcebergSpike

Reads Iceberg tables written by PyIceberg and checks every snapshot against the rows PyIceberg reads back.

- `fixtures/generate.sh` writes the test tables with PyIceberg 0.10.0 in Docker: `fixtures/warehouse` (tables) and `fixtures/expected` (rows per snapshot as JSON). Run it again only when the tables change; the output is committed, so tests run without Docker.
- Tables: `orders_plain` (two snapshots), `orders_by_day` (partitioned by country and day), `orders_bucket` (bucket(4, id)), `orders_evolved` (renamed, added and dropped columns).
- Metadata points to `file:///warehouse/...`; the tests map it to `fixtures/warehouse`.

```bash
dotnet test spikes/IcebergSpike
```
