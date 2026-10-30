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

## rest-catalog

Lakekeeper (Iceberg REST catalog) with Postgres and Silo (S3 + STS, a MinIO fork) in Docker. A PyIceberg container creates `shop.orders` through the catalog and writes the rows it reads back to `rest-catalog/expected/orders.json`.

```bash
cd spikes/rest-catalog && docker compose up -d   # wait until the seed container exits
dotnet test spikes/IcebergSpike --filter TestCategory=Docker
./deletes.sh                                       # optional: position deletes from Trino
docker compose --profile trino down -v
```

`deletes.sh` starts Trino and makes `shop.orders_mor` with `DELETE` and `UPDATE`, so the table has position delete files; PyIceberg dumps the rows to `expected/orders_mor.json`.

`RestCatalogTests` load the tables through REST with vended S3 credentials and read it from Silo. Without the catalog running they are skipped. CI runs the same steps in the `rest-catalog` job of `.github/workflows/build.yml` with `REQUIRE_DOCKER=1`, so there a catalog that does not start is a failure, not a skip.

Lakekeeper hands out `http://silo:9000` as the S3 endpoint; the tests use `http://localhost:9000` instead.
