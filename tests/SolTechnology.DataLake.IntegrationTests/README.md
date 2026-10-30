# Integration tests

Real services in Docker through Testcontainers. One setup for the whole run (`IntegrationTestsFixture`):

- **Silo**: S3 and STS (a maintained MinIO fork)
- **Lakekeeper** on Postgres: Iceberg REST catalog, vends S3 credentials per table
- **PyIceberg** (image from `Docker/pyiceberg`): writes `shop.orders` through the catalog and dumps the rows it reads; that is the expected result
- **Trino**: copies the table to `shop.orders_mor` and changes it with `DELETE` / `UPDATE`, which writes position delete files

```bash
dotnet test tests/SolTechnology.DataLake.IntegrationTests
```

Needs Docker with Linux containers. A run from scratch takes about 30 s once the images are pulled.

`TESTCONTAINERS_REUSE=true` keeps the containers running between runs (about 15 s per run). Remove them with `docker rm -f $(docker ps -aq --filter name=soltech-datalake-)`.

Lakekeeper hands out `http://silo:9000` as the S3 endpoint, which works inside the Docker network. Tests on the host read through Silo's mapped port instead.
