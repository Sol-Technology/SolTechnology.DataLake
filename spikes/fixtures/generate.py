"""Writes Iceberg test tables with PyIceberg and the rows PyIceberg reads back from every snapshot.

Run through generate.sh (Docker), not directly. Output: warehouse/ (tables) and expected/ (rows as JSON).
"""
import datetime as dt
import decimal
import json
import os
import shutil

import pyarrow as pa
import pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import (BooleanType, DateType, DecimalType, DoubleType, LongType, NestedField, StringType,
                             TimestamptzType)

WAREHOUSE = "/warehouse"
OUT = "/out"

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "customer", StringType(), required=True),
    NestedField(3, "country", StringType(), required=True),
    NestedField(4, "amount", DecimalType(10, 2), required=False),
    NestedField(5, "created_at", TimestamptzType(), required=True),
    NestedField(6, "order_date", DateType(), required=False),
    NestedField(7, "is_paid", BooleanType(), required=False),
    NestedField(8, "score", DoubleType(), required=False),
)

ARROW_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("customer", pa.string(), nullable=False),
    pa.field("country", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(10, 2), nullable=True),
    pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("order_date", pa.date32(), nullable=True),
    pa.field("is_paid", pa.bool_(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
])

COUNTRIES = ["PL", "DE", "US"]
START = dt.datetime(2026, 10, 1, 8, 0, tzinfo=dt.timezone.utc)


def orders(first_id, count, day_offset=0, country=None):
    rows = []
    for i in range(count):
        oid = first_id + i
        created = START + dt.timedelta(days=day_offset, hours=i)
        rows.append({
            "id": oid,
            "customer": f"customer-{oid % 7}",
            "country": country or COUNTRIES[oid % len(COUNTRIES)],
            "amount": None if oid % 5 == 0 else decimal.Decimal(oid * 10 + 99) / 100,
            "created_at": created,
            "order_date": None if oid % 4 == 0 else created.date(),
            "is_paid": None if oid % 6 == 0 else oid % 2 == 0,
            "score": None if oid % 3 == 0 else oid / 8,
        })
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


def to_json(value):
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def dump(table, name):
    snapshots = []
    for snap in table.metadata.snapshots:
        rows = table.scan(snapshot_id=snap.snapshot_id).to_arrow().to_pylist()
        rows.sort(key=lambda r: r["id"])
        snapshots.append({
            "snapshot_id": snap.snapshot_id,
            "rows": [{k: to_json(v) for k, v in r.items()} for r in rows],
        })
    current = table.current_snapshot()
    data_files = [task.file.file_path for task in table.scan().plan_files()]
    result = {
        "table": name,
        "pyiceberg": pyiceberg.__version__,
        "metadata": table.metadata_location.replace(f"file://{WAREHOUSE}/", ""),
        "current_snapshot_id": current.snapshot_id,
        "data_files": len(data_files),
        "snapshots": snapshots,
    }
    with open(os.path.join(OUT, "expected", f"{name}.json"), "w") as f:
        json.dump(result, f, indent=2)


def main():
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    os.makedirs(WAREHOUSE)
    os.makedirs(os.path.join(OUT, "expected"), exist_ok=True)

    catalog = SqlCatalog("fixtures", uri=f"sqlite:///{WAREHOUSE}/catalog.db", warehouse=f"file://{WAREHOUSE}")
    catalog.create_namespace("shop")

    # Unpartitioned, two appends: two snapshots for time travel.
    plain = catalog.create_table("shop.orders_plain", schema=SCHEMA)
    plain.append(orders(1, 5))
    plain.append(orders(6, 5, day_offset=1))
    dump(plain, "orders_plain")

    # Partitioned by country and day: one file per (country, day), so filters can skip files.
    spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="country"),
        PartitionField(source_id=5, field_id=1001, transform=DayTransform(), name="created_at_day"),
    )
    by_day = catalog.create_table("shop.orders_by_day", schema=SCHEMA, partition_spec=spec)
    next_id = 1
    for day in range(3):
        for country in COUNTRIES:
            by_day.append(orders(next_id, 4, day_offset=day, country=country))
            next_id += 4
    dump(by_day, "orders_by_day")

    # Bucket partitioning: the bucket of every file must match our Murmur3.
    bucket_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="id_bucket"),
    )
    bucket = catalog.create_table("shop.orders_bucket", schema=SCHEMA, partition_spec=bucket_spec)
    bucket.append(orders(1, 20))
    dump(bucket, "orders_bucket")

    # Schema evolution: rename, add and drop columns between appends. Old files keep old names.
    evolved = catalog.create_table("shop.orders_evolved", schema=SCHEMA)
    evolved.append(orders(1, 5))
    with evolved.update_schema() as update:
        update.rename_column("amount", "total")
        update.add_column("note", StringType())
        update.delete_column("score")
    new_schema = evolved.schema().as_arrow()
    batch = orders(6, 5).rename_columns(["id", "customer", "country", "total", "created_at", "order_date", "is_paid",
                                         "score"]).drop_columns(["score"])
    batch = batch.append_column(pa.field("note", pa.string(), nullable=True),
                                pa.array([f"note-{i}" for i in range(6, 11)], pa.string()))
    evolved.append(batch.select(new_schema.names).cast(new_schema))
    dump(evolved, "orders_evolved")

    os.remove(os.path.join(WAREHOUSE, "catalog.db"))
    shutil.copytree(WAREHOUSE, os.path.join(OUT, "warehouse"), dirs_exist_ok=True)
    print("pyiceberg", pyiceberg.__version__)


if __name__ == "__main__":
    main()
