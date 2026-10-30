"""Creates shop.orders in the Lakekeeper REST catalog with PyIceberg and writes the rows PyIceberg reads back."""
import datetime as dt
import decimal
import json

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DecimalType, LongType, NestedField, StringType, TimestamptzType

catalog = load_catalog("lakekeeper", type="rest", uri="http://lakekeeper:8181/catalog", warehouse="spike")

schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "country", StringType(), required=True),
    NestedField(3, "amount", DecimalType(10, 2), required=False),
    NestedField(4, "created_at", TimestamptzType(), required=True),
)
spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="country"),
    PartitionField(source_id=4, field_id=1001, transform=DayTransform(), name="created_at_day"),
)

catalog.create_namespace_if_not_exists("shop")
if catalog.table_exists("shop.orders"):
    catalog.drop_table("shop.orders")
table = catalog.create_table("shop.orders", schema=schema, partition_spec=spec)

arrow_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("country", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(10, 2), nullable=True),
    pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
])
start = dt.datetime(2026, 10, 1, 8, 0, tzinfo=dt.timezone.utc)
for day in range(2):
    rows = [{
        "id": day * 10 + i,
        "country": ["PL", "DE"][i % 2],
        "amount": None if i == 3 else decimal.Decimal(day * 1000 + i * 101) / 100,
        "created_at": start + dt.timedelta(days=day, hours=i),
    } for i in range(6)]
    table.append(pa.Table.from_pylist(rows, schema=arrow_schema))

rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["id"])


def to_json(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return v


with open("/out/orders.json", "w") as f:
    json.dump({"metadata_location": table.metadata_location,
               "rows": [{k: to_json(v) for k, v in r.items()} for r in rows]}, f, indent=2)
print("seeded", len(rows), "rows,", table.metadata_location)
