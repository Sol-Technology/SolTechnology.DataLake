"""Writes Parquet files with nested columns using pyarrow, plus the rows pyarrow reads back as JSON.
Run with generate.sh (Docker). Output is committed; tests don't need Docker."""
import datetime as dt
import decimal
import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

address = pa.struct([("city", pa.string()), ("zip", pa.string())])
line = pa.struct([("sku", pa.string()), ("qty", pa.int32()), ("price", pa.decimal128(10, 2))])

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("address", address),                          # struct, null at struct and leaf level
    pa.field("tags", pa.list_(pa.string())),               # list: null list, empty list, null element
    pa.field("scores", pa.list_(pa.int32())),
    pa.field("lines", pa.list_(line)),                     # list of structs
    pa.field("attributes", pa.map_(pa.string(), pa.int64())),
    pa.field("matrix", pa.list_(pa.list_(pa.int32()))),    # list of lists
    pa.field("customer", pa.struct([("name", pa.string()), ("emails", pa.list_(pa.string()))])),  # struct with list
    pa.field("at", pa.time64("us")),
    pa.field("uid", pa.uuid()),
])

rows = [
    {"id": 1, "address": {"city": "Wroclaw", "zip": "50-001"}, "tags": ["a", "b"], "scores": [1, 2, 3],
     "lines": [{"sku": "x", "qty": 2, "price": decimal.Decimal("9.99")}, {"sku": "y", "qty": 1, "price": None}],
     "attributes": [("color", 1), ("size", 2)], "matrix": [[1, 2], [3]], "customer": {"name": "Ann", "emails": ["a@x", "b@x"]},
     "at": dt.time(12, 30, 15, 123456), "uid": uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7").bytes},
    {"id": 2, "address": None, "tags": [], "scores": None, "lines": [], "attributes": [], "matrix": [[], None, [4]],
     "customer": {"name": None, "emails": []}, "at": None, "uid": None},
    {"id": 3, "address": {"city": None, "zip": "00-950"}, "tags": None, "scores": [], "lines": None, "attributes": None,
     "matrix": None, "customer": None, "at": dt.time(0, 0), "uid": uuid.UUID("00000000-0000-0000-0000-000000000001").bytes},
    {"id": 4, "address": {"city": "Gdansk", "zip": None}, "tags": ["c", None, "d"], "scores": [7],
     "lines": [None, {"sku": None, "qty": None, "price": decimal.Decimal("0.01")}], "attributes": [("k", None)],
     "matrix": [[None]], "customer": {"name": "Bob", "emails": None}, "at": dt.time(23, 59, 59), "uid": None},
]

table = pa.Table.from_pylist(rows, schema=schema)
pq.write_table(table, "/out/nested.parquet", row_group_size=2)


def to_json(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.time):
        return v.isoformat()
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, bytes):
        return str(uuid.UUID(bytes=v))
    if isinstance(v, list):
        if v and isinstance(v[0], tuple):
            return {k: to_json(x) for k, x in v}
        return [to_json(x) for x in v]
    if isinstance(v, dict):
        return {k: to_json(x) for k, x in v.items()}
    return v


back = pq.read_table("/out/nested.parquet").to_pylist()
maps = {"attributes"}
with open("/out/nested.json", "w") as f:
    json.dump([{k: (dict((mk, to_json(mv)) for mk, mv in v) if k in maps and v is not None else to_json(v)) for k, v in r.items()} for r in back], f, indent=2)
print(pq.read_schema("/out/nested.parquet"))
print("pyarrow", pa.__version__)
