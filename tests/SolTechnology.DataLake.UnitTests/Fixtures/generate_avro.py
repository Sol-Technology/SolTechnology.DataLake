"""Writes an Avro container file with fastavro (an independent writer), plus the records as JSON.
Run with generate-avro.sh (Docker). Output is committed; tests don't need Docker."""
import datetime as dt
import decimal
import json
import uuid

from fastavro import parse_schema, writer

schema = parse_schema({
    "type": "record", "name": "Order", "namespace": "shop",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": ["null", "string"], "default": None},
        {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}], "default": None},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "local_at", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "at", "type": ["null", {"type": "long", "logicalType": "time-micros"}], "default": None},
        {"name": "uid", "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": None},
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
        {"name": "address", "type": ["null", {"type": "record", "name": "Address",
                                               "fields": [{"name": "city", "type": ["null", "string"]}, {"name": "zip", "type": "string"}]}], "default": None},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attributes", "type": {"type": "map", "values": ["null", "long"]}},
    ],
})

utc = dt.timezone.utc
records = [
    {"id": 1, "customer": "Ann", "amount": decimal.Decimal("9.99"), "created_at": dt.datetime(2026, 10, 29, 12, 30, 15, 123456, tzinfo=utc),
     "local_at": dt.datetime(2026, 10, 29, 14, 30, 15, 123000), "day": dt.date(2026, 10, 29), "at": dt.time(12, 30, 15, 123456),
     "uid": uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), "status": "PAID", "address": {"city": "Wroclaw", "zip": "50-001"},
     "tags": ["a", "b"], "attributes": {"color": 1, "size": None}},
    {"id": 2, "customer": None, "amount": None, "created_at": dt.datetime(2026, 10, 30, 0, 0, tzinfo=utc),
     "local_at": dt.datetime(2026, 10, 30, 0, 0), "day": dt.date(2026, 10, 30), "at": None, "uid": None, "status": "NEW",
     "address": None, "tags": [], "attributes": {}},
]

with open("/out/orders.avro", "wb") as f:
    writer(f, schema, records, codec="deflate")


def to_json(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.isoformat().replace("+00:00", "Z")
    if isinstance(v, (dt.date, dt.time, uuid.UUID)):
        return str(v) if isinstance(v, uuid.UUID) else v.isoformat()
    if isinstance(v, list):
        return [to_json(x) for x in v]
    if isinstance(v, dict):
        return {k: to_json(x) for k, x in v.items()}
    return v


with open("/out/orders.avro.json", "w") as f:
    json.dump([{k: to_json(v) for k, v in r.items()} for r in records], f, indent=2)
print("written", len(records))
