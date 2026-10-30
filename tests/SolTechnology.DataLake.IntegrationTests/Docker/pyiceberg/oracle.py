"""Writes the rows PyIceberg reads from the given tables to /out/<table>.json (oracle for the .NET reader)."""
import datetime as dt
import decimal
import json
import sys

from pyiceberg.catalog import load_catalog

catalog = load_catalog("lakekeeper", type="rest", uri="http://lakekeeper:8181/catalog", warehouse="spike")


def to_json(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return v


for name in sys.argv[1:]:
    table = catalog.load_table(f"shop.{name}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["id"])
    summary = table.current_snapshot().summary
    with open(f"/out/{name}.json", "w") as f:
        json.dump({"metadata_location": table.metadata_location,
                   "format_version": table.format_version,
                   "summary": {k: v for k, v in summary.additional_properties.items()},
                   "rows": [{k: to_json(v) for k, v in r.items()} for r in rows]}, f, indent=2)
    print(name, len(rows), "rows", summary.additional_properties.get("total-position-deletes"), "position deletes")
