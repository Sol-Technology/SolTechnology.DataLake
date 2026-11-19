"""Reads a dataset DataLake wrote to S3 with pyarrow and DuckDB, writes what they see to /out/<name>.json.

Usage: interop.py <name> <s3 location> <timestamp column> <filter from, ISO with Z>
"""
import datetime as dt
import decimal
import json
import sys
import uuid

import duckdb
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

name, location, ts_column, ts_from = sys.argv[1:5]
bucket_path = location.removeprefix("s3://")

s3 = pafs.S3FileSystem(access_key="silo-root-user", secret_key="silo-root-password", endpoint_override="silo:9000",
                       scheme="http", region="local-01")


def to_json(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, dict):
        return {str(k): to_json(x) for k, x in v.items()}
    if isinstance(v, list):
        if v and isinstance(v[0], tuple) and len(v[0]) == 2:  # pyarrow map
            return {str(k): to_json(x) for k, x in v}
        return [to_json(x) for x in v]
    return v


def rows_of(records):
    return [{k: to_json(v) for k, v in r.items()} for r in sorted(records, key=lambda r: r["Id"])]


# pyarrow.dataset, hive partitions
dataset = ds.dataset(bucket_path, filesystem=s3, format="parquet", partitioning="hive")
table = dataset.to_table()
ts_from_value = dt.datetime.fromisoformat(ts_from.replace("Z", "+00:00"))
pyarrow_filtered = dataset.to_table(filter=ds.field(ts_column) >= ts_from_value).num_rows

# Statistics of the timestamp column, file by file, as pyarrow decodes them.
stats = []
for fragment in dataset.get_fragments():
    metadata = pq.ParquetFile(fragment.path, filesystem=s3).metadata
    index = metadata.schema.names.index(ts_column)
    for g in range(metadata.num_row_groups):
        s = metadata.row_group(g).column(index).statistics
        stats.append({"file": fragment.path, "created_by": metadata.created_by,
                      "min": to_json(s.min) if s and s.has_min_max else None,
                      "max": to_json(s.max) if s and s.has_min_max else None})

# DuckDB over httpfs, hive partitions
con = duckdb.connect()
con.sql("SET TimeZone = 'UTC'")
con.sql("""CREATE SECRET silo (TYPE s3, KEY_ID 'silo-root-user', SECRET 'silo-root-password',
           ENDPOINT 'silo:9000', URL_STYLE 'path', USE_SSL false, REGION 'local-01')""")
source = f"read_parquet('{location}/**/*.parquet', hive_partitioning = true)"
duck_schema = {c: str(t) for c, t in zip(*(lambda r: (r.columns, r.types))(con.sql(f"SELECT * FROM {source}")))}
cursor = con.execute(f"SELECT * FROM {source}")
columns = [d[0] for d in cursor.description]
duck_rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
duck_filtered = con.execute(f"SELECT count(*) FROM {source} WHERE \"{ts_column}\" >= TIMESTAMPTZ '{ts_from}'").fetchall()[0][0]

with open(f"/out/{name}.json", "w") as f:
    json.dump({
        "pyarrow": {"schema": {field.name: str(field.type) for field in table.schema}, "count": table.num_rows,
                    "filtered": pyarrow_filtered, "rows": rows_of(table.to_pylist())},
        "duckdb": {"schema": duck_schema, "count": len(duck_rows), "filtered": duck_filtered, "rows": rows_of(duck_rows)},
        "stats": stats,
    }, f, indent=2)
print(name, table.num_rows, "rows (pyarrow)", len(duck_rows), "rows (duckdb)")
