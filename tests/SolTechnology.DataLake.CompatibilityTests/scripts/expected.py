"""
Expected values for the compatibility corpus, from pyarrow (the reference reader).

For every *.parquet / *.parq under data/ (bad_data excluded), writes data/expected/<path>.json:
  {"rows": N, "head": [first 20 rows]}  or  {"error": "..."} when pyarrow can't read the file either.

Values are canonical, so C# and Python can be compared as text:
  integers and floats as JSON numbers (NaN/Infinity as strings), decimals as plain strings without trailing zeros,
  bytes as "base64:...", dates "YYYY-MM-DD", times "HH:MM:SS.ffffff", timestamps "YYYY-MM-DDTHH:MM:SS.ffffff"
  (UTC for instants, wall clock otherwise; below microseconds dropped), UUIDs as lowercase strings,
  structs as objects, lists as arrays, maps as arrays of [key, value].
"""
import base64
import datetime
import decimal
import json
import math
import pathlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

HEAD = 20
root = pathlib.Path(__file__).resolve().parent.parent / "data"
SKIP = {"large_string_map.brotli.parquet": "skipped: pyarrow needs more than 2 GB of memory for it"}


def canon(value, typ):
    if value is None:
        return None
    if pa.types.is_struct(typ):
        return {typ.field(i).name: canon(value.get(typ.field(i).name), typ.field(i).type) for i in range(typ.num_fields)}
    if pa.types.is_map(typ):
        return [[canon(k, typ.key_type), canon(v, typ.item_type)] for k, v in value]
    if pa.types.is_list(typ) or pa.types.is_large_list(typ) or pa.types.is_fixed_size_list(typ):
        return [canon(v, typ.value_type) for v in value]
    if isinstance(typ, pa.ExtensionType) and typ.extension_name == "arrow.uuid":
        return str(value if isinstance(value, uuid.UUID) else uuid.UUID(bytes=value))
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, decimal.Decimal):
        return decimal_text(value)
    if isinstance(value, (bytes, bytearray)):
        return "base64:" + base64.b64encode(bytes(value)).decode()
    if isinstance(value, str):
        return value
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d}T{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
    if isinstance(value, datetime.date):
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
    if isinstance(value, datetime.time):
        return f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"
    if isinstance(value, uuid.UUID):
        return str(value)
    return repr(value)


def decimal_text(value):
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def micros(typ):
    """Nanosecond times as microseconds: Python's time has no nanoseconds (timestamps come as pandas.Timestamp)."""
    if pa.types.is_time64(typ) and typ.unit == "ns":
        return pa.time64("us")
    if pa.types.is_struct(typ):
        return pa.struct([f.with_type(micros(f.type)) for f in typ])
    if pa.types.is_map(typ):
        return pa.map_(micros(typ.key_type), micros(typ.item_type))
    if pa.types.is_list(typ):
        return pa.list_(typ.value_field.with_type(micros(typ.value_type)))
    if pa.types.is_large_list(typ):
        return pa.large_list(typ.value_field.with_type(micros(typ.value_type)))
    return typ


def expected(path):
    if path.name in SKIP:
        return {"error": SKIP[path.name]}
    try:
        table = pq.read_table(path)
        head = table.slice(0, HEAD)
        head = head.cast(pa.schema([f.with_type(micros(f.type)) for f in head.schema]), safe=False)
        rows = []
        for r in range(head.num_rows):
            rows.append({f.name: canon(head.column(i)[r].as_py(), f.type) for i, f in enumerate(head.schema)})
        return {"rows": table.num_rows, "head": rows}
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"[:300]}


for path in sorted(list(root.rglob("*.parquet")) + list(root.rglob("*.parq"))):
    relative = path.relative_to(root)
    if relative.parts[0] == "expected" or "bad_data" in relative.parts:
        continue
    out = root / "expected" / (str(relative) + ".json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(expected(path), indent=1, ensure_ascii=False, allow_nan=False) + "\n")
    print(relative)
