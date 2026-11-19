"""pyarrow.dataset on the dataset of DatasetBenchmarks: the same three reads, timed like BenchmarkDotNet (warm-up, then 10 runs).

to_table() is columnar Arrow memory, the cheapest thing pyarrow does. to_pylist() turns rows into Python dicts, which is
closer to what DataLake hands out (one object per row).
"""
import datetime as dt
import statistics
import sys
import time

import pyarrow
import pyarrow.dataset as ds

root = sys.argv[1]
dataset = ds.dataset(root, format="parquet",
                     partitioning=ds.partitioning(pyarrow.schema([("Date", pyarrow.date32()), ("Country", pyarrow.string())]), flavor="hive"))
day = dt.date(2026, 10, 1) + dt.timedelta(days=17)

reads = {
    "all rows": None,
    "one day, one country": (ds.field("Date") == day) & (ds.field("Country") == "PL"),
    "5 000 ids": (ds.field("Id") >= 400_000) & (ds.field("Id") < 405_000),
}


def timed(run, warmup=3, iterations=10):
    for _ in range(warmup):
        run()
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        rows = run()
        times.append((time.perf_counter() - start) * 1000)
    return statistics.mean(times), statistics.stdev(times), rows


print(f"pyarrow {pyarrow.__version__}, Python {sys.version.split()[0]}")
print("| Read | rows | to_table() | to_pylist() |")
print("|---|---:|---:|---:|")
for name, condition in reads.items():
    table_ms, table_sd, rows = timed(lambda: dataset.to_table(filter=condition).num_rows)
    pylist_ms, pylist_sd, _ = timed(lambda: len(dataset.to_table(filter=condition).to_pylist()))
    print(f"| {name} | {rows:,} | {table_ms:.1f} ms ± {table_sd:.1f} | {pylist_ms:.1f} ms ± {pylist_sd:.1f} |")
