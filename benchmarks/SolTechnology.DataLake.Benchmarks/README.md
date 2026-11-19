# Dataset benchmark

```bash
dotnet run -c Release -- verify                          # both ways return the same rows
dotnet run -c Release -- --filter '*DatasetBenchmarks*'  # dataset reads, below
dotnet run -c Release -- --filter '*NestedBenchmarks*'   # nested columns, below
./pyarrow/run.sh                                         # pyarrow.dataset on the same files (Docker)
```

A local hive-partitioned dataset: 90 Parquet files (30 days x 3 countries), 10 000 rows each in 2 row groups, 900 000 rows. Columns `Id` (long), `Amount` (decimal?), `Product` (string); `Date` and `Country` come from the path.

DataLake against what people do today with Parquet.Net: list the files, read each one with `ParquetSerializer`, set the partition values from the path, filter in memory.

| Read | DataLake | Parquet.Net, file by file | |
|---|---:|---:|---|
| all 900 000 rows | 450 ms, 417 MB | 474 ms, 438 MB | about the same |
| one day, one country (`Date == d && Country == "PL"`) | **12.7 ms, 4.9 MB** | 421 ms, 423 MB | **33x faster**, 85x less memory |
| 5 000 ids (`Id >= a && Id < b`) | **21.1 ms, 15.6 MB** | 422 ms, 423 MB | **20x faster**, 27x less memory |

BenchmarkDotNet 0.15.8, .NET 10.0.0, Apple M1 Max, macOS 26.7.1, 31 October 2026. Local disk; on S3 or ADLS the difference grows, because skipped files and row groups are not downloaded at all.

Where it comes from: the partition filter opens 1 file of 90; the id filter opens every file but reads only the one row group whose min/max can match. Reading everything is the same work either way.

## Against pyarrow.dataset

The same 90 files, read by [`pyarrow.dataset`](https://arrow.apache.org/docs/python/dataset.html) with the same filters ([pyarrow/bench.py](pyarrow/bench.py)). pyarrow has two answers, because what you get back differs: `to_table()` is columnar Arrow memory (multithreaded C++, no objects at all), `to_pylist()` makes one Python dict per row, which is what DataLake makes in C#.

| Read | DataLake (C# objects) | pyarrow `to_table()` (columns) | pyarrow `to_pylist()` (dicts) |
|---|---:|---:|---:|
| all 900 000 rows | 450 ms | 14.6 ms | 2 299 ms |
| one day, one country | 11.9 ms | 0.7 ms | 26.6 ms |
| 5 000 ids | 19.9 ms | 5.7 ms | 17.6 ms |

DataLake: BenchmarkDotNet 0.15.8, .NET 10.0.0, macOS on the host. pyarrow 21.0.0, Python 3.12, in Docker (linux/arm64) on the same Apple M1 Max, files copied into the container. 19 November 2026.

What it says: if the result stays columnar (aggregate it, hand it to another Arrow consumer), Arrow in C++ is far ahead, and DataLake does not try to compete there; that's DuckDB's job. When every row becomes an object, DataLake is 5x faster than pyarrow for a full read, 2x faster for one partition, and about the same for the id range, where both skip the same row groups and the work left is small.

## Nested columns

200 000 orders in one file, each with a list of 3 line structs and an address struct ([NestedBenchmarks.cs](NestedBenchmarks.cs)), read into classes.

| Read | DataLake | Parquet.Net `ParquetSerializer` |
|---|---:|---:|
| 200 000 orders, 600 000 lines | **303 ms, 276 MB** | 316 ms, 299 MB |

Before 0.2 DataLake took 625 ms and 544 MB for the same file: nested values went through `object` one by one. Now each leaf column is read once into a typed array and assembled by readers compiled per class, list and map type.
