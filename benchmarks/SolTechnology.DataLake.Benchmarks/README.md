# Dataset benchmark

```bash
dotnet run -c Release -- verify   # both ways return the same rows
dotnet run -c Release
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
