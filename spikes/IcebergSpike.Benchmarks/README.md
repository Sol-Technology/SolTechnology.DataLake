# D2: Parquet to C# classes, directly or through Arrow

```bash
dotnet run -c Release -- verify   # all three return the same 1 000 000 rows
dotnet run -c Release
```

1 000 000 rows in 10 row groups, 5 columns: `long`, `string`, `decimal?` (10 % null), `DateTime`, `double?`. The file is in memory, so this is decoding and mapping only, no I/O.

| Path | Mean | Allocated | vs Columns |
|---|---:|---:|---|
| **Columns**: Parquet.Net column arrays straight into the classes | **415 ms** | **482 MB** | 1.00 |
| ThroughArrow: the same arrays copied into an Arrow `RecordBatch`, then into the classes | 747 ms | 712 MB | 1.80× time, 1.48× memory |
| ParquetSerializer: Parquet.Net's own class mapper | 485 ms | 506 MB | 1.17× time, 1.05× memory |

BenchmarkDotNet 0.15.8, .NET 10.0.0, Apple M1 Max, macOS 26.7.1, 30 October 2026.

So the column layer (D2 option b) stays. With Parquet.Net, Arrow is always an extra copy: Parquet.Net does not decode into Arrow buffers. Arrow is still the public format for ADBC results (0.2), it is just not the path from Parquet.

Limits of this measurement:
- the Columns mapper is a hand-written loop; the real one is compiled per type, so it should not be slower
- the Arrow builders append value by value for `string`, `decimal` and timestamps; a smarter copy would be faster, but still a copy
- Arrow returns the same values in another shape: `decimal` with scale 18 (`0.010000000000000000` vs `0.01`), `DateTime` with `Kind.Utc` where Parquet.Net gives `Unspecified`
