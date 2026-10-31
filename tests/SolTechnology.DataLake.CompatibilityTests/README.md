# Compatibility tests

Real files from other writers, read by DataLake and compared with pyarrow.

* `data/parquet-testing/`: [apache/parquet-testing](https://github.com/apache/parquet-testing) at `eedfad3f086678d282a00d8ff3cf8932d5d29af5`, the `data/*.parquet` files with their `*.csv` expectations and `bad_data/`. Apache License 2.0, see `LICENSE.txt` there. Encrypted, geospatial and variant files are not copied.
* `data/parquet-dotnet/`: test files of [aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) at `658e1cafebdd1d80d6833c40b0f387d0769898c8`, from `src/Parquet.Test/data` (`issues/x` and `real/x` saved as `issues_x` and `real_x`). MIT, see `LICENSE` there.
* `data/expected/`: what pyarrow 21 reads from each file: row count and the first 20 rows in a canonical JSON form. Regenerate with `scripts/expected.sh` (Docker) after adding files.

Tests:

* `CorpusTests`: every file through the untyped reader, compared with `data/expected`. Files DataLake can't read are in `KnownGaps.cs` with the reason; their tests fail when the file starts to work.
* `BadDataTests`: the `bad_data` files end with an `InvalidDataException` that names the file.
* `TypedTests`: real files into classes, as applications read them.

```
dotnet test tests/SolTechnology.DataLake.CompatibilityTests
```
