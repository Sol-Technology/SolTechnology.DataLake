# PyIceberg as the reference implementation: seed.py writes tables through the REST catalog,
# oracle.py dumps the rows PyIceberg reads. interop.py reads what DataLake writes with pyarrow and DuckDB.
# Built once per test run by Testcontainers.
FROM python:3.12-slim
RUN pip install --no-cache-dir --root-user-action=ignore "pyiceberg[pyarrow]==0.10.0" "duckdb==2.0.0" "pytz==2025.2" && mkdir /out \
    && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
COPY seed.py oracle.py interop.py /scripts/
