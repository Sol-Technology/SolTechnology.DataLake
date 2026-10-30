# PyIceberg as the reference implementation: seed.py writes tables through the REST catalog,
# oracle.py dumps the rows PyIceberg reads. Built once per test run by Testcontainers.
FROM python:3.12-slim
RUN pip install --no-cache-dir --root-user-action=ignore "pyiceberg[pyarrow]==0.10.0" && mkdir /out
COPY seed.py oracle.py /scripts/
