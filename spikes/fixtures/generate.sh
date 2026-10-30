#!/bin/sh
# Regenerates the Iceberg test tables in spikes/fixtures/{warehouse,expected} with PyIceberg in Docker.
# Table metadata points to file:///warehouse/...; readers map that prefix to spikes/fixtures/warehouse.
set -e
cd "$(dirname "$0")"
rm -rf warehouse expected
docker run --rm \
  -v "$PWD:/out" \
  python:3.12-slim \
  sh -c 'pip install -q --root-user-action=ignore "pyiceberg[sql-sqlite,pyarrow]==0.10.0" && python /out/generate.py'
