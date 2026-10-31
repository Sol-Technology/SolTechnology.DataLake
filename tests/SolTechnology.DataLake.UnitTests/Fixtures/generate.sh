#!/bin/sh
# Regenerates nested.parquet and nested.json with pyarrow in Docker.
set -e
cd "$(dirname "$0")"
docker run --rm -v "$PWD:/out" python:3.12-slim sh -c 'pip install -q --root-user-action=ignore pyarrow==21.0.0 && python /out/generate.py'
