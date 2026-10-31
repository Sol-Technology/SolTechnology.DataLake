#!/bin/sh
# Regenerates orders.avro and orders.avro.json with fastavro in Docker.
set -e
cd "$(dirname "$0")"
docker run --rm -v "$PWD:/out" python:3.12-slim sh -c 'pip install -q --root-user-action=ignore fastavro==1.12.1 && python /out/generate_avro.py'
