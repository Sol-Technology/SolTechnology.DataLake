#!/bin/sh
# Regenerates data/expected/*.json with pyarrow in Docker. Run after adding files to data/.
set -e
cd "$(dirname "$0")/.."
docker run --rm -v "$PWD:/work" python:3.12-slim sh -c 'pip install -q --root-user-action=ignore pyarrow==21.0.0 pandas==2.3.3 && python /work/scripts/expected.py'
