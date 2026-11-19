#!/bin/sh
# pyarrow.dataset on the same files as DatasetBenchmarks. The files are copied into the container, not mounted,
# so pyarrow reads from the container's disk like DataLake reads from the host's.
set -e
cd "$(dirname "$0")/.."
data="$(mktemp -d)/dataset"
dotnet run -c Release -- write "$data"
container=$(docker create python:3.12-slim sh -c 'pip install -q --root-user-action=ignore pyarrow==21.0.0 && python /bench/bench.py /data')
docker cp pyarrow "$container:/bench"
docker cp "$data" "$container:/data"
docker start -a "$container"
docker rm "$container" > /dev/null
rm -rf "$(dirname "$data")"
