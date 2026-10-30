#!/bin/sh
# Makes shop.orders_mor: a copy of shop.orders changed by Trino with DELETE and UPDATE, so the table has
# positional delete files (merge-on-read). Then PyIceberg dumps the rows it reads to expected/orders_mor.json.
# Needs the stack from docker-compose.yaml running (docker compose up -d).
set -e
cd "$(dirname "$0")"
docker compose --profile trino up -d --wait trino

sql() { docker compose exec -T trino trino --catalog lake --schema shop --execute "$1"; }
until sql "SELECT 1" >/dev/null 2>&1; do sleep 3; done
sql "DROP TABLE IF EXISTS orders_mor"
sql "CREATE TABLE orders_mor WITH (partitioning = ARRAY['country'], format_version = 2) AS SELECT * FROM orders"
sql "DELETE FROM orders_mor WHERE id IN (1, 12)"
sql "UPDATE orders_mor SET amount = 99.99 WHERE id = 4"

docker compose run --rm --no-deps -v "$PWD/oracle.py:/seed/oracle.py:ro" seed \
  sh -c "pip install -q --root-user-action=ignore 'pyiceberg[pyarrow]==0.10.0' && python /seed/oracle.py orders_mor"
