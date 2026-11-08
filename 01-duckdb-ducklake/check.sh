#!/usr/bin/env bash
# Replays the tutorial's example on the deployed lake, and checks each result.
# Run it from this directory after `terraform apply`. It works on a table of its own, and drops
# it at the end.
set -euo pipefail
cd "$(dirname "$0")"

BUCKET=$(terraform output -raw bucket)
CELLAR_HOST=$(terraform output -raw cellar_host)
CELLAR_KEY_ID=$(terraform output -raw cellar_key_id)
CELLAR_KEY_SECRET=$(terraform output -raw cellar_key_secret)
CATALOG=$(terraform output -raw postgresql_conninfo)
URI=$(terraform output -raw postgresql_uri)
SERVER=$(terraform output -raw server_host)
TOKEN=$(terraform output -raw quack_token)
T=check_$(date +%s)

LAKE_SQL=$(mktemp)
trap 'rm -f "$LAKE_SQL"' EXIT
cat > "$LAKE_SQL" <<SQL
CREATE SECRET cellar (TYPE s3, KEY_ID '$CELLAR_KEY_ID', SECRET '$CELLAR_KEY_SECRET',
                      ENDPOINT '$CELLAR_HOST', URL_STYLE 'path', REGION 'us-east-1');
ATTACH 'ducklake:postgres:$CATALOG' AS lake (DATA_PATH 's3://$BUCKET/lake/', METADATA_SCHEMA 'ducklake');
USE lake;
SQL
lake() { cat "$LAKE_SQL" - | duckdb -csv -noheader | tail -1; }
# The aws command line needs a region value; Cellar accepts us-east-1.
s3() { AWS_ACCESS_KEY_ID=$CELLAR_KEY_ID AWS_SECRET_ACCESS_KEY=$CELLAR_KEY_SECRET AWS_DEFAULT_REGION=us-east-1 aws s3 "$@" --endpoint-url "https://$CELLAR_HOST"; }
failures=0
check() { # check <label> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "OK   $1"; else echo "BAD  $1: expected $2, got $3"; failures=$((failures + 1)); fi
}

# 1 and 2. DuckDB attaches the lake, creates a table and writes a few rows.
lake <<SQL > /dev/null
CALL lake.set_option('data_inlining_row_limit', 0);
CREATE TABLE $T (writer INTEGER, n INTEGER);
INSERT INTO $T VALUES (0, 1), (0, 2), (0, 3);
SQL
check "a table of 3 rows" 3 "$(echo "SELECT count(*) FROM $T;" | lake)"
files=$(s3 ls "s3://$BUCKET/lake/main/$T/" | grep -c parquet || true)
check "its rows are a Parquet file on Cellar" 1 "$files"

# 3. Two processes write to the same table at once, ten commits each.
for writer in 1 2; do
  for i in $(seq 10); do echo "INSERT INTO $T VALUES ($writer, $i);"; done | lake > /dev/null &
done
wait
check "no row lost by two writers at once" 23 "$(echo "SELECT count(*) FROM $T;" | lake)"

# 4. The DuckDB server, another DuckDB on Clever Cloud, reads the same table. Skipped without it.
if [ -n "$SERVER" ]; then
  remote=$(duckdb -csv -noheader -c "ATTACH 'quack:$SERVER:443' AS server (TOKEN '$TOKEN'); SELECT count(*) FROM server.$T;" 2>/dev/null | tail -1 || true)
  check "the server reads the same 23 rows" 23 "$remote"
  refused=$(duckdb -c "ATTACH 'quack:$SERVER:443' AS server (TOKEN 'not-the-token');" 2>&1 | grep -c 'Authentication failed' || true)
  check "the server refuses a wrong token" 1 "$refused"
  escaped=$(duckdb -c "FROM quack_query('quack:$SERVER:443', 'SELECT * FROM read_text(''/proc/self/environ'')', token = '$TOKEN');" 2>&1 | grep -c 'Permission Error' || true)
  check "the server refuses to read its environment" 1 "$escaped"
fi

# 5. DuckDB reads the table as it was before the last write.
version=$(echo "SELECT max(snapshot_id) FROM lake.snapshots();" | lake)
echo "INSERT INTO $T VALUES (3, 1);" | lake > /dev/null
check "24 rows now" 24 "$(echo "SELECT count(*) FROM $T;" | lake)"
check "23 rows before the last write" 23 "$(echo "SELECT count(*) FROM $T AT (VERSION => $version);" | lake)"

# 6. An aggregate is copied to the reporting schema, which reporting tools read.
lake <<SQL > /dev/null
ATTACH '$CATALOG' AS pg (TYPE postgres);
CALL postgres_execute('pg', 'CREATE SCHEMA IF NOT EXISTS reporting');
CREATE OR REPLACE TABLE pg.reporting.$T AS SELECT writer, count(*) AS n FROM $T GROUP BY writer;
SQL
check "PostgreSQL reads the copy in reporting" "0:3 1:10 2:10 3:1" \
  "$(psql "$URI" -tA -F: -c "SELECT writer, n FROM reporting.$T ORDER BY writer" | tr '\n' ' ' | sed 's/ $//')"

# Clean up the check's tables.
psql "$URI" -q -c "DROP TABLE reporting.$T"
echo "DROP TABLE $T;" | lake > /dev/null

[ "$failures" = 0 ] && echo "All checks passed." || { echo "$failures check(s) failed."; exit 1; }
