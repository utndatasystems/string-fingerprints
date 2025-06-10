#!/bin/bash

if [ "$EUID" -eq 0 ]; then
  echo "Please don't run this script as root. The script will ask for your password when needed."
  exit 1
fi

# Note: no '-e' to allow graceful failure logging
set -uo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <duckdb:cli> <config_file:path>"
  exit 1
fi

DUCKDB_CLI="$1"
QUERY_DIR="$2"
OUTPUT_DIR="query-log"

if [[ ! -d "$QUERY_DIR" ]]; then
  echo "ERROR: Query directory '$QUERY_DIR' does not exist."
  exit 1
fi

test -z "${SUDO_PASSWORD:-}" && read -s -p "Please enter password for sudo: " SUDO_PASSWORD && echo ""

echo "Looking in directory: $QUERY_DIR"

find "$QUERY_DIR" -type f -name "*.sql" | while read -r sql_file; do
  sql_dir=$(dirname "$sql_file")
  db_path="${sql_dir}/temp.db"

  if [[ ! -f "$db_path" ]]; then
    echo "⚠️ WARNING: No database found at $db_path for $sql_file"
    continue
  fi

  echo "▶ Clearing cache for: $sql_file"
  log_name="/tmp/duckdb-dropcache-$(basename "$sql_file" .sql).log"
  echo "$SUDO_PASSWORD" | sudo -S bash -c 'free && sync && echo 3 > /proc/sys/vm/drop_caches && free' > "$log_name"

  echo "▶ Executing: $sql_file"
  if "$DUCKDB_CLI" "$db_path" < "$sql_file"; then
    echo "✅ Success: $sql_file"
  else
    echo "❌ ERROR: Query failed - $sql_file"
  fi
done

echo "🎉 All queries executed."
