#!/usr/bin/env bash
# Download NYC TLC Yellow Taxi parquet into data/raw/ for LOCAL inspection/testing.
# The REAL pipeline pulls these straight into Databricks (see notebooks/01_bronze.py) —
# this local copy is just so you can peek at the schema and run unit tests offline.
#
# Usage:   bash scripts/download_data.sh
# Each monthly file is ~50 MB / ~3M rows. A full year (default below) is ~600 MB / ~40M rows.
set -euo pipefail

BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
DEST="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
mkdir -p "$DEST"

YEARS="2023"                                        # add "2023 2024" to roughly double the data
MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"

for y in $YEARS; do
  for m in $MONTHS; do
    f="yellow_tripdata_${y}-${m}.parquet"
    if [[ -f "$DEST/$f" ]]; then echo "skip  $f (already here)"; continue; fi
    echo "get   $f"
    curl -fL --retry 3 -o "$DEST/$f" "$BASE/$f"
  done
done

echo "Done."
ls -lh "$DEST"
