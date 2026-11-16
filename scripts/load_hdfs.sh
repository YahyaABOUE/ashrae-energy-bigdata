#!/usr/bin/env bash
# Load the ASHRAE CSV files into HDFS (run inside the HDP sandbox).
# Usage: ./load_hdfs.sh /path/to/csv/folder
set -euo pipefail

SRC="${1:-/root}"
BASE=/user/hive/warehouse/energy_project/raw

hdfs dfs -mkdir -p "$BASE/meter_readings" "$BASE/building_metadata" "$BASE/weather"

hdfs dfs -put -f "$SRC/train.csv"             "$BASE/meter_readings/"
hdfs dfs -put -f "$SRC/building_metadata.csv" "$BASE/building_metadata/"
hdfs dfs -put -f "$SRC/weather_train.csv"     "$BASE/weather/"

hdfs dfs -ls -R "$BASE"
