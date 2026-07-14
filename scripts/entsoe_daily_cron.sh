#!/bin/bash
# Daily incremental fetch: per-plant generation (A73, ~5-day lag) and
# weekly reservoir levels (A72). Both scripts are resumable/idempotent.
cd /home/exedev/austria-grid
/usr/bin/python3 backfill_per_plant.py >> logs/per_plant_cron.log 2>&1
/usr/bin/python3 backfill_reservoir.py >> logs/reservoir_cron.log 2>&1
