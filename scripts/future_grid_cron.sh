#!/bin/bash
# Steadily build the Austria future power map.
# - Re-seeds monthly (1st of month) to pick up fresh ENTSO-E stats & plant data
# - Otherwise processes pending cells in batches
cd /home/exedev/austria-grid
LOG=logs/future_grid.log
if [ "$(date +%d)" = "01" ] && [ "$(date +%H)" = "02" ]; then
    echo "[$(date -Is)] monthly reseed" >> "$LOG"
    /usr/bin/python3 future_grid_planner.py seed >> "$LOG" 2>&1
fi
/usr/bin/python3 future_grid_planner.py batch 2000 >> "$LOG" 2>&1
