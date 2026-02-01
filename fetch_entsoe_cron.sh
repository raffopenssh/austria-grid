#!/bin/bash
# ENTSO-E data fetcher cron job
# Runs every 5 minutes to fetch latest 15-minute resolution data
# Data is stored in SQLite for historical analysis and model training

cd /home/exedev/austria-grid

# Create log dir if needed
mkdir -p /home/exedev/austria-grid/logs

# Fetch last 2 hours of data (overlaps ensure no gaps)
/usr/bin/python3 entsoe_fetcher.py fetch 2 >> /home/exedev/austria-grid/logs/entsoe_fetch.log 2>&1

# Rotate log if > 10MB
if [ -f /home/exedev/austria-grid/logs/entsoe_fetch.log ]; then
    size=$(stat -f%z /home/exedev/austria-grid/logs/entsoe_fetch.log 2>/dev/null || stat -c%s /home/exedev/austria-grid/logs/entsoe_fetch.log 2>/dev/null || echo 0)
    if [ "$size" -gt 10485760 ]; then
        mv /home/exedev/austria-grid/logs/entsoe_fetch.log /home/exedev/austria-grid/logs/entsoe_fetch.log.old
    fi
fi
