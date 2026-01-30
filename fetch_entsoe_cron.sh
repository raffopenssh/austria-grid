#!/bin/bash
cd /home/exedev/austria-grid
/usr/bin/python3 entsoe_fetcher.py fetch 4 >> /home/exedev/austria-grid/logs/entsoe_fetch.log 2>&1
