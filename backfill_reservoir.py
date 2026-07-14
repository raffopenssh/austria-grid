#!/usr/bin/env python3
"""Backfill weekly reservoir filling levels (ENTSO-E A72, stored energy MWh)
for Austria into entsoe_data.db table reservoir_levels."""
import sqlite3, time
from datetime import datetime, timezone
import pandas as pd
from entsoe_fetcher import get_client, AUSTRIA_BZ, DB_PATH

conn = sqlite3.connect(DB_PATH)
conn.execute('''CREATE TABLE IF NOT EXISTS reservoir_levels (
    timestamp TEXT PRIMARY KEY,
    stored_energy_mwh REAL,
    fetched_at TEXT)''')
conn.commit()

client = get_client()
now = pd.Timestamp.now(tz='Europe/Vienna')
total = 0
# incremental: if history already loaded, only refresh the current year
have = conn.execute('SELECT COUNT(*) FROM reservoir_levels').fetchone()[0]
first_year = now.year if have > 400 else 2015
for year in range(first_year, now.year + 1):
    start = pd.Timestamp(f'{year}-01-01', tz='Europe/Vienna')
    end = min(pd.Timestamp(f'{year+1}-01-01', tz='Europe/Vienna'), now)
    try:
        s = client.query_aggregate_water_reservoirs_and_hydro_storage(AUSTRIA_BZ, start=start, end=end)
        ts_now = datetime.now(timezone.utc).isoformat()
        for ts, val in s.items():
            if pd.notna(val):
                conn.execute('INSERT OR REPLACE INTO reservoir_levels VALUES (?,?,?)',
                             (ts.isoformat(), float(val), ts_now))
                total += 1
        conn.commit()
        print(f'{year}: +{len(s)} weeks (total {total})', flush=True)
    except Exception as e:
        print(f'{year}: ERROR {type(e).__name__} {e}', flush=True)
    time.sleep(1.5)
print('done', total)
