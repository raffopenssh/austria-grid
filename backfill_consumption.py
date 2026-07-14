#!/usr/bin/env python3
"""Backfill 'Actual Consumption' series (pumped storage pumping etc.) since 2023.
Stored as psr_type = '<type> Consumption' in the generation table."""
import sqlite3, time, sys
from datetime import datetime, timezone
import pandas as pd
from entsoe_fetcher import get_client, AUSTRIA_BZ, DB_PATH

def store(df, conn):
    if df is None or df.empty: return 0
    now = datetime.now(timezone.utc).isoformat(); n = 0
    for col in df.columns:
        if not (isinstance(col, tuple) and len(col) > 1 and 'Consumption' in str(col[1])):
            continue
        psr = f'{col[0]} Consumption'
        for ts, val in df[col].items():
            if pd.notna(val) and val >= 0:
                conn.execute('INSERT OR IGNORE INTO generation VALUES (?,?,?,?)',
                             (ts.isoformat(), psr, float(val), now))
                n += 1
    return n

client = get_client()
conn = sqlite3.connect(DB_PATH)
start = pd.Timestamp('2023-01-01', tz='Europe/Vienna')
end = pd.Timestamp.now(tz='Europe/Vienna')
cur = start
total = 0
while cur < end:
    batch_end = min(cur + pd.Timedelta(days=14), end)
    try:
        df = client.query_generation(AUSTRIA_BZ, start=cur, end=batch_end)
        n = store(df, conn); conn.commit(); total += n
        print(f'{cur.date()} -> {batch_end.date()}: +{n} (total {total})', flush=True)
    except Exception as e:
        print(f'{cur.date()}: ERROR {e}', flush=True)
    cur = batch_end
    time.sleep(1.5)
print('done', total)
