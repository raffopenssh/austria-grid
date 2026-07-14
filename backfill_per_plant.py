#!/usr/bin/env python3
"""Backfill ENTSO-E A73 'Actual Generation per Generation Unit' for Austria
into entsoe_data.db table plant_generation. 15-min resolution.
Covers pumped storage units (Malta, Reisseck 2, Limberg II/III, Haeusling),
reservoir units, gas blocks and (earlier years) Danube run-of-river chain.
Resumable: tracks done days in plant_generation_days."""
import sqlite3, time, sys
from datetime import datetime, timezone
import pandas as pd
from entsoe_fetcher import get_client, AUSTRIA_BZ, DB_PATH

conn = sqlite3.connect(DB_PATH, timeout=60)
conn.executescript('''
CREATE TABLE IF NOT EXISTS plant_generation (
    timestamp TEXT,
    plant TEXT,
    psr_type TEXT,
    value_mw REAL,
    fetched_at TEXT,
    PRIMARY KEY (timestamp, plant)
);
CREATE INDEX IF NOT EXISTS idx_pg_plant ON plant_generation(plant);
CREATE INDEX IF NOT EXISTS idx_pg_ts ON plant_generation(timestamp);
CREATE TABLE IF NOT EXISTS plant_generation_days (day TEXT PRIMARY KEY, records INTEGER);
''')
conn.commit()

client = get_client()
start = pd.Timestamp('2023-01-01', tz='Europe/Vienna')
end = pd.Timestamp.now(tz='Europe/Vienna').normalize() - pd.Timedelta(days=5)  # publication lag
done = {r[0] for r in conn.execute('SELECT day FROM plant_generation_days')}
days = pd.date_range(start, end, freq='D')
todo = [d for d in days if d.strftime('%Y-%m-%d') not in done]
print(f'{len(todo)} days to fetch', flush=True)

total = 0
for i, day in enumerate(todo):
    key = day.strftime('%Y-%m-%d')
    try:
        df = client.query_generation_per_plant(AUSTRIA_BZ, start=day, end=day + pd.Timedelta(days=1))
        now = datetime.now(timezone.utc).isoformat()
        n = 0
        for col in df.columns:
            name, psr, mode = col[0], col[1], col[2]
            if 'Consumption' in mode:
                name = f'{name} (pumping)'
            for ts, val in df[col].items():
                if pd.notna(val):
                    conn.execute('INSERT OR REPLACE INTO plant_generation VALUES (?,?,?,?,?)',
                                 (ts.isoformat(), name, psr, float(val), now))
                    n += 1
        conn.execute('INSERT OR REPLACE INTO plant_generation_days VALUES (?,?)', (key, n))
        conn.commit()
        total += n
        if i % 20 == 0:
            print(f'{key}: +{n} ({i+1}/{len(todo)}, total {total})', flush=True)
    except Exception as e:
        etype = type(e).__name__
        if etype == 'NoMatchingDataError':
            conn.execute('INSERT OR REPLACE INTO plant_generation_days VALUES (?,0)', (key,))
            conn.commit()
        else:
            print(f'{key}: ERROR {etype} {str(e)[:80]}', flush=True)
            time.sleep(10)
    time.sleep(1.0)
print('done', total, flush=True)
