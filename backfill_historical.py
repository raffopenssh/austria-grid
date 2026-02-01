#!/usr/bin/env python3
"""
Backfill multiple years of ENTSO-E data for ML model training.
Fetches data in weekly batches with rate limiting.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from entsoe import EntsoePandasClient
import time
import sys

API_KEY = '35efd923-6969-4470-b2bd-0155b2254346'
DB_PATH = '/home/exedev/austria-grid/data/entsoe_data.db'
AUSTRIA_BZ = '10YAT-APG------L'

def get_client():
    return EntsoePandasClient(api_key=API_KEY)

def store_prices(df, conn):
    """Store price data."""
    if df is None or df.empty:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for ts, val in df.items():
        if pd.notna(val):
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO prices (timestamp, price_eur_mwh, fetched_at)
                    VALUES (?, ?, ?)
                ''', (ts.isoformat(), float(val), now))
                count += 1
            except:
                pass
    return count

def store_load(df, conn):
    """Store load data."""
    if df is None or df.empty:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    series = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
    for ts, val in series.items():
        if pd.notna(val):
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO load (timestamp, load_mw, fetched_at)
                    VALUES (?, ?, ?)
                ''', (ts.isoformat(), float(val), now))
                count += 1
            except:
                pass
    return count

def store_generation(df, conn):
    """Store generation data - only Actual Aggregated values."""
    if df is None or df.empty:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for col in df.columns:
        if isinstance(col, tuple):
            psr_type = col[0]
            if 'Consumption' in str(col[1]):
                continue
        else:
            psr_type = col
        
        for ts, val in df[col].items():
            if pd.notna(val) and val >= 0:
                try:
                    conn.execute('''
                        INSERT OR IGNORE INTO generation (timestamp, psr_type, value_mw, fetched_at)
                        VALUES (?, ?, ?, ?)
                    ''', (ts.isoformat(), str(psr_type), float(val), now))
                    count += 1
                except:
                    pass
    return count

def backfill_year(year, conn):
    """Backfill a full year of data."""
    client = get_client()
    
    start_date = pd.Timestamp(f'{year}-01-01', tz='Europe/Vienna')
    end_date = pd.Timestamp(f'{year}-12-31 23:59', tz='Europe/Vienna')
    
    # Don't fetch future data
    now = pd.Timestamp.now(tz='Europe/Vienna')
    if end_date > now:
        end_date = now
    if start_date > now:
        print(f"  Year {year} is in the future, skipping")
        return
    
    print(f"\n{'='*50}")
    print(f"Backfilling year {year}: {start_date.date()} to {end_date.date()}")
    print(f"{'='*50}")
    
    total = {'prices': 0, 'load': 0, 'generation': 0}
    
    # Process in weekly batches
    current = start_date
    while current < end_date:
        batch_end = min(current + pd.Timedelta(days=7), end_date)
        
        print(f"\n  {current.date()} to {batch_end.date()}...", end=" ", flush=True)
        
        try:
            # Prices
            prices = client.query_day_ahead_prices(AUSTRIA_BZ, start=current, end=batch_end)
            p_count = store_prices(prices, conn)
            total['prices'] += p_count
            print(f"P:{p_count}", end=" ", flush=True)
            time.sleep(0.5)
            
            # Load
            load = client.query_load(AUSTRIA_BZ, start=current, end=batch_end)
            l_count = store_load(load, conn)
            total['load'] += l_count
            print(f"L:{l_count}", end=" ", flush=True)
            time.sleep(0.5)
            
            # Generation
            gen = client.query_generation(AUSTRIA_BZ, start=current, end=batch_end)
            g_count = store_generation(gen, conn)
            total['generation'] += g_count
            print(f"G:{g_count}", end="", flush=True)
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error: {e}")
        
        conn.commit()
        current = batch_end
        time.sleep(2)  # Rate limiting between batches
    
    print(f"\n\nYear {year} complete: {total}")
    return total

def main():
    years = [2023, 2024, 2025, 2026]  # Fetch 3+ years of data
    
    if len(sys.argv) > 1:
        years = [int(y) for y in sys.argv[1:]]
    
    print(f"Backfilling years: {years}")
    print("This will take 30-60 minutes per year...")
    
    conn = sqlite3.connect(DB_PATH)
    
    for year in years:
        try:
            backfill_year(year, conn)
        except Exception as e:
            print(f"Error on year {year}: {e}")
            continue
    
    conn.close()
    
    # Show final stats
    print("\n" + "="*50)
    print("BACKFILL COMPLETE - Final Statistics:")
    print("="*50)
    
    conn = sqlite3.connect(DB_PATH)
    for table in ['prices', 'load', 'generation']:
        cur = conn.execute(f"SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM {table}")
        cnt, mn, mx = cur.fetchone()
        print(f"  {table}: {cnt:,} records ({mn[:10] if mn else 'N/A'} to {mx[:10] if mx else 'N/A'})")
    conn.close()

if __name__ == '__main__':
    main()
