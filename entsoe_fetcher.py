#!/usr/bin/env python3
"""
ENTSO-E data fetcher and storage using entsoe-py library.
Stores historical data in SQLite for analysis and model training.

Data Resolution: 15 minutes (ENTSO-E standard for Austria)
Storage: SQLite with indexes for efficient time-series queries

Usage:
    python entsoe_fetcher.py fetch [hours]   - Fetch recent data
    python entsoe_fetcher.py backfill [days] - Backfill historical data
    python entsoe_fetcher.py stats           - Show database statistics
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from entsoe import EntsoePandasClient
import os

# Configuration
API_KEY = os.environ.get('ENTSOE_API_KEY', '35efd923-6969-4470-b2bd-0155b2254346')
DB_PATH = '/home/exedev/austria-grid/data/entsoe_data.db'
AUSTRIA_BZ = '10YAT-APG------L'

# Country codes for cross-border
COUNTRY_CODES = {
    'DE': '10Y1001A1001A83F',
    'CZ': '10YCZ-CEPS-----N', 
    'SK': '10YSK-SEPS-----K',
    'HU': '10YHU-MAVIR----U',
    'SI': '10YSI-ELES-----O',
    'IT': '10YIT-GRTN-----B',
    'CH': '10YCH-SWISSGRIDZ',
}

def init_db():
    """Initialize SQLite database with tables for ENTSO-E data."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Generation by type (15-min resolution)
    c.execute('''
        CREATE TABLE IF NOT EXISTS generation (
            timestamp TEXT,
            psr_type TEXT,
            value_mw REAL,
            fetched_at TEXT,
            PRIMARY KEY (timestamp, psr_type)
        )
    ''')
    
    # Day-ahead prices (hourly)
    c.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            timestamp TEXT PRIMARY KEY,
            price_eur_mwh REAL,
            fetched_at TEXT
        )
    ''')
    
    # Cross-border flows (15-min resolution)
    c.execute('''
        CREATE TABLE IF NOT EXISTS cross_border_flows (
            timestamp TEXT,
            country_code TEXT,
            import_mw REAL,
            export_mw REAL,
            fetched_at TEXT,
            PRIMARY KEY (timestamp, country_code)
        )
    ''')
    
    # Total load (15-min resolution)
    c.execute('''
        CREATE TABLE IF NOT EXISTS load (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL,
            fetched_at TEXT
        )
    ''')
    
    # Installed capacity by type (annual)
    c.execute('''
        CREATE TABLE IF NOT EXISTS installed_capacity (
            year INTEGER,
            psr_type TEXT,
            capacity_mw REAL,
            fetched_at TEXT,
            PRIMARY KEY (year, psr_type)
        )
    ''')
    
    # Fetch history for tracking backfills
    c.execute('''
        CREATE TABLE IF NOT EXISTS fetch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_type TEXT,
            start_time TEXT,
            end_time TEXT,
            records_fetched INTEGER,
            fetched_at TEXT
        )
    ''')
    
    # Create indexes for efficient time-series queries
    c.execute('CREATE INDEX IF NOT EXISTS idx_generation_timestamp ON generation(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_generation_psr_type ON generation(psr_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_load_timestamp ON load(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_crossborder_timestamp ON cross_border_flows(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_crossborder_country ON cross_border_flows(country_code)')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

def get_client():
    """Get ENTSO-E client."""
    return EntsoePandasClient(api_key=API_KEY)

def fetch_generation(start, end):
    """Fetch actual generation by type."""
    client = get_client()
    try:
        df = client.query_generation(AUSTRIA_BZ, start=start, end=end, psr_type=None)
        return df
    except Exception as e:
        print(f"Error fetching generation: {e}")
        return None

def fetch_load(start, end):
    """Fetch actual load."""
    client = get_client()
    try:
        df = client.query_load(AUSTRIA_BZ, start=start, end=end)
        return df
    except Exception as e:
        print(f"Error fetching load: {e}")
        return None

def fetch_prices(start, end):
    """Fetch day-ahead prices."""
    client = get_client()
    try:
        df = client.query_day_ahead_prices(AUSTRIA_BZ, start=start, end=end)
        return df
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return None

def fetch_crossborder(start, end):
    """Fetch cross-border physical flows."""
    client = get_client()
    results = {}
    
    for country, code in COUNTRY_CODES.items():
        try:
            # Import to Austria
            imp = client.query_crossborder_flows(code, AUSTRIA_BZ, start=start, end=end)
            # Export from Austria
            exp = client.query_crossborder_flows(AUSTRIA_BZ, code, start=start, end=end)
            results[country] = {'import': imp, 'export': exp}
        except Exception as e:
            print(f"Error fetching {country} flows: {e}")
            results[country] = None
    
    return results

def store_generation(df):
    """Store generation data in database.
    
    ENTSO-E returns columns like ('Solar', 'Actual Aggregated') and ('Solar', 'Actual Consumption').
    We only want 'Actual Aggregated' values (generation output, not consumption).
    """
    if df is None or df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    
    for col in df.columns:
        # Handle multi-level columns from ENTSO-E
        if isinstance(col, tuple):
            psr_type = col[0]
            value_type = col[1] if len(col) > 1 else 'Actual Aggregated'
            # Only store 'Actual Aggregated' (generation), not 'Actual Consumption'
            if 'Consumption' in value_type:
                continue
        else:
            psr_type = col
        
        for ts, val in df[col].items():
            if pd.notna(val) and val >= 0:  # Only store non-negative values
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO generation (timestamp, psr_type, value_mw, fetched_at)
                        VALUES (?, ?, ?, ?)
                    ''', (ts.isoformat(), str(psr_type), float(val), now))
                    count += 1
                except Exception as e:
                    print(f"Error storing generation: {e}")
    
    conn.commit()
    conn.close()
    return count

def store_load(df):
    """Store load data in database."""
    if df is None or df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    
    # Handle both Series and DataFrame
    if isinstance(df, pd.DataFrame):
        series = df.iloc[:, 0]
    else:
        series = df
    
    for ts, val in series.items():
        if pd.notna(val):
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO load (timestamp, load_mw, fetched_at)
                    VALUES (?, ?, ?)
                ''', (ts.isoformat(), float(val), now))
                count += 1
            except Exception as e:
                print(f"Error storing load: {e}")
    
    conn.commit()
    conn.close()
    return count

def store_prices(df):
    """Store price data in database."""
    if df is None or df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    
    for ts, val in df.items():
        if pd.notna(val):
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO prices (timestamp, price_eur_mwh, fetched_at)
                    VALUES (?, ?, ?)
                ''', (ts.isoformat(), float(val), now))
                count += 1
            except Exception as e:
                print(f"Error storing prices: {e}")
    
    conn.commit()
    conn.close()
    return count

def store_crossborder(flows_dict, timestamp_index):
    """Store cross-border flows in database."""
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    
    for country, flows in flows_dict.items():
        if flows is None:
            continue
        
        imp_series = flows.get('import')
        exp_series = flows.get('export')
        
        if imp_series is None and exp_series is None:
            continue
        
        # Get union of timestamps
        timestamps = set()
        if imp_series is not None and not imp_series.empty:
            timestamps.update(imp_series.index)
        if exp_series is not None and not exp_series.empty:
            timestamps.update(exp_series.index)
        
        for ts in timestamps:
            imp_val = imp_series.get(ts, 0) if imp_series is not None else 0
            exp_val = exp_series.get(ts, 0) if exp_series is not None else 0
            
            if pd.notna(imp_val) or pd.notna(exp_val):
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO cross_border_flows 
                        (timestamp, country_code, import_mw, export_mw, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (ts.isoformat(), country, 
                          float(imp_val) if pd.notna(imp_val) else 0,
                          float(exp_val) if pd.notna(exp_val) else 0,
                          now))
                    count += 1
                except Exception as e:
                    print(f"Error storing crossborder: {e}")
    
    conn.commit()
    conn.close()
    return count

def fetch_and_store_recent(hours=24):
    """Fetch and store recent data."""
    end = pd.Timestamp.now(tz='Europe/Vienna')
    start = end - pd.Timedelta(hours=hours)
    
    print(f"Fetching data from {start} to {end}")
    
    # Generation
    print("Fetching generation...")
    gen_df = fetch_generation(start, end)
    gen_count = store_generation(gen_df)
    print(f"  Stored {gen_count} generation records")
    
    # Load
    print("Fetching load...")
    load_df = fetch_load(start, end)
    load_count = store_load(load_df)
    print(f"  Stored {load_count} load records")
    
    # Prices
    print("Fetching prices...")
    price_df = fetch_prices(start, end)
    price_count = store_prices(price_df)
    print(f"  Stored {price_count} price records")
    
    # Cross-border
    print("Fetching cross-border flows...")
    cb_flows = fetch_crossborder(start, end)
    cb_count = store_crossborder(cb_flows, None)
    print(f"  Stored {cb_count} cross-border records")
    
    return {
        'generation': gen_count,
        'load': load_count,
        'prices': price_count,
        'crossborder': cb_count
    }

def get_latest_data():
    """Get the latest data from the database."""
    conn = sqlite3.connect(DB_PATH)
    
    # Latest generation by type
    gen_df = pd.read_sql_query('''
        SELECT psr_type, value_mw, timestamp 
        FROM generation 
        WHERE timestamp = (SELECT MAX(timestamp) FROM generation)
    ''', conn)
    
    # Latest load
    load_df = pd.read_sql_query('''
        SELECT load_mw, timestamp 
        FROM load 
        WHERE timestamp = (SELECT MAX(timestamp) FROM load)
    ''', conn)
    
    # Latest price
    price_df = pd.read_sql_query('''
        SELECT price_eur_mwh, timestamp 
        FROM prices 
        WHERE timestamp = (SELECT MAX(timestamp) FROM prices)
    ''', conn)
    
    # Latest cross-border
    cb_df = pd.read_sql_query('''
        SELECT country_code, import_mw, export_mw, timestamp 
        FROM cross_border_flows 
        WHERE timestamp = (SELECT MAX(timestamp) FROM cross_border_flows)
    ''', conn)
    
    conn.close()
    
    return {
        'generation': gen_df,
        'load': load_df,
        'prices': price_df,
        'crossborder': cb_df
    }

def backfill_historical(days=30, batch_days=7):
    """
    Backfill historical data in batches.
    ENTSO-E API has rate limits, so we fetch in chunks.
    """
    import time
    
    end = pd.Timestamp.now(tz='Europe/Vienna')
    total_results = {'generation': 0, 'load': 0, 'prices': 0, 'crossborder': 0}
    
    for i in range(0, days, batch_days):
        batch_end = end - pd.Timedelta(days=i)
        batch_start = end - pd.Timedelta(days=min(i + batch_days, days))
        
        print(f"\nBackfilling {batch_start.date()} to {batch_end.date()}...")
        
        # Generation
        print("  Fetching generation...")
        gen_df = fetch_generation(batch_start, batch_end)
        gen_count = store_generation(gen_df)
        total_results['generation'] += gen_count
        print(f"    Stored {gen_count} records")
        
        time.sleep(1)  # Rate limit
        
        # Load
        print("  Fetching load...")
        load_df = fetch_load(batch_start, batch_end)
        load_count = store_load(load_df)
        total_results['load'] += load_count
        print(f"    Stored {load_count} records")
        
        time.sleep(1)
        
        # Prices
        print("  Fetching prices...")
        price_df = fetch_prices(batch_start, batch_end)
        price_count = store_prices(price_df)
        total_results['prices'] += price_count
        print(f"    Stored {price_count} records")
        
        time.sleep(1)
        
        # Cross-border (skip for backfill - takes too long)
        # Can be enabled if needed
        
        # Log the fetch
        conn = sqlite3.connect(DB_PATH)
        conn.execute('''
            INSERT INTO fetch_history (fetch_type, start_time, end_time, records_fetched, fetched_at)
            VALUES (?, ?, ?, ?, ?)
        ''', ('backfill', batch_start.isoformat(), batch_end.isoformat(), 
              gen_count + load_count + price_count, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
        
        print(f"  Batch complete, sleeping 5s...")
        time.sleep(5)  # Longer pause between batches
    
    return total_results


def get_db_stats():
    """Get database statistics for monitoring."""
    conn = sqlite3.connect(DB_PATH)
    
    stats = {}
    
    # Generation stats
    df = pd.read_sql_query('''
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT psr_type) as num_types,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(DISTINCT date(timestamp)) as days_covered
        FROM generation
    ''', conn)
    stats['generation'] = df.iloc[0].to_dict()
    
    # Load stats
    df = pd.read_sql_query('''
        SELECT 
            COUNT(*) as total_records,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            AVG(load_mw) as avg_load_mw,
            MIN(load_mw) as min_load_mw,
            MAX(load_mw) as max_load_mw
        FROM load
    ''', conn)
    stats['load'] = df.iloc[0].to_dict()
    
    # Price stats
    df = pd.read_sql_query('''
        SELECT 
            COUNT(*) as total_records,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            AVG(price_eur_mwh) as avg_price,
            MIN(price_eur_mwh) as min_price,
            MAX(price_eur_mwh) as max_price
        FROM prices
    ''', conn)
    stats['prices'] = df.iloc[0].to_dict()
    
    # Cross-border stats
    df = pd.read_sql_query('''
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT country_code) as num_countries,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest
        FROM cross_border_flows
    ''', conn)
    stats['cross_border'] = df.iloc[0].to_dict()
    
    # Database file size
    import os
    stats['db_size_mb'] = os.path.getsize(DB_PATH) / (1024 * 1024)
    
    conn.close()
    return stats


def check_data_gaps():
    """Check for gaps in the time series data."""
    conn = sqlite3.connect(DB_PATH)
    
    # Check load data for gaps (should have 15-min intervals)
    df = pd.read_sql_query('''
        SELECT timestamp FROM load ORDER BY timestamp
    ''', conn)
    
    if df.empty:
        print("No load data found")
        return
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['gap_minutes'] = df['timestamp'].diff().dt.total_seconds() / 60
    
    # Find gaps > 15 minutes
    gaps = df[df['gap_minutes'] > 20]
    
    if not gaps.empty:
        print(f"Found {len(gaps)} gaps in load data:")
        for _, row in gaps.head(10).iterrows():
            print(f"  {row['timestamp']}: {row['gap_minutes']:.0f} min gap")
    else:
        print("No significant gaps found in load data")
    
    conn.close()
    return gaps


if __name__ == '__main__':
    import sys
    
    init_db()
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        
        if cmd == 'fetch':
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
            results = fetch_and_store_recent(hours)
            print(f"\nFetch complete: {results}")
            
        elif cmd == 'backfill':
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            print(f"Backfilling {days} days of historical data...")
            results = backfill_historical(days)
            print(f"\nBackfill complete: {results}")
            
        elif cmd == 'stats':
            stats = get_db_stats()
            print("\n=== ENTSO-E Database Statistics ===")
            for table, data in stats.items():
                if isinstance(data, dict):
                    print(f"\n{table.upper()}:")
                    for k, v in data.items():
                        print(f"  {k}: {v}")
                else:
                    print(f"\n{table}: {data}")
                    
        elif cmd == 'gaps':
            print("Checking for data gaps...")
            check_data_gaps()
            
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: python entsoe_fetcher.py [fetch|backfill|stats|gaps] [arg]")
    else:
        print("Usage:")
        print("  python entsoe_fetcher.py fetch [hours]   - Fetch recent data (default: 24h)")
        print("  python entsoe_fetcher.py backfill [days] - Backfill historical data (default: 30 days)")
        print("  python entsoe_fetcher.py stats           - Show database statistics")
        print("  python entsoe_fetcher.py gaps            - Check for data gaps")
        print("\nGetting latest data from database...")
        data = get_latest_data()
        for key, df in data.items():
            print(f"\n{key}:")
            print(df)
