#!/usr/bin/env python3
"""
ENTSO-E data fetcher and storage using entsoe-py library.
Stores historical data in SQLite for analysis and model training.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
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
    
    # Generation by type
    c.execute('''
        CREATE TABLE IF NOT EXISTS generation (
            timestamp TEXT,
            psr_type TEXT,
            value_mw REAL,
            fetched_at TEXT,
            PRIMARY KEY (timestamp, psr_type)
        )
    ''')
    
    # Day-ahead prices
    c.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            timestamp TEXT PRIMARY KEY,
            price_eur_mwh REAL,
            fetched_at TEXT
        )
    ''')
    
    # Cross-border flows
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
    
    # Total load
    c.execute('''
        CREATE TABLE IF NOT EXISTS load (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL,
            fetched_at TEXT
        )
    ''')
    
    # Installed capacity by type (less frequent updates)
    c.execute('''
        CREATE TABLE IF NOT EXISTS installed_capacity (
            year INTEGER,
            psr_type TEXT,
            capacity_mw REAL,
            fetched_at TEXT,
            PRIMARY KEY (year, psr_type)
        )
    ''')
    
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
    """Store generation data in database."""
    if df is None or df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    now = datetime.utcnow().isoformat()
    count = 0
    
    for col in df.columns:
        psr_type = col[0] if isinstance(col, tuple) else col
        for ts, val in df[col].items():
            if pd.notna(val):
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
    now = datetime.utcnow().isoformat()
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
    now = datetime.utcnow().isoformat()
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
    now = datetime.utcnow().isoformat()
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

if __name__ == '__main__':
    import sys
    
    init_db()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'fetch':
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
        results = fetch_and_store_recent(hours)
        print(f"\nFetch complete: {results}")
    else:
        print("Usage: python entsoe_fetcher.py fetch [hours]")
        print("\nGetting latest data from database...")
        data = get_latest_data()
        for key, df in data.items():
            print(f"\n{key}:")
            print(df)
