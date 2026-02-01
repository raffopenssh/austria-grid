#!/usr/bin/env python3
"""
Simple price forecasting model based on historical patterns.
Uses hour-of-day and day-of-week patterns from historical data.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

DB_PATH = '/home/exedev/austria-grid/data/entsoe_data.db'


def get_historical_patterns():
    """Calculate average price patterns from historical data."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get all price data
    df = pd.read_sql_query("""
        SELECT timestamp, price_eur_mwh 
        FROM prices 
        WHERE price_eur_mwh > 0
        ORDER BY timestamp
    """, conn)
    conn.close()
    
    if df.empty:
        return None
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    df['dayofweek'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['dayofweek'] >= 5
    
    # Calculate hourly patterns for weekday and weekend
    weekday_pattern = df[~df['is_weekend']].groupby('hour')['price_eur_mwh'].agg(['mean', 'std']).round(2)
    weekend_pattern = df[df['is_weekend']].groupby('hour')['price_eur_mwh'].agg(['mean', 'std']).round(2)
    
    # Overall stats
    stats = {
        'total_records': len(df),
        'date_range': f"{df['timestamp'].min().date()} to {df['timestamp'].max().date()}",
        'overall_mean': round(df['price_eur_mwh'].mean(), 2),
        'overall_std': round(df['price_eur_mwh'].std(), 2),
        'weekday_mean': round(df[~df['is_weekend']]['price_eur_mwh'].mean(), 2),
        'weekend_mean': round(df[df['is_weekend']]['price_eur_mwh'].mean(), 2),
    }
    
    return {
        'weekday': weekday_pattern.to_dict(),
        'weekend': weekend_pattern.to_dict(),
        'stats': stats
    }


def forecast_prices(hours_ahead=48):
    """Forecast prices for the next N hours based on historical patterns."""
    patterns = get_historical_patterns()
    if not patterns:
        return None
    
    now = datetime.now(timezone.utc)
    forecasts = []
    
    for h in range(hours_ahead):
        future_time = now + timedelta(hours=h)
        hour = future_time.hour
        is_weekend = future_time.weekday() >= 5
        
        if is_weekend:
            mean = patterns['weekend']['mean'].get(hour, patterns['stats']['overall_mean'])
            std = patterns['weekend']['std'].get(hour, patterns['stats']['overall_std'])
        else:
            mean = patterns['weekday']['mean'].get(hour, patterns['stats']['overall_mean'])
            std = patterns['weekday']['std'].get(hour, patterns['stats']['overall_std'])
        
        forecasts.append({
            'timestamp': future_time.isoformat(),
            'hour': hour,
            'is_weekend': is_weekend,
            'predicted_price': round(mean, 2),
            'uncertainty_low': round(mean - std, 2),
            'uncertainty_high': round(mean + std, 2),
        })
    
    return {
        'generated_at': now.isoformat(),
        'model': 'historical_pattern_v1',
        'forecasts': forecasts,
        'patterns': patterns
    }


def get_price_generation_correlation():
    """Analyze correlation between generation mix and prices."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get prices
    prices = pd.read_sql_query("SELECT timestamp, price_eur_mwh FROM prices", conn)
    prices['timestamp'] = pd.to_datetime(prices['timestamp'])
    prices.set_index('timestamp', inplace=True)
    
    # Get generation
    gen = pd.read_sql_query("SELECT timestamp, psr_type, value_mw FROM generation", conn)
    gen['timestamp'] = pd.to_datetime(gen['timestamp'])
    gen_pivot = gen.pivot_table(index='timestamp', columns='psr_type', values='value_mw')
    
    # Get load
    load = pd.read_sql_query("SELECT timestamp, load_mw FROM load", conn)
    load['timestamp'] = pd.to_datetime(load['timestamp'])
    load.set_index('timestamp', inplace=True)
    
    conn.close()
    
    # Merge all
    merged = prices.join(gen_pivot, how='inner').join(load, how='inner')
    
    if merged.empty or len(merged) < 100:
        return {'error': 'Not enough data for correlation analysis'}
    
    # Calculate correlations
    correlations = {}
    for col in merged.columns:
        if col not in ['price_eur_mwh']:
            corr = merged['price_eur_mwh'].corr(merged[col])
            if pd.notna(corr):
                correlations[col] = round(corr, 3)
    
    # Sort by absolute correlation
    sorted_corr = dict(sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True))
    
    return {
        'correlations': sorted_corr,
        'sample_size': len(merged),
        'interpretation': {
            'positive': 'Higher values → Higher prices',
            'negative': 'Higher values → Lower prices'
        }
    }


if __name__ == '__main__':
    import json
    
    print("=== PRICE PATTERNS ===")
    patterns = get_historical_patterns()
    if patterns:
        print(f"Data: {patterns['stats']['date_range']}")
        print(f"Records: {patterns['stats']['total_records']}")
        print(f"Overall mean: €{patterns['stats']['overall_mean']}/MWh")
        print(f"Weekday mean: €{patterns['stats']['weekday_mean']}/MWh")
        print(f"Weekend mean: €{patterns['stats']['weekend_mean']}/MWh")
    
    print("\n=== 24-HOUR FORECAST ===")
    forecast = forecast_prices(24)
    if forecast:
        for f in forecast['forecasts'][:12]:  # Show first 12 hours
            print(f"{f['timestamp'][:16]}: €{f['predicted_price']}/MWh (±{f['uncertainty_high']-f['predicted_price']:.0f})")
    
    print("\n=== PRICE-GENERATION CORRELATIONS ===")
    corr = get_price_generation_correlation()
    if 'correlations' in corr:
        for k, v in list(corr['correlations'].items())[:8]:
            direction = "↑" if v > 0 else "↓"
            print(f"  {k}: {v:+.3f} {direction}")
