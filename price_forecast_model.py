#!/usr/bin/env python3
"""
XGBoost-based electricity price forecasting model for Austria.

Features:
- Temporal: hour, day of week, month, is_weekend, is_holiday
- Lag features: previous hours/days prices
- Generation mix: solar, wind, hydro, gas
- Load: current and forecasted
- Seasonal patterns

Austrian holidays included for accurate forecasting.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import xgboost as xgb
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import os
import json

DB_PATH = '/home/exedev/austria-grid/data/entsoe_data.db'
MODEL_PATH = '/home/exedev/austria-grid/data/price_model.pkl'
METADATA_PATH = '/home/exedev/austria-grid/data/price_model_meta.json'

# Austrian public holidays (fixed dates - movable ones need calculation)
AUSTRIAN_HOLIDAYS = {
    # Fixed holidays
    (1, 1): "Neujahr",
    (1, 6): "Heilige Drei Könige", 
    (5, 1): "Staatsfeiertag",
    (8, 15): "Mariä Himmelfahrt",
    (10, 26): "Nationalfeiertag",
    (11, 1): "Allerheiligen",
    (12, 8): "Mariä Empfängnis",
    (12, 25): "Christtag",
    (12, 26): "Stefanitag",
}

def get_easter_dates(year):
    """Calculate Easter Sunday using Anonymous Gregorian algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return pd.Timestamp(year, month, day)

def get_movable_holidays(year):
    """Get movable Austrian holidays based on Easter."""
    easter = get_easter_dates(year)
    return {
        easter + timedelta(days=1): "Ostermontag",
        easter + timedelta(days=39): "Christi Himmelfahrt",
        easter + timedelta(days=50): "Pfingstmontag",
        easter + timedelta(days=60): "Fronleichnam",
    }

def is_holiday(dt):
    """Check if a date is an Austrian holiday."""
    # Check fixed holidays
    if (dt.month, dt.day) in AUSTRIAN_HOLIDAYS:
        return True
    # Check movable holidays
    movable = get_movable_holidays(dt.year)
    for holiday_date in movable.keys():
        if dt.date() == holiday_date.date():
            return True
    return False

def is_holiday_period(dt):
    """Check if date is in a typical holiday period (Christmas, summer)."""
    # Christmas period (Dec 23 - Jan 2)
    if (dt.month == 12 and dt.day >= 23) or (dt.month == 1 and dt.day <= 2):
        return True
    # Summer holiday period (mid-July to mid-August) - reduced industrial load
    if dt.month == 8 or (dt.month == 7 and dt.day >= 15):
        return True
    return False


def load_training_data():
    """Load and prepare training data from database."""
    conn = sqlite3.connect(DB_PATH)
    
    # Load prices
    prices = pd.read_sql_query("""
        SELECT timestamp, price_eur_mwh 
        FROM prices 
        WHERE price_eur_mwh IS NOT NULL
        ORDER BY timestamp
    """, conn)
    prices['timestamp'] = pd.to_datetime(prices['timestamp'], utc=True)
    prices.set_index('timestamp', inplace=True)
    prices = prices.resample('h').mean()  # Ensure hourly
    
    # Load load data
    load_df = pd.read_sql_query("""
        SELECT timestamp, load_mw 
        FROM load 
        WHERE load_mw IS NOT NULL
        ORDER BY timestamp
    """, conn)
    load_df['timestamp'] = pd.to_datetime(load_df['timestamp'], utc=True)
    load_df.set_index('timestamp', inplace=True)
    load_df = load_df.resample('h').mean()
    
    # Load generation by type
    gen = pd.read_sql_query("""
        SELECT timestamp, psr_type, value_mw 
        FROM generation 
        WHERE value_mw > 0
        ORDER BY timestamp
    """, conn)
    gen['timestamp'] = pd.to_datetime(gen['timestamp'], utc=True)
    gen_pivot = gen.pivot_table(index='timestamp', columns='psr_type', values='value_mw', aggfunc='mean')
    gen_pivot = gen_pivot.resample('h').mean()
    
    conn.close()
    
    # Merge all data
    df = prices.join(load_df, how='inner')
    df = df.join(gen_pivot, how='left')
    
    # Fill missing generation values with 0
    df = df.fillna(0)
    
    print(f"Loaded {len(df)} hourly records from {df.index.min()} to {df.index.max()}")
    return df


def create_features(df):
    """Create features for the model."""
    features = pd.DataFrame(index=df.index)
    
    # Temporal features
    features['hour'] = df.index.hour
    features['dayofweek'] = df.index.dayofweek
    features['month'] = df.index.month
    features['dayofyear'] = df.index.dayofyear
    features['weekofyear'] = df.index.isocalendar().week.astype(int)
    features['is_weekend'] = (df.index.dayofweek >= 5).astype(int)
    features['is_holiday'] = df.index.to_series().apply(is_holiday).astype(int)
    features['is_holiday_period'] = df.index.to_series().apply(is_holiday_period).astype(int)
    
    # Cyclical encoding for hour and month (captures continuity)
    features['hour_sin'] = np.sin(2 * np.pi * features['hour'] / 24)
    features['hour_cos'] = np.cos(2 * np.pi * features['hour'] / 24)
    features['month_sin'] = np.sin(2 * np.pi * features['month'] / 12)
    features['month_cos'] = np.cos(2 * np.pi * features['month'] / 12)
    features['dow_sin'] = np.sin(2 * np.pi * features['dayofweek'] / 7)
    features['dow_cos'] = np.cos(2 * np.pi * features['dayofweek'] / 7)
    
    # Load features
    if 'load_mw' in df.columns:
        features['load_mw'] = df['load_mw']
        features['load_mw_lag1'] = df['load_mw'].shift(1)
        features['load_mw_lag24'] = df['load_mw'].shift(24)
        features['load_mw_rolling_24h'] = df['load_mw'].rolling(24).mean()
    
    # Generation features
    gen_cols = ['Solar', 'Wind Onshore', 'Hydro Run-of-river and poundage', 
                'Hydro Water Reservoir', 'Fossil Gas', 'Hydro Pumped Storage']
    for col in gen_cols:
        if col in df.columns:
            safe_name = col.replace(' ', '_').replace('-', '_')
            features[f'gen_{safe_name}'] = df[col]
    
    # Total renewables
    renewable_cols = ['Solar', 'Wind Onshore', 'Hydro Run-of-river and poundage', 'Hydro Water Reservoir']
    available_renewable = [c for c in renewable_cols if c in df.columns]
    if available_renewable:
        features['total_renewables'] = df[available_renewable].sum(axis=1)
    
    # Price lag features (crucial for time series)
    features['price_lag_1h'] = df['price_eur_mwh'].shift(1)
    features['price_lag_2h'] = df['price_eur_mwh'].shift(2)
    features['price_lag_3h'] = df['price_eur_mwh'].shift(3)
    features['price_lag_24h'] = df['price_eur_mwh'].shift(24)  # Same hour yesterday
    features['price_lag_48h'] = df['price_eur_mwh'].shift(48)  # Same hour 2 days ago
    features['price_lag_168h'] = df['price_eur_mwh'].shift(168)  # Same hour last week
    
    # Rolling statistics
    features['price_rolling_24h_mean'] = df['price_eur_mwh'].rolling(24).mean()
    features['price_rolling_24h_std'] = df['price_eur_mwh'].rolling(24).std()
    features['price_rolling_168h_mean'] = df['price_eur_mwh'].rolling(168).mean()  # Weekly
    
    # Target
    features['target'] = df['price_eur_mwh']
    
    # Drop rows with NaN from lag features
    features = features.dropna()
    
    return features


def train_model(df, test_size=0.2):
    """Train XGBoost model with time series cross-validation."""
    
    # Create features
    features = create_features(df)
    print(f"Created {len(features.columns)-1} features, {len(features)} samples")
    
    # Split features and target
    X = features.drop('target', axis=1)
    y = features['target']
    
    # Time-based split (don't shuffle for time series!)
    split_idx = int(len(X) * (1 - test_size))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    print(f"Training: {len(X_train)} samples ({X_train.index.min().date()} to {X_train.index.max().date()})")
    print(f"Testing: {len(X_test)} samples ({X_test.index.min().date()} to {X_test.index.max().date()})")
    
    # XGBoost parameters tuned for price forecasting
    params = {
        'objective': 'reg:squarederror',
        'max_depth': 8,
        'learning_rate': 0.05,
        'n_estimators': 500,
        'min_child_weight': 5,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'reg_alpha': 0.1,
        'reg_lambda': 1.0,
        'random_state': 42,
        'n_jobs': -1,
    }
    
    # Train model
    model = xgb.XGBRegressor(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50
    )
    
    # Evaluate
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    metrics = {
        'train': {
            'mae': float(mean_absolute_error(y_train, y_pred_train)),
            'rmse': float(np.sqrt(mean_squared_error(y_train, y_pred_train))),
            'r2': float(r2_score(y_train, y_pred_train)),
        },
        'test': {
            'mae': float(mean_absolute_error(y_test, y_pred_test)),
            'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred_test))),
            'r2': float(r2_score(y_test, y_pred_test)),
        }
    }
    
    print(f"\n=== Model Performance ===")
    print(f"Training - MAE: €{metrics['train']['mae']:.2f}, RMSE: €{metrics['train']['rmse']:.2f}, R²: {metrics['train']['r2']:.3f}")
    print(f"Testing  - MAE: €{metrics['test']['mae']:.2f}, RMSE: €{metrics['test']['rmse']:.2f}, R²: {metrics['test']['r2']:.3f}")
    
    # Feature importance
    importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n=== Top 15 Most Important Features ===")
    for _, row in importance.head(15).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")
    
    return model, X.columns.tolist(), metrics, importance


def save_model(model, feature_names, metrics, importance):
    """Save model and metadata."""
    # Save model
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    
    # Save metadata
    metadata = {
        'trained_at': datetime.now(timezone.utc).isoformat(),
        'feature_names': feature_names,
        'metrics': metrics,
        'top_features': importance.head(20).to_dict('records'),
        'model_params': model.get_params(),
    }
    with open(METADATA_PATH, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\nModel saved to {MODEL_PATH}")
    print(f"Metadata saved to {METADATA_PATH}")


def load_model():
    """Load trained model and metadata."""
    if not os.path.exists(MODEL_PATH):
        return None, None
    
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    
    with open(METADATA_PATH, 'r') as f:
        metadata = json.load(f)
    
    return model, metadata


def forecast_prices(hours_ahead=48):
    """Generate price forecast for the next N hours."""
    model, metadata = load_model()
    if model is None:
        return {'error': 'Model not trained yet'}
    
    # Load recent data for lag features
    conn = sqlite3.connect(DB_PATH)
    
    # Need at least 168 hours (1 week) of recent data for lag features
    recent_prices = pd.read_sql_query("""
        SELECT timestamp, price_eur_mwh FROM prices 
        ORDER BY timestamp DESC LIMIT 200
    """, conn)
    recent_prices['timestamp'] = pd.to_datetime(recent_prices['timestamp'], utc=True)
    recent_prices.set_index('timestamp', inplace=True)
    recent_prices = recent_prices.sort_index()
    
    recent_load = pd.read_sql_query("""
        SELECT timestamp, load_mw FROM load 
        ORDER BY timestamp DESC LIMIT 200
    """, conn)
    recent_load['timestamp'] = pd.to_datetime(recent_load['timestamp'], utc=True)
    recent_load.set_index('timestamp', inplace=True)
    recent_load = recent_load.sort_index()
    
    conn.close()
    
    # Generate forecasts
    now = pd.Timestamp.now(tz='UTC').floor('h')
    forecasts = []
    
    # Create a rolling forecast
    price_history = recent_prices['price_eur_mwh'].to_dict()
    load_history = recent_load['load_mw'].to_dict()
    
    for h in range(hours_ahead):
        forecast_time = now + timedelta(hours=h)
        
        # Build feature vector
        features = {}
        features['hour'] = forecast_time.hour
        features['dayofweek'] = forecast_time.dayofweek
        features['month'] = forecast_time.month
        features['dayofyear'] = forecast_time.dayofyear
        features['weekofyear'] = forecast_time.isocalendar()[1]
        features['is_weekend'] = int(forecast_time.dayofweek >= 5)
        features['is_holiday'] = int(is_holiday(forecast_time))
        features['is_holiday_period'] = int(is_holiday_period(forecast_time))
        
        features['hour_sin'] = np.sin(2 * np.pi * features['hour'] / 24)
        features['hour_cos'] = np.cos(2 * np.pi * features['hour'] / 24)
        features['month_sin'] = np.sin(2 * np.pi * features['month'] / 12)
        features['month_cos'] = np.cos(2 * np.pi * features['month'] / 12)
        features['dow_sin'] = np.sin(2 * np.pi * features['dayofweek'] / 7)
        features['dow_cos'] = np.cos(2 * np.pi * features['dayofweek'] / 7)
        
        # Load features (use last known if not available)
        last_load = list(load_history.values())[-1] if load_history else 8000
        features['load_mw'] = last_load
        features['load_mw_lag1'] = last_load
        features['load_mw_lag24'] = last_load
        features['load_mw_rolling_24h'] = last_load
        
        # Generation features (use averages or zeros)
        for col in ['Solar', 'Wind_Onshore', 'Hydro_Run_of_river_and_poundage', 
                    'Hydro_Water_Reservoir', 'Fossil_Gas', 'Hydro_Pumped_Storage']:
            features[f'gen_{col}'] = 0  # Will be filled by patterns
        features['total_renewables'] = 0
        
        # Price lag features
        def get_lag_price(hours_back):
            lag_time = forecast_time - timedelta(hours=hours_back)
            return price_history.get(lag_time, list(price_history.values())[-1] if price_history else 100)
        
        features['price_lag_1h'] = get_lag_price(1)
        features['price_lag_2h'] = get_lag_price(2)
        features['price_lag_3h'] = get_lag_price(3)
        features['price_lag_24h'] = get_lag_price(24)
        features['price_lag_48h'] = get_lag_price(48)
        features['price_lag_168h'] = get_lag_price(168)
        
        # Rolling stats
        recent_vals = list(price_history.values())[-24:] if price_history else [100]
        features['price_rolling_24h_mean'] = np.mean(recent_vals)
        features['price_rolling_24h_std'] = np.std(recent_vals) if len(recent_vals) > 1 else 10
        features['price_rolling_168h_mean'] = np.mean(list(price_history.values())[-168:]) if price_history else 100
        
        # Create feature vector in correct order
        X = pd.DataFrame([features])[metadata['feature_names']]
        
        # Predict
        pred_price = float(model.predict(X)[0])
        
        # Store prediction for next iteration's lag features
        price_history[forecast_time] = pred_price
        
        forecasts.append({
            'timestamp': forecast_time.isoformat(),
            'hour': forecast_time.hour,
            'day': forecast_time.strftime('%a'),
            'date': forecast_time.strftime('%Y-%m-%d'),
            'is_weekend': bool(forecast_time.dayofweek >= 5),
            'is_holiday': bool(is_holiday(forecast_time)),
            'predicted_price': round(pred_price, 2),
        })
    
    return {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'model': 'xgboost_v1',
        'model_metrics': metadata['metrics']['test'],
        'forecasts': forecasts,
    }


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'train':
        print("Loading training data...")
        df = load_training_data()
        
        if len(df) < 1000:
            print(f"WARNING: Only {len(df)} samples. Need more data for reliable model.")
            print("Run backfill_historical.py to get more data.")
        
        print("\nTraining XGBoost model...")
        model, feature_names, metrics, importance = train_model(df)
        
        save_model(model, feature_names, metrics, importance)
        
    elif len(sys.argv) > 1 and sys.argv[1] == 'forecast':
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 48
        result = forecast_prices(hours)
        
        if 'error' in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Model: {result['model']}")
            print(f"Test MAE: €{result['model_metrics']['mae']:.2f}")
            print(f"\n48-hour forecast:")
            for f in result['forecasts'][:48]:
                marker = "🎄" if f['is_holiday'] else ("📅" if f['is_weekend'] else "  ")
                print(f"{marker} {f['date']} {f['hour']:02d}:00 {f['day']}: €{f['predicted_price']:.2f}/MWh")
    
    else:
        print("Usage:")
        print("  python price_forecast_model.py train    - Train model on historical data")
        print("  python price_forecast_model.py forecast [hours] - Generate forecast")
