#!/usr/bin/env python3
"""Market analytics on 3.5 years of stored ENTSO-E data (2023 - today).

Provides pre-aggregated analytics endpoints for the analytics dashboard:
- Negative price hours (solar cannibalization)
- Duck curve / residual load evolution
- Capture rates for solar and wind (value factor)
- Battery arbitrage value (daily spreads)
- Import/export dependency
All computed from data/entsoe_data.db, cached for 1 hour.
"""

import sqlite3
import time
from flask import Blueprint, jsonify, request

DB_PATH = '/home/exedev/austria-grid/data/entsoe_data.db'

analytics_bp = Blueprint('analytics', __name__)

_cache = {}
CACHE_TTL = 3600


def cached(key, fn):
    now = time.time()
    if key in _cache and now - _cache[key][0] < CACHE_TTL:
        return _cache[key][1]
    data = fn()
    _cache[key] = (now, data)
    return data


def q(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


# Hourly price view: prices switched from 60-min (<=2025) to 15-min (2026 MTU),
# so we always aggregate to hourly for cross-year comparability.
HOURLY_PRICES = """
    SELECT substr(timestamp, 1, 13) AS hr,
           substr(timestamp, 1, 10) AS day,
           substr(timestamp, 1, 7)  AS month,
           substr(timestamp, 1, 4)  AS year,
           substr(timestamp, 12, 2) AS hod,
           AVG(price_eur_mwh) AS price
    FROM prices GROUP BY hr
"""

HOURLY_GEN = """
    SELECT substr(timestamp, 1, 13) AS hr, psr_type,
           AVG(value_mw) AS mw
    FROM generation GROUP BY hr, psr_type
"""


@analytics_bp.route('/api/analytics/negative-prices')
def negative_prices():
    def compute():
        monthly = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT month,
                   SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS neg_hours,
                   SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) AS nonpos_hours,
                   ROUND(MIN(price), 2) AS min_price,
                   ROUND(AVG(price), 2) AS avg_price,
                   COUNT(*) AS total_hours
            FROM hp GROUP BY month ORDER BY month
        """)
        yearly = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT year,
                   SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS neg_hours,
                   ROUND(AVG(CASE WHEN price < 0 THEN price END), 2) AS avg_neg_price,
                   ROUND(MIN(price), 2) AS min_price,
                   COUNT(*) AS total_hours
            FROM hp GROUP BY year ORDER BY year
        """)
        # When do negative prices occur? Hour-of-day histogram per year
        by_hour = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT year, hod, SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS neg_hours
            FROM hp GROUP BY year, hod ORDER BY year, hod
        """)
        return {'monthly': monthly, 'yearly': yearly, 'by_hour_of_day': by_hour,
                'note': 'Hourly averaged prices (2026 15-min MTU aggregated to hours)'}
    return jsonify(cached('negprices', compute))


@analytics_bp.route('/api/analytics/duck-curve')
def duck_curve():
    """Average hour-of-day profile of load, solar+wind, and residual load per year (summer months)."""
    months = request.args.get('months', '04,05,06,07,08,09')
    month_list = [m.strip() for m in months.split(',')]
    key = f'duck:{months}'

    def compute():
        ph = ','.join('?' * len(month_list))
        rows = q(f"""
            WITH hl AS (
                SELECT substr(timestamp,1,13) hr, substr(timestamp,1,4) year,
                       substr(timestamp,12,2) hod, substr(timestamp,6,2) mon,
                       AVG(load_mw) load_mw
                FROM load GROUP BY hr
            ),
            hgp AS (
                SELECT substr(timestamp,1,13) hr, psr_type, AVG(value_mw) mw
                FROM generation GROUP BY hr, psr_type
            ),
            hg AS (
                SELECT hr,
                       SUM(CASE WHEN psr_type='Solar' THEN mw ELSE 0 END) solar,
                       SUM(CASE WHEN psr_type='Wind Onshore' THEN mw ELSE 0 END) wind,
                       SUM(CASE WHEN psr_type='Hydro Run-of-river and poundage' THEN mw ELSE 0 END) ror
                FROM hgp GROUP BY hr
            )
            SELECT hl.year, hl.hod,
                   ROUND(AVG(hl.load_mw)) load_mw,
                   ROUND(AVG(hg.solar)) solar_mw,
                   ROUND(AVG(hg.wind)) wind_mw,
                   ROUND(AVG(hg.ror)) ror_mw,
                   ROUND(AVG(hl.load_mw - hg.solar - hg.wind)) residual_mw
            FROM hl JOIN hg ON hl.hr = hg.hr
            WHERE hl.mon IN ({ph})
            GROUP BY hl.year, hl.hod ORDER BY hl.year, hl.hod
        """, month_list)
        # avg hourly price profile too
        prices = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT year, hod, ROUND(AVG(price),2) price
            FROM hp WHERE substr(month,6,2) IN ({ph})
            GROUP BY year, hod ORDER BY year, hod
        """, month_list)
        return {'months': month_list, 'profile': rows, 'price_profile': prices}
    return jsonify(cached(key, compute))


@analytics_bp.route('/api/analytics/capture-rates')
def capture_rates():
    """Generation-weighted price vs baseload price (value factor) for solar & wind, monthly."""
    def compute():
        rows = q(f"""
            WITH hp AS ({HOURLY_PRICES}),
            hgp AS (
                SELECT substr(timestamp,1,13) hr, psr_type, AVG(value_mw) mw
                FROM generation GROUP BY hr, psr_type
            ),
            hg AS (
                SELECT hr,
                       SUM(CASE WHEN psr_type='Solar' THEN mw ELSE 0 END) solar,
                       SUM(CASE WHEN psr_type='Wind Onshore' THEN mw ELSE 0 END) wind
                FROM hgp GROUP BY hr
            )
            SELECT hp.month,
                   ROUND(AVG(hp.price),2) baseload_price,
                   ROUND(SUM(hg.solar*hp.price)/NULLIF(SUM(hg.solar),0),2) solar_capture_price,
                   ROUND(SUM(hg.wind*hp.price)/NULLIF(SUM(hg.wind),0),2) wind_capture_price,
                   ROUND(SUM(hg.solar*hp.price)/NULLIF(SUM(hg.solar),0)/NULLIF(AVG(hp.price),0)*100,1) solar_capture_rate_pct,
                   ROUND(SUM(hg.wind*hp.price)/NULLIF(SUM(hg.wind),0)/NULLIF(AVG(hp.price),0)*100,1) wind_capture_rate_pct,
                   ROUND(SUM(hg.solar)/1000.0,1) solar_gwh,
                   ROUND(SUM(hg.wind)/1000.0,1) wind_gwh
            FROM hp JOIN hg ON hp.hr = hg.hr
            GROUP BY hp.month ORDER BY hp.month
        """)
        return {'monthly': rows,
                'note': 'capture_rate = generation-weighted price / baseload avg price'}
    return jsonify(cached('capture', compute))


@analytics_bp.route('/api/analytics/battery-arbitrage')
def battery_arbitrage():
    """Daily top-N minus bottom-N hourly price spread => theoretical 1MW/nMWh battery revenue."""
    hours = min(max(int(request.args.get('hours', 2)), 1), 6)
    key = f'battery:{hours}'

    def compute():
        rows = q(f"""
            WITH hp AS ({HOURLY_PRICES}),
            ranked AS (
                SELECT day, month, year, price,
                       ROW_NUMBER() OVER (PARTITION BY day ORDER BY price DESC) rd,
                       ROW_NUMBER() OVER (PARTITION BY day ORDER BY price ASC) ra
                FROM hp
            ),
            daily AS (
                SELECT day, month, year,
                       AVG(CASE WHEN rd <= {hours} THEN price END) top_avg,
                       AVG(CASE WHEN ra <= {hours} THEN price END) bottom_avg
                FROM ranked GROUP BY day
            )
            SELECT month,
                   ROUND(AVG(top_avg - bottom_avg),2) avg_daily_spread,
                   ROUND(MAX(top_avg - bottom_avg),2) max_daily_spread,
                   ROUND(SUM(top_avg - bottom_avg) * {hours} * 0.9, 0) monthly_revenue_eur_per_mw,
                   COUNT(*) days
            FROM daily GROUP BY month ORDER BY month
        """)
        yearly = q(f"""
            WITH hp AS ({HOURLY_PRICES}),
            ranked AS (
                SELECT day, year, price,
                       ROW_NUMBER() OVER (PARTITION BY day ORDER BY price DESC) rd,
                       ROW_NUMBER() OVER (PARTITION BY day ORDER BY price ASC) ra
                FROM hp
            ),
            daily AS (
                SELECT day, year,
                       AVG(CASE WHEN rd <= {hours} THEN price END) top_avg,
                       AVG(CASE WHEN ra <= {hours} THEN price END) bottom_avg
                FROM ranked GROUP BY day
            )
            SELECT year, ROUND(AVG(top_avg - bottom_avg),2) avg_daily_spread,
                   ROUND(SUM(top_avg - bottom_avg) * {hours} * 0.9, 0) revenue_eur_per_mw,
                   COUNT(*) days
            FROM daily GROUP BY year ORDER BY year
        """)
        return {'battery_hours': hours, 'efficiency': 0.9,
                'monthly': rows, 'yearly': yearly,
                'note': f'Perfect-foresight 1-cycle/day arbitrage, {hours}h battery, 90% round-trip efficiency'}
    return jsonify(cached(key, compute))


@analytics_bp.route('/api/analytics/import-dependency')
def import_dependency():
    """Monthly generation vs load balance + generation mix shares."""
    def compute():
        rows = q("""
            WITH hl AS (
                SELECT substr(timestamp,1,13) hr, substr(timestamp,1,7) month, AVG(load_mw) load_mw
                FROM load GROUP BY hr
            ),
            hgp AS (
                SELECT substr(timestamp,1,13) hr, psr_type, AVG(value_mw) mw
                FROM generation GROUP BY hr, psr_type
            ),
            hg AS (
                SELECT hr, SUM(mw) gen_mw,
                       SUM(CASE WHEN psr_type IN ('Solar','Wind Onshore','Biomass',
                            'Hydro Run-of-river and poundage','Hydro Water Reservoir',
                            'Other renewable','Geothermal') THEN mw ELSE 0 END) renewable_mw,
                       SUM(CASE WHEN psr_type IN ('Fossil Gas','Fossil Hard coal','Fossil Oil') THEN mw ELSE 0 END) fossil_mw
                FROM hgp
                GROUP BY hr HAVING COUNT(*) >= 10
            )
            SELECT hl.month,
                   ROUND(SUM(hl.load_mw)/1000.0,1) load_gwh,
                   ROUND(SUM(hg.gen_mw)/1000.0,1) gen_gwh,
                   ROUND(SUM(hg.renewable_mw)/1000.0,1) renewable_gwh,
                   ROUND(SUM(hg.fossil_mw)/1000.0,1) fossil_gwh,
                   ROUND((SUM(hg.gen_mw)-SUM(hl.load_mw))/1000.0,1) balance_gwh,
                   ROUND(SUM(hg.renewable_mw)/SUM(hl.load_mw)*100,1) renewable_share_of_load_pct,
                   ROUND(SUM(hg.fossil_mw)/NULLIF(SUM(hg.gen_mw),0)*100,1) fossil_share_pct,
                   SUM(CASE WHEN hg.gen_mw >= hl.load_mw THEN 1 ELSE 0 END) surplus_hours,
                   COUNT(*) hours
            FROM hl JOIN hg ON hl.hr = hg.hr
            GROUP BY hl.month ORDER BY hl.month
        """)
        borders = q("""
            SELECT substr(timestamp,1,7) month, country_code,
                   ROUND(SUM(import_mw)/4000.0,1) import_gwh,
                   ROUND(SUM(export_mw)/4000.0,1) export_gwh
            FROM cross_border_flows
            GROUP BY month, country_code ORDER BY month, country_code
        """)
        return {'monthly': rows, 'cross_border_monthly': borders,
                'note': 'Cross-border data available since 2026-01 (15-min values, /4 => MWh)'}
    return jsonify(cached('importdep', compute))


@analytics_bp.route('/api/analytics/summary')
def summary():
    """Headline KPIs per year for the dashboard."""
    def compute():
        years = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT year, ROUND(AVG(price),1) avg_price,
                   SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) neg_hours,
                   SUM(CASE WHEN price > 200 THEN 1 ELSE 0 END) high_hours,
                   COUNT(*) hours
            FROM hp GROUP BY year ORDER BY year
        """)
        solar = q("""
            SELECT substr(timestamp,1,4) year, ROUND(MAX(value_mw)) peak_solar_mw,
                   ROUND(SUM(value_mw)/4000.0/COUNT(DISTINCT substr(timestamp,1,10))*365,0) est_annual_gwh
            FROM generation WHERE psr_type='Solar' GROUP BY year ORDER BY year
        """)
        wind = q("""
            SELECT substr(timestamp,1,4) year, ROUND(MAX(value_mw)) peak_wind_mw
            FROM generation WHERE psr_type='Wind Onshore' GROUP BY year ORDER BY year
        """)
        rng = q("SELECT MIN(timestamp) min_ts, MAX(timestamp) max_ts, COUNT(*) n FROM generation")
        return {'years': years, 'solar': solar, 'wind': wind, 'data_range': rng[0]}
    return jsonify(cached('summary', compute))
