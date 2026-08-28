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
CACHE_TTL = 600  # live page: 10 min (ENTSO-E fetcher cron runs every 5 min)


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


@analytics_bp.route('/api/analytics/hydro-drought')
def hydro_drought():
    """Drought monitor: hydro output vs. previous years, reservoir storage, market effects.

    Live-ish: 10 min cache. Compares the trailing 7/30-day window against the
    same calendar window in every earlier year in the DB.
    """
    ROR = 'Hydro Run-of-river and poundage'
    RES = 'Hydro Water Reservoir'
    PS = 'Hydro Pumped Storage'

    def compute():
        latest = q("SELECT MAX(timestamp) t FROM generation")[0]['t']
        today = latest[:10]
        year = int(today[:4])
        md = today[5:10]  # MM-DD

        # ---- monthly averages per year, for ROR / reservoir / pumped storage
        monthly = q("""
            SELECT substr(timestamp,1,4) year, substr(timestamp,6,2) mon, psr_type,
                   ROUND(AVG(value_mw)) mw
            FROM generation
            WHERE psr_type IN (?,?,?)
            GROUP BY year, mon, psr_type ORDER BY year, mon
        """, (ROR, RES, PS))

        # ---- trailing-window comparison vs. same window in earlier years
        def window_avg(psr, y, days):
            end = f'{y}-{md}'
            start = q("SELECT date(?, ?) d", (end, f'-{days} day'))[0]['d']
            r = q("""SELECT ROUND(AVG(value_mw),1) mw FROM generation
                     WHERE psr_type=? AND timestamp>=? AND timestamp<?""",
                  (psr, start, end + 'T23:59'))
            return r[0]['mw']

        years = sorted({r['year'] for r in monthly})
        comparison = []
        for psr, label in ((ROR, 'run_of_river'), (RES, 'reservoir'), (PS, 'pumped_storage')):
            row = {'psr_type': psr, 'key': label}
            for y in years:
                row[y] = {'d7': window_avg(psr, int(y), 7), 'd30': window_avg(psr, int(y), 30)}
            prev = [row[y]['d30'] for y in years if y != str(year) and row[y]['d30']]
            row['prev_years_avg_d30'] = round(sum(prev) / len(prev), 1) if prev else None
            cur = row[str(year)]['d30']
            row['deviation_pct'] = (round((cur / row['prev_years_avg_d30'] - 1) * 100, 1)
                                    if cur and row['prev_years_avg_d30'] else None)
            comparison.append(row)

        # ---- state assessment, so the page's wording adapts to the situation
        # instead of asserting a drought forever.
        ror_row = next(c for c in comparison if c['key'] == 'run_of_river')
        dev = ror_row['deviation_pct']
        cur_d30 = ror_row[str(year)]['d30']
        prev_vals = [(row_y, ror_row[row_y]['d30']) for row_y in years
                     if row_y != str(year) and ror_row[row_y]['d30']]
        below_all = bool(prev_vals) and cur_d30 is not None and all(
            cur_d30 < v for _, v in prev_vals)
        above_all = bool(prev_vals) and cur_d30 is not None and all(
            cur_d30 > v for _, v in prev_vals)
        rank = (sorted([v for _, v in prev_vals] + [cur_d30], reverse=True).index(cur_d30) + 1
                if cur_d30 is not None else None)
        if dev is None:
            level = 'unknown'
        elif dev <= -30:
            level = 'severe'
        elif dev <= -15:
            level = 'stressed'
        elif dev < 15:
            level = 'normal'
        else:
            level = 'wet'
        assessment = {
            'level': level,
            'deviation_pct': dev,
            'lowest_of_record': below_all,
            'highest_of_record': above_all,
            'rank_of_years': rank,
            'years_compared': len(prev_vals) + 1,
            'first_year': years[0] if years else None,
        }

        # ---- daily series, current year (for the live chart)
        daily = q("""
            WITH g AS (
              SELECT substr(timestamp,1,10) d, psr_type, AVG(value_mw) mw
              FROM generation WHERE psr_type IN (?,?) AND timestamp>=?
              GROUP BY d, psr_type
            ),
            p AS (SELECT substr(timestamp,1,10) d, AVG(price_eur_mwh) price
                  FROM prices WHERE timestamp>=? GROUP BY d),
            f AS (SELECT substr(timestamp,1,10) d,
                         AVG(net) net FROM (
                     SELECT timestamp, SUM(import_mw)-SUM(export_mw) net
                     FROM cross_border_flows WHERE timestamp>=? GROUP BY timestamp)
                  GROUP BY d)
            SELECT g.d day,
                   ROUND(MAX(CASE WHEN psr_type=? THEN mw END)) ror_mw,
                   ROUND(MAX(CASE WHEN psr_type=? THEN mw END)) reservoir_mw,
                   ROUND(MAX(p.price),1) price_eur_mwh,
                   ROUND(MAX(f.net)) net_import_mw
            FROM g LEFT JOIN p ON p.d=g.d LEFT JOIN f ON f.d=g.d
            GROUP BY g.d ORDER BY g.d
        """, (ROR, RES, f'{year}-01-01', f'{year}-01-01', f'{year}-01-01', ROR, RES))

        # ---- reservoir stored energy (A72), weekly, day-of-year aligned per year
        storage = q("""
            SELECT substr(timestamp,1,4) year,
                   CAST(strftime('%j', substr(timestamp,1,10)) AS INTEGER) doy,
                   substr(timestamp,1,10) day,
                   ROUND(stored_energy_mwh/1000.0,1) stored_gwh
            FROM reservoir_levels WHERE timestamp >= '2018-01-01' ORDER BY timestamp
        """)
        cur_store = [s for s in storage if s['year'] == str(year)]
        latest_store = cur_store[-1] if cur_store else None
        peers, peer_detail = [], []
        if latest_store:
            for y in sorted({s['year'] for s in storage} - {str(year)}):
                same = [s for s in storage if s['year'] == y
                        and abs(s['doy'] - latest_store['doy']) <= 4]
                if same:
                    peers.append(same[0]['stored_gwh'])
                    peer_detail.append({'year': y, 'stored_gwh': same[0]['stored_gwh'],
                                        'day': same[0]['day']})
        storage_norm = round(sum(peers) / len(peers), 1) if peers else None

        # ---- market effects: monthly net import share of load, monthly price
        market = q("""
            WITH f AS (SELECT timestamp, SUM(import_mw)-SUM(export_mw) net
                       FROM cross_border_flows GROUP BY timestamp),
            m AS (SELECT substr(f.timestamp,1,7) month, AVG(f.net) net, AVG(l.load_mw) load_mw
                  FROM f JOIN load l ON l.timestamp=f.timestamp GROUP BY month)
            SELECT month, ROUND(net) net_import_mw, ROUND(load_mw) load_mw,
                   ROUND(net/load_mw*100,1) net_import_share_pct FROM m ORDER BY month
        """)
        price_monthly = q(f"""
            WITH hp AS ({HOURLY_PRICES})
            SELECT month, ROUND(AVG(price),1) avg_price FROM hp GROUP BY month ORDER BY month
        """)

        # ---- which plants are picking up the slack (A73 per-plant data)
        # Rolling 60-day window ending at the latest available A73 day (~5 day
        # lag), compared with the same calendar window one year earlier. No
        # hardcoded months, so this stays meaningful in every season.
        p_end = q("SELECT substr(MAX(timestamp),1,10) d FROM plant_generation")[0]['d']
        plants, p_start, p_prev_start, p_prev_end = [], None, None, None
        if p_end:
            p_start = q("SELECT date(?, '-60 day') d", (p_end,))[0]['d']
            p_prev_start = q("SELECT date(?, '-1 year') d", (p_start,))[0]['d']
            p_prev_end = q("SELECT date(?, '-1 year') d", (p_end,))[0]['d']
            plants = q("""
                SELECT plant, psr_type,
                       ROUND(AVG(CASE WHEN timestamp>=? AND timestamp<? THEN value_mw END),1) cur_mw,
                       ROUND(AVG(CASE WHEN timestamp>=? AND timestamp<? THEN value_mw END),1) prev_mw
                FROM plant_generation
                WHERE (timestamp>=? AND timestamp<?) OR (timestamp>=? AND timestamp<?)
                GROUP BY plant, psr_type HAVING cur_mw > 1 OR prev_mw > 1
                ORDER BY cur_mw DESC
            """, (p_start, p_end + 'T23:59', p_prev_start, p_prev_end + 'T23:59',
                  p_start, p_end + 'T23:59', p_prev_start, p_prev_end + 'T23:59'))
        for p in plants:
            p['change_pct'] = (round((p['cur_mw'] / p['prev_mw'] - 1) * 100, 1)
                               if p['cur_mw'] is not None and p['prev_mw'] else None)

        return {
            'as_of': latest,
            'reference_day': today,
            'monthly': monthly,
            'comparison': comparison,
            'daily': daily,
            'storage': storage,
            'storage_latest': latest_store,
            'storage_normal_gwh': storage_norm,
            'storage_peers': peer_detail,
            'storage_deviation_pct': (round((latest_store['stored_gwh'] / storage_norm - 1) * 100, 1)
                                      if latest_store and storage_norm else None),
            'market_monthly': market,
            'assessment': assessment,
            'price_monthly': price_monthly,
            'plants': plants,
            'plants_jun_jul': plants,  # deprecated alias
            'plants_window': {'start': p_start, 'end': p_end,
                              'prev_start': p_prev_start, 'prev_end': p_prev_end},
            'note': ('Run-of-river = Danube/Inn/Drau chain (~5.6 GW). Reservoir storage from '
                     'ENTSO-E A72 (weekly, published with ~2-3 week lag).'),
        }

    now = time.time()
    key = 'hydrodrought'
    if key in _cache and now - _cache[key][0] < 600:
        return jsonify(_cache[key][1])
    data = compute()
    _cache[key] = (now, data)
    return jsonify(data)


@analytics_bp.route('/api/analytics/pulse')
def pulse():
    """Tiny live snapshot for the map page's teaser pill (fast, 5 min cache).

    Everything is derived from the latest data in the DB; no hardcoded years or
    seasons, so the teaser stays correct in every situation.
    """
    ROR = 'Hydro Run-of-river and poundage'
    RENEW = ('Solar', 'Wind Onshore', 'Biomass', ROR,
             'Hydro Water Reservoir', 'Other renewable', 'Geothermal')

    def compute():
        latest = q("SELECT MAX(timestamp) t FROM generation")[0]['t']
        if not latest:
            return {'ok': False}
        day, year = latest[:10], int(latest[:4])
        md = latest[5:10]

        price = q("""SELECT ROUND(price_eur_mwh,1) p, timestamp FROM prices
                     ORDER BY timestamp DESC LIMIT 1""")
        # renewable share of the most recent complete generation timestamp
        mix = q(f"""SELECT SUM(value_mw) total,
                          SUM(CASE WHEN psr_type IN ({','.join('?' * len(RENEW))})
                                   THEN value_mw ELSE 0 END) renew
                   FROM generation WHERE timestamp = ?""", (*RENEW, latest))
        share = None
        if mix and mix[0]['total']:
            share = round(mix[0]['renew'] / mix[0]['total'] * 100)

        def ror_window(y):
            end = f'{y}-{md}'
            start = q("SELECT date(?, '-30 day') d", (end,))[0]['d']
            return q("""SELECT ROUND(AVG(value_mw),1) mw FROM generation
                        WHERE psr_type=? AND timestamp>=? AND timestamp<?""",
                     (ROR, start, end + 'T23:59'))[0]['mw']

        first_year = int(q("SELECT MIN(substr(timestamp,1,4)) y FROM generation")[0]['y'])
        cur = ror_window(year)
        prev = [v for v in (ror_window(y) for y in range(first_year, year)) if v]
        norm = sum(prev) / len(prev) if prev else None
        dev = round((cur / norm - 1) * 100, 1) if cur and norm else None
        level = ('unknown' if dev is None else
                 'severe' if dev <= -30 else
                 'stressed' if dev <= -15 else
                 'wet' if dev >= 15 else 'normal')

        return {
            'ok': True,
            'as_of': latest,
            'day': day,
            'price_eur_mwh': price[0]['p'] if price else None,
            'price_as_of': price[0]['timestamp'] if price else None,
            'renewable_share_pct': share,
            'hydro_deviation_pct': dev,
            'hydro_level': level,
            'hydro_ror_mw': cur,
        }

    now = time.time()
    key = 'pulse'
    if key in _cache and now - _cache[key][0] < 300:
        return jsonify(_cache[key][1])
    data = compute()
    _cache[key] = (now, data)
    return jsonify(data)


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
