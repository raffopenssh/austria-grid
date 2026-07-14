#!/usr/bin/env python3
"""Future Grid Planner — reverse Standortanalyse.

Instead of scoring one user-supplied address, this job walks the whole of
Austria on an H3 hex grid (res 7, ~5 km² per cell) and answers per cell:

  demand  : annual electricity demand (national ENTSO-E load distributed by
            Kontur 400m population raster), plus a 2040 electrification
            scenario (ÖNIP: ~ +45%)
  supply  : annual generation of every plant type currently in use.
            National per-type generation (ENTSO-E, trailing 365d) is
            allocated to mapped plants proportional to capacity:
            hydro RoR / reservoir / pumped storage, wind, solar (utility +
            rooftop share by population), gas, biomass, waste, coal, other.
  gap     : demand - supply (today and 2040)
  advice  : which technology should be built here to close the gap, sized in
            MW, considering grid headroom (e-control transformer registry),
            state capacity factors, protected areas (INSPIRE), existing
            buildout, and rooftop potential.

State lives in data/future_grid.db so the job can run incrementally from
cron and "steadily build" the map.

CLI:
  python3 future_grid_planner.py seed          # (re)build hex grid + inputs
  python3 future_grid_planner.py batch [N]     # process N pending cells
  python3 future_grid_planner.py status
  python3 future_grid_planner.py run-all       # seed + loop batches
"""

import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import h3

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
DB_PATH = os.path.join(DATA_DIR, 'future_grid.db')
ENTSOE_DB = os.path.join(DATA_DIR, 'entsoe_data.db')
KONTUR_GPKG = os.path.join(DATA_DIR, 'kontur_population_AT_20231101.gpkg')

HEX_RES = 7            # ~5.1 km² per cell
DEMAND_2040_FACTOR = 1.45   # ÖNIP electrification scenario
AT_INSTALLED_SOLAR_MW = 8200.0  # E-Control 2024; OSM only maps ~580 MW utility

# capacity factors by state (same base data as opportunity_map.py)
WIND_CF = {
    'Burgenland': 0.28, 'Niederösterreich': 0.25, 'Wien': 0.20,
    'Steiermark': 0.22, 'Oberösterreich': 0.20, 'Kärnten': 0.18,
    'Salzburg': 0.15, 'Tirol': 0.15, 'Vorarlberg': 0.15,
}
SOLAR_CF = {
    'Burgenland': 0.12, 'Niederösterreich': 0.11, 'Wien': 0.11,
    'Steiermark': 0.11, 'Oberösterreich': 0.10, 'Kärnten': 0.12,
    'Salzburg': 0.10, 'Tirol': 0.11, 'Vorarlberg': 0.10,
}

# OSM plant source -> ENTSO-E psr type
OSM_TO_PSR = {
    'hydro_run_of_river': 'Hydro Run-of-river and poundage',
    'wind': 'Wind Onshore',
    'solar': 'Solar',
    'gas': 'Fossil Gas',
    'coal': 'Fossil Hard coal',
    'biomass': 'Biomass',
    'waste': 'Waste',
    'other': 'Other',
}
HYDRO_TYPE_TO_PSR = {
    'Laufkraftwerk': 'Hydro Run-of-river and poundage',
    'Speicherkraftwerk': 'Hydro Water Reservoir',
    'Pumpspeicherkraftwerk': 'Hydro Pumped Storage',
}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _load(name):
    with open(os.path.join(DATA_DIR, name)) as f:
        return json.load(f)


def _num(v):
    if v in (None, '', '0'):
        return 0.0
    try:
        return float(str(v).replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0


def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def init_db(conn):
    conn.executescript('''
    CREATE TABLE IF NOT EXISTS hex_cells (
        h3 TEXT PRIMARY KEY,
        lat REAL, lon REAL,
        state TEXT,
        pop REAL DEFAULT 0,
        demand_gwh REAL,
        demand_2040_gwh REAL,
        supply_gwh REAL,
        supply_by_type TEXT,
        gap_gwh REAL,
        gap_2040_gwh REAL,
        station_name TEXT,
        station_km REAL,
        headroom_mw REAL,
        protected INTEGER DEFAULT 0,
        recommendation TEXT,
        status TEXT DEFAULT 'pending',
        updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_hex_status ON hex_cells(status);
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
    ''')
    conn.commit()


def set_meta(conn, key, value):
    conn.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, str(value)))
    conn.commit()


def get_meta(conn, key, default=None):
    r = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return r[0] if r else default


# ---------------------------------------------------------------- state lookup

_STATE_BOXES = None


def _get_state(lat, lon):
    """Rough state from coords (same heuristic as substation_load_model)."""
    if lon > 16.18 and 48.1 < lat < 48.33 and lon < 16.58:
        return 'Wien'
    if lon < 10.3:
        return 'Vorarlberg'
    if lon < 12.3 and lat < 47.8:
        return 'Tirol'
    if 12.3 <= lon < 13.9 and lat < 48.0 and lat > 46.9:
        return 'Salzburg'
    if 12.6 <= lon < 15.1 and lat < 47.05:
        return 'Kärnten'
    if lon >= 16.35 and lat < 48.15:
        return 'Burgenland'
    if 13.6 <= lon < 16.4 and lat < 47.85:
        return 'Steiermark'
    if 12.7 <= lon < 15.0 and lat >= 47.75:
        return 'Oberösterreich'
    return 'Niederösterreich'


# ------------------------------------------------------------------- seeding

def seed():
    t0 = time.time()
    conn = db()
    init_db(conn)

    # 1. hex grid over Austria from district polygons
    bez = _load('bezirke.json')
    cells = set()
    for f in bez['features']:
        g = f['geometry']
        polys = g['coordinates'] if g['type'] == 'MultiPolygon' else [g['coordinates']]
        for poly in polys:
            cells.update(h3.geo_to_cells({'type': 'Polygon', 'coordinates': poly}, HEX_RES))
    print(f'hex grid: {len(cells)} cells (res {HEX_RES})')

    # 2. population per cell from Kontur res-8 raster -> res-7 parent
    pop = {}
    kdb = sqlite3.connect(KONTUR_GPKG)
    for cell8, p in kdb.execute('SELECT h3, population FROM population'):
        try:
            parent = h3.cell_to_parent(cell8, HEX_RES)
        except Exception:
            continue
        pop[parent] = pop.get(parent, 0.0) + (p or 0.0)
    kdb.close()
    total_pop = sum(pop.get(c, 0.0) for c in cells)
    print(f'population mapped: {total_pop:,.0f}')

    # 3. national demand (trailing 365d) from ENTSO-E
    edb = sqlite3.connect(ENTSOE_DB)
    avg_load = edb.execute(
        "SELECT AVG(load_mw) FROM load WHERE timestamp >= date('now','-365 days')"
    ).fetchone()[0] or 7000.0
    national_twh = avg_load * 8.76 / 1000.0  # MW -> TWh/yr
    edb.close()
    print(f'national demand: {national_twh:.1f} TWh/yr')

    # 4. insert cells
    rows = []
    for c in cells:
        lat, lon = h3.cell_to_latlng(c)
        p = pop.get(c, 0.0)
        demand = national_twh * 1000.0 * (p / total_pop) if total_pop else 0.0  # GWh/yr
        rows.append((c, round(lat, 5), round(lon, 5), _get_state(lat, lon),
                     round(p, 1), round(demand, 4), round(demand * DEMAND_2040_FACTOR, 4)))
    conn.execute('DELETE FROM hex_cells')
    conn.executemany(
        '''INSERT INTO hex_cells (h3, lat, lon, state, pop, demand_gwh, demand_2040_gwh)
           VALUES (?,?,?,?,?,?,?)''', rows)
    set_meta(conn, 'seeded_at', _now())
    set_meta(conn, 'national_demand_twh', round(national_twh, 2))
    set_meta(conn, 'total_pop', round(total_pop))
    set_meta(conn, 'completed_at', '')
    conn.commit()
    conn.close()
    print(f'seeded {len(rows)} cells in {time.time() - t0:.1f}s')


# ------------------------------------------------------- plant energy mapping

def build_plant_index():
    """Return {hex: {psr_type: gwh_per_year}} for all mapped plants, calibrated
    so per-type national totals match ENTSO-E trailing-365d generation."""
    edb = sqlite3.connect(ENTSOE_DB)
    gen = {}  # psr -> GWh/yr
    for psr, avg in edb.execute(
            "SELECT psr_type, AVG(value_mw) FROM generation "
            "WHERE timestamp >= date('now','-365 days') GROUP BY psr_type"):
        gen[psr] = (avg or 0.0) * 8.76  # GWh/yr
    edb.close()

    # collect plants: (lat, lon, psr, capacity_mw)
    plants = []
    pp = _load('all_power_plants.json')
    for f in pp['features']:
        props = f['properties']
        src = props.get('source')
        if src in ('wind',):
            continue  # use AustroControl turbine registry instead (more complete)
        psr = OSM_TO_PSR.get(src)
        if not psr:
            continue
        cap = _num(props.get('capacity_mw'))
        lon, lat = f['geometry']['coordinates'][:2]
        plants.append((lat, lon, psr, cap if cap > 0 else 0.05))

    hp = _load('hydropower_plants.json')
    for f in hp['features']:
        props = f['properties']
        psr = HYDRO_TYPE_TO_PSR.get(props.get('type'))
        if not psr or psr == 'Hydro Run-of-river and poundage':
            continue  # RoR already well covered by OSM; add storage/pumped only
        lon, lat = f['geometry']['coordinates'][:2]
        plants.append((lat, lon, psr, _num(props.get('mw')) or 10.0))

    wt = _load('wind_turbines_enhanced.json')
    for t in wt:
        plants.append((t['lat'], t['lon'], 'Wind Onshore', _num(t.get('estimated_mw')) or 3.0))

    # allocate national generation per type proportional to capacity share
    cap_by_psr = {}
    for lat, lon, psr, cap in plants:
        cap_by_psr[psr] = cap_by_psr.get(psr, 0.0) + cap

    hex_supply = {}  # hex -> {psr: gwh}

    def add(cell, psr, gwh):
        if gwh <= 0:
            return
        d = hex_supply.setdefault(cell, {})
        d[psr] = d.get(psr, 0.0) + gwh

    # Solar special case: OSM only maps utility-scale (~580 MW of ~8.2 GW).
    # Utility share of national solar gen goes to mapped plants; the rooftop
    # remainder is distributed later by population (in process_batch).
    solar_total_gwh = gen.get('Solar', 0.0)
    utility_share = min(cap_by_psr.get('Solar', 0.0) / AT_INSTALLED_SOLAR_MW, 1.0)
    rooftop_solar_gwh = solar_total_gwh * (1.0 - utility_share)

    for lat, lon, psr, cap in plants:
        total_cap = cap_by_psr.get(psr, 0.0)
        if total_cap <= 0:
            continue
        type_gwh = gen.get(psr, 0.0)
        if psr == 'Solar':
            type_gwh = solar_total_gwh * utility_share
        gwh = type_gwh * (cap / total_cap)
        cell = h3.latlng_to_cell(lat, lon, HEX_RES)
        add(cell, psr, gwh)

    return hex_supply, rooftop_solar_gwh, gen


def build_station_index():
    """Transformer stations with headroom, bucketed on a coarse lat/lon grid
    for fast nearest lookup."""
    stations = _load('transformer_stations.json')
    idx = {}
    for s in stations:
        lat, lon = s.get('latitude'), s.get('longitude')
        if not lat or not lon:
            continue
        key = (int(lat * 4), int(lon * 4))  # ~25 km buckets
        idx.setdefault(key, []).append(
            (lat, lon, s.get('substationName', '?'), _num(s.get('availableCapacity'))))
    return idx


def nearest_station(idx, lat, lon):
    best = (None, 1e9, 0.0)
    k0, k1 = int(lat * 4), int(lon * 4)
    for ring in range(1, 6):
        found = False
        for dk0 in range(-ring, ring + 1):
            for dk1 in range(-ring, ring + 1):
                for slat, slon, name, avail in idx.get((k0 + dk0, k1 + dk1), []):
                    d = _haversine(lat, lon, slat, slon)
                    if d < best[1]:
                        best = (name, d, avail)
                        found = True
        if found and ring >= 2:
            break
    return best


def build_protected_index():
    try:
        from shapely.geometry import shape
        from shapely.strtree import STRtree
        pa = _load('inspire/protected_areas.geojson')
        geoms = []
        for f in pa['features']:
            try:
                geoms.append(shape(f['geometry']))
            except Exception:
                continue
        return STRtree(geoms), geoms
    except Exception as e:
        print(f'protected areas unavailable: {e}')
        return None, []


# ---------------------------------------------------------- recommendation

def recommend(cell_row, supply_by_type, gap_2040_gwh, headroom_mw, protected, state, pop):
    """Pick technologies to close the 2040 gap for this cell."""
    recs = []
    if gap_2040_gwh <= 0.05:
        surplus = -gap_2040_gwh
        if surplus > 20 and (supply_by_type.get('Solar', 0) + supply_by_type.get('Wind Onshore', 0)) > 0.5 * surplus:
            recs.append({'tech': 'battery', 'mw': round(min(surplus / 8.76 * 0.25, 50), 1),
                         'why': 'Erzeugungsüberschuss aus Erneuerbaren – Speicher stabilisiert Netz und erhöht Erlöse'})
        else:
            recs.append({'tech': 'none', 'mw': 0,
                         'why': 'Zelle ist bilanziell versorgt'})
        return recs

    wind_cf = WIND_CF.get(state, 0.20)
    solar_cf = SOLAR_CF.get(state, 0.11)
    has_wind_nearby = supply_by_type.get('Wind Onshore', 0) > 0
    has_hydro = any(k.startswith('Hydro') for k in supply_by_type)
    urban = pop > 8000  # dense cell

    remaining = gap_2040_gwh

    # 1. rooftop solar first where people live (no land use, no permits)
    if pop > 50:
        # assume 1.5 kWp realistic rooftop potential per person not yet used
        roof_mw = pop * 0.0015
        roof_gwh = roof_mw * solar_cf * 8.76
        take = min(roof_gwh, remaining * (0.8 if urban else 0.4))
        if take > 0.05:
            recs.append({'tech': 'solar_rooftop', 'mw': round(take / (solar_cf * 8.76), 1),
                         'why': f'Dachflächen-PV-Potenzial ({pop:,.0f} Einwohner), CF {solar_cf:.0%}'})
            remaining -= take

    # 2. wind where CF is good and not protected/urban
    if remaining > 0.1 and not urban and not protected and wind_cf >= 0.20:
        take = remaining * (0.7 if has_wind_nearby else 0.5)
        mw = take / (wind_cf * 8.76)
        mw = min(mw, 30)  # max ~6-8 turbines per 5 km² cell
        take = mw * wind_cf * 8.76
        why = f'Wind-CF {wind_cf:.0%} in {state}'
        if has_wind_nearby:
            why += ', bestehende Windkraft belegt Ressource'
        recs.append({'tech': 'wind', 'mw': round(mw, 1), 'why': why})
        remaining -= take

    # 3. ground-mount / agri PV for the rest (rural)
    if remaining > 0.1 and not urban:
        mw = min(remaining / (solar_cf * 8.76), 40)
        if protected:
            mw = min(mw, 5)
        recs.append({'tech': 'solar_ground', 'mw': round(mw, 1),
                     'why': 'Freiflächen-/Agri-PV' + (' (Schutzgebiet – nur kleinteilig)' if protected else '')})
        remaining -= mw * solar_cf * 8.76

    # 4. hydro repowering where hydro exists
    if remaining > 0.1 and has_hydro:
        mw = min(remaining / (0.5 * 8.76), 10)
        recs.append({'tech': 'hydro_repowering', 'mw': round(mw, 1),
                     'why': 'Bestehende Wasserkraft – Repowering/Effizienzsteigerung'})
        remaining -= mw * 0.5 * 8.76

    # 5. remainder must come via the grid
    if remaining > 0.1:
        need_mw = remaining / 8.76 * 2.5  # peak factor
        if headroom_mw >= need_mw:
            recs.append({'tech': 'grid_import', 'mw': round(need_mw, 1),
                         'why': f'Netzbezug – Umspannwerk hat {headroom_mw:.0f} MW frei'})
        else:
            recs.append({'tech': 'grid_upgrade', 'mw': round(need_mw, 1),
                         'why': f'Netzausbau nötig – Bedarf ~{need_mw:.0f} MW übersteigt freie Kapazität ({headroom_mw:.0f} MW) am nächsten Umspannwerk'})
    return recs


# ------------------------------------------------------------------ batching

_CACHE = {}


def process_batch(n=1500):
    conn = db()
    init_db(conn)
    pending = conn.execute(
        "SELECT h3, lat, lon, state, pop, demand_gwh, demand_2040_gwh "
        "FROM hex_cells WHERE status='pending' LIMIT ?", (n,)).fetchall()
    if not pending:
        if not get_meta(conn, 'completed_at'):
            set_meta(conn, 'completed_at', _now())
            print('all cells done; marked complete')
        else:
            print('nothing to do')
        conn.close()
        return 0

    t0 = time.time()
    if 'plants' not in _CACHE:
        _CACHE['plants'], _CACHE['rooftop_gwh'], _CACHE['gen'] = build_plant_index()
        _CACHE['stations'] = build_station_index()
        _CACHE['ptree'], _CACHE['pgeoms'] = build_protected_index()
        total_pop = float(get_meta(conn, 'total_pop', 9000000))
        _CACHE['rooftop_per_person'] = _CACHE['rooftop_gwh'] / total_pop if total_pop else 0
        print(f'indexes built in {time.time() - t0:.1f}s '
              f'(rooftop solar pool: {_CACHE["rooftop_gwh"]:.0f} GWh/yr)')

    from shapely.geometry import Point
    hex_supply = _CACHE['plants']
    updates = []
    for cell, lat, lon, state, pop, demand, demand40 in pending:
        supply_by_type = dict(hex_supply.get(cell, {}))
        # rooftop solar share by population
        roof = _CACHE['rooftop_per_person'] * (pop or 0)
        if roof > 0.001:
            supply_by_type['Solar'] = supply_by_type.get('Solar', 0.0) + roof
        supply = sum(supply_by_type.values())

        name, dist, avail = nearest_station(_CACHE['stations'], lat, lon)

        protected = 0
        if _CACHE['ptree'] is not None:
            try:
                hits = _CACHE['ptree'].query(Point(lon, lat))
                for i in hits:
                    if _CACHE['pgeoms'][int(i)].contains(Point(lon, lat)):
                        protected = 1
                        break
            except Exception:
                pass

        gap = demand - supply
        gap40 = demand40 - supply
        recs = recommend(cell, supply_by_type, gap40, avail, protected, state, pop or 0)

        updates.append((
            round(supply, 4),
            json.dumps({k: round(v, 3) for k, v in supply_by_type.items()}, ensure_ascii=False),
            round(gap, 4), round(gap40, 4),
            name, round(dist, 2) if dist < 1e8 else None, avail,
            protected, json.dumps(recs, ensure_ascii=False),
            'done', _now(), cell))

    conn.executemany(
        '''UPDATE hex_cells SET supply_gwh=?, supply_by_type=?, gap_gwh=?, gap_2040_gwh=?,
           station_name=?, station_km=?, headroom_mw=?, protected=?, recommendation=?,
           status=?, updated_at=? WHERE h3=?''', updates)
    conn.commit()
    left = conn.execute("SELECT COUNT(*) FROM hex_cells WHERE status='pending'").fetchone()[0]
    if left == 0:
        set_meta(conn, 'completed_at', _now())
    conn.close()
    print(f'processed {len(updates)} cells in {time.time() - t0:.1f}s, {left} pending')
    return len(updates)


def status():
    conn = db()
    init_db(conn)
    total = conn.execute('SELECT COUNT(*) FROM hex_cells').fetchone()[0]
    done = conn.execute("SELECT COUNT(*) FROM hex_cells WHERE status='done'").fetchone()[0]
    print(f'{done}/{total} cells done')
    for k in ('seeded_at', 'completed_at', 'national_demand_twh', 'total_pop'):
        print(f'  {k}: {get_meta(conn, k)}')
    if done:
        r = conn.execute(
            "SELECT SUM(demand_gwh), SUM(supply_gwh), SUM(CASE WHEN gap_2040_gwh>0 THEN gap_2040_gwh ELSE 0 END) "
            "FROM hex_cells WHERE status='done'").fetchone()
        print(f'  demand {r[0]/1000:.1f} TWh | supply {r[1]/1000:.1f} TWh | 2040 deficit {r[2]/1000:.1f} TWh')
    conn.close()


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'batch'
    if cmd == 'seed':
        seed()
    elif cmd == 'batch':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
        process_batch(n)
    elif cmd == 'status':
        status()
    elif cmd == 'run-all':
        seed()
        while process_batch(2000):
            pass
        status()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
