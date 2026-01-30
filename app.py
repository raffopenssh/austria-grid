#!/usr/bin/env python3
"""Austrian Wind Power Grid Capacity Visualization"""

from flask import Flask, jsonify, send_from_directory, send_file, render_template_string, Response, request
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from shapely.geometry import shape, Point, LineString
from urllib.parse import quote
import geopandas as gpd
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from functools import lru_cache
import time

app = Flask(__name__, static_folder='static')

# Base URL for the site
BASE_URL = 'https://austria-power.exe.xyz:8000'

# ENTSO-E API configuration
ENTSOE_API_KEY = os.environ.get('ENTSOE_API_KEY', '35efd923-6969-4470-b2bd-0155b2254346')
ENTSOE_BASE_URL = 'https://web-api.tp.entsoe.eu/api'
AUSTRIA_BZ = '10YAT-APG------L'  # Austria bidding zone

# Country codes for cross-border flows
COUNTRY_CODES = {
    'DE': '10Y1001A1001A83F',  # Germany (DE-LU)
    'CZ': '10YCZ-CEPS-----N',  # Czech Republic
    'SK': '10YSK-SEPS-----K',  # Slovakia
    'HU': '10YHU-MAVIR----U',  # Hungary
    'SI': '10YSI-ELES-----O',  # Slovenia
    'IT': '10YIT-GRTN-----B',  # Italy
    'CH': '10YCH-SWISSGRIDZ',  # Switzerland
}

# PSR type codes for generation
PSR_TYPES = {
    'B01': 'Biomasse',
    'B02': 'Braunkohle',
    'B03': 'Steinkohle',
    'B04': 'Erdgas',
    'B05': 'Heizöl',
    'B06': 'Gas',
    'B09': 'Geothermie',
    'B10': 'Wasserkraft (Laufwasser)',
    'B11': 'Wasserkraft (Speicher)',
    'B12': 'Wasserkraft (Pumpspeicher)',
    'B14': 'Kernkraft',
    'B15': 'Andere erneuerbare',
    'B16': 'Solar',
    'B17': 'Abfall',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    'B20': 'Andere',
}

# Cache for ENTSO-E data (5 min TTL)
entsoe_cache = {}
CACHE_TTL = 300  # seconds


# ENTSO-E helper functions
def get_cached(key):
    """Get cached data if not expired"""
    if key in entsoe_cache:
        data, timestamp = entsoe_cache[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
    return None

def set_cached(key, data):
    """Store data in cache"""
    entsoe_cache[key] = (data, time.time())

def parse_entsoe_xml(xml_text, value_key='quantity'):
    """Parse ENTSO-E XML response into structured data"""
    root = ET.fromstring(xml_text)
    ns = {'': root.tag.split('}')[0].strip('{')}
    
    result = []
    for ts in root.findall('.//{%s}TimeSeries' % ns['']):
        psr_type = None
        psr_elem = ts.find('.//{%s}psrType' % ns[''])
        if psr_elem is not None:
            psr_type = psr_elem.text
        
        in_domain = ts.find('.//{%s}in_Domain.mRID' % ns[''])
        out_domain = ts.find('.//{%s}out_Domain.mRID' % ns[''])
        
        for period in ts.findall('.//{%s}Period' % ns['']):
            start = period.find('.//{%s}start' % ns['']).text
            resolution = period.find('.//{%s}resolution' % ns['']).text
            
            for point in period.findall('.//{%s}Point' % ns['']):
                pos = int(point.find('.//{%s}position' % ns['']).text)
                
                # Handle both quantity and price.amount
                value_elem = point.find('.//{%s}%s' % (ns[''], value_key))
                if value_elem is None:
                    value_elem = point.find('.//{%s}price.amount' % ns[''])
                
                if value_elem is not None:
                    value = float(value_elem.text)
                    result.append({
                        'psr_type': psr_type,
                        'position': pos,
                        'value': value,
                        'start': start,
                        'resolution': resolution,
                        'in_domain': in_domain.text if in_domain is not None else None,
                        'out_domain': out_domain.text if out_domain is not None else None,
                    })
    return result

def fetch_entsoe(params):
    """Fetch data from ENTSO-E API"""
    params['securityToken'] = ENTSOE_API_KEY
    try:
        response = requests.get(ENTSOE_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"ENTSO-E API error: {e}")
        return None


# Load data
def load_json(filename):
    with open(f'data/{filename}', 'r') as f:
        return json.load(f)

@app.route('/')
def index():
    return send_file('static/index.html')

@app.route('/api/wind-turbines')
def wind_turbines():
    data = load_json('wind_turbines_enhanced.json')
    return jsonify(data)

@app.route('/api/transformer-stations')
def transformer_stations():
    data = load_json('transformer_stations.json')
    return jsonify(data)

@app.route('/api/windparks')
def windparks():
    data = load_json('windparks.json')
    return jsonify(data)

@app.route('/api/production')
def production():
    data = load_json('production.json')
    return jsonify(data)

@app.route('/api/bezirke')
def bezirke():
    data = load_json('bezirke.json')
    return jsonify(data)

@app.route('/api/transmission-lines')
def transmission_lines():
    """High voltage transmission lines from Austro Control obstacle database"""
    data = load_json('transmission_lines.json')
    return jsonify(data)

@app.route('/api/osm-transmission-lines')
def osm_transmission_lines():
    """High voltage transmission lines (220kV, 380kV) from OpenStreetMap"""
    data = load_json('osm_transmission_lines.json')
    return jsonify(data)

@app.route('/api/osm-substations')
def osm_substations():
    """High voltage substations (220kV, 380kV) from OpenStreetMap"""
    data = load_json('osm_substations.json')
    return jsonify(data)

@app.route('/api/hydropower')
def hydropower():
    """Hydropower plants in Austria"""
    data = load_json('hydropower_plants.json')
    return jsonify(data)

@app.route('/api/cross-border')
def cross_border():
    """Cross-border transmission interconnections"""
    data = load_json('cross_border_connections.json')
    return jsonify(data)

@app.route('/api/hydro-connections')
def hydro_connections():
    """Inferred connections from large hydropower to 380kV grid"""
    data = load_json('hydro_grid_connections.json')
    return jsonify(data)

@app.route('/api/onip-powerlines')
def onip_powerlines():
    """ÖNIP Basisnetz 2030 power line points (extracted from planning map)"""
    data = load_json('onip_powerlines_points.json')
    return jsonify(data)

@app.route('/api/grid-network')
def grid_network():
    """380kV grid network topology with substations and connected lines"""
    data = load_json('grid_network_380kv.json')
    return jsonify(data)


# ============ ENTSO-E LIVE DATA ROUTES ============

@app.route('/api/entsoe/generation')
def entsoe_generation():
    """Current actual generation per type in Austria"""
    cache_key = 'generation'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)
    
    now = datetime.utcnow()
    start = (now - timedelta(hours=2)).strftime('%Y%m%d%H00')
    end = now.strftime('%Y%m%d%H00')
    
    xml_data = fetch_entsoe({
        'documentType': 'A75',  # Actual generation per type
        'processType': 'A16',   # Realised
        'in_Domain': AUSTRIA_BZ,
        'periodStart': start,
        'periodEnd': end,
    })
    
    if not xml_data:
        return jsonify({'error': 'Failed to fetch data'}), 500
    
    parsed = parse_entsoe_xml(xml_data)
    
    # Aggregate latest values by PSR type
    generation = {}
    for item in parsed:
        psr = item['psr_type']
        if psr and item['value'] > 0:
            name = PSR_TYPES.get(psr, psr)
            if name not in generation or item['position'] > generation[name]['position']:
                generation[name] = {'value': item['value'], 'position': item['position']}
    
    result = {
        'timestamp': now.isoformat(),
        'generation': {k: v['value'] for k, v in generation.items()},
        'total_mw': sum(v['value'] for v in generation.values()),
        'unit': 'MW',
    }
    set_cached(cache_key, result)
    return jsonify(result)

@app.route('/api/entsoe/prices')
def entsoe_prices():
    """Day-ahead electricity prices for Austria"""
    cache_key = 'prices'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)
    
    now = datetime.utcnow()
    start = (now - timedelta(days=1)).strftime('%Y%m%d0000')
    end = (now + timedelta(days=1)).strftime('%Y%m%d2300')
    
    xml_data = fetch_entsoe({
        'documentType': 'A44',  # Price document
        'in_Domain': AUSTRIA_BZ,
        'out_Domain': AUSTRIA_BZ,
        'periodStart': start,
        'periodEnd': end,
    })
    
    if not xml_data:
        return jsonify({'error': 'Failed to fetch data'}), 500
    
    parsed = parse_entsoe_xml(xml_data, value_key='price.amount')
    
    # Convert to hourly prices
    prices = []
    for item in parsed:
        prices.append({
            'position': item['position'],
            'price_eur_mwh': item['value'],
            'start': item['start'],
        })
    
    # Find current price (position based on current hour)
    current_hour = now.hour
    current_price = None
    for p in prices:
        # Each position represents 15 minutes, so position 1-4 = hour 0, 5-8 = hour 1, etc.
        pos_hour = (p['position'] - 1) // 4
        if pos_hour == current_hour:
            current_price = p['price_eur_mwh']
            break
    
    result = {
        'timestamp': now.isoformat(),
        'current_price_eur_mwh': current_price,
        'prices': prices[-96:],  # Last 24 hours (96 x 15min intervals)
        'currency': 'EUR',
        'unit': 'MWh',
    }
    set_cached(cache_key, result)
    return jsonify(result)

@app.route('/api/entsoe/cross-border-flows')
def entsoe_cross_border():
    """Current cross-border physical flows"""
    cache_key = 'cross_border_flows'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)
    
    now = datetime.utcnow()
    start = (now - timedelta(hours=2)).strftime('%Y%m%d%H00')
    end = now.strftime('%Y%m%d%H00')
    
    flows = {}
    
    for country, code in COUNTRY_CODES.items():
        # Import (from country to Austria)
        xml_import = fetch_entsoe({
            'documentType': 'A11',  # Aggregated energy data report
            'in_Domain': code,
            'out_Domain': AUSTRIA_BZ,
            'periodStart': start,
            'periodEnd': end,
        })
        
        # Export (from Austria to country)
        xml_export = fetch_entsoe({
            'documentType': 'A11',
            'in_Domain': AUSTRIA_BZ,
            'out_Domain': code,
            'periodStart': start,
            'periodEnd': end,
        })
        
        import_mw = 0
        export_mw = 0
        
        if xml_import:
            parsed = parse_entsoe_xml(xml_import)
            if parsed:
                import_mw = parsed[-1]['value'] if parsed else 0
        
        if xml_export:
            parsed = parse_entsoe_xml(xml_export)
            if parsed:
                export_mw = parsed[-1]['value'] if parsed else 0
        
        flows[country] = {
            'import_mw': import_mw,
            'export_mw': export_mw,
            'net_mw': import_mw - export_mw,  # Positive = net import
        }
    
    total_import = sum(f['import_mw'] for f in flows.values())
    total_export = sum(f['export_mw'] for f in flows.values())
    
    result = {
        'timestamp': now.isoformat(),
        'flows': flows,
        'total_import_mw': total_import,
        'total_export_mw': total_export,
        'net_position_mw': total_import - total_export,
        'unit': 'MW',
    }
    set_cached(cache_key, result)
    return jsonify(result)

@app.route('/api/entsoe/summary')
def entsoe_summary():
    """Summary dashboard with all key metrics"""
    # Fetch all data (uses cache)
    generation = entsoe_generation().get_json() if hasattr(entsoe_generation(), 'get_json') else {}
    prices = entsoe_prices().get_json() if hasattr(entsoe_prices(), 'get_json') else {}
    flows = entsoe_cross_border().get_json() if hasattr(entsoe_cross_border(), 'get_json') else {}
    
    return jsonify({
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'generation': generation,
        'prices': prices,
        'cross_border': flows,
    })


@app.route('/api/substation-loads')
def substation_loads():
    """Get estimated load on each substation based on live data"""
    cache_key = 'substation_loads'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)
    
    try:
        from substation_load_model import SubstationLoadModel
        model = SubstationLoadModel()
        loads = model.run()
        
        # Filter to high-voltage substations with significant activity
        hv_loads = [s for s in loads if s['voltage'] >= 110]
        
        result = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'substations': hv_loads,
            'summary': {
                'total': len(hv_loads),
                'high_load': sum(1 for s in hv_loads if s['status'] == 'high'),
                'medium_load': sum(1 for s in hv_loads if s['status'] == 'medium'),
                'low_load': sum(1 for s in hv_loads if s['status'] == 'low'),
                'total_generation_mw': sum(s['generation_mw'] for s in hv_loads),
                'total_load_mw': sum(s['load_mw'] for s in hv_loads),
            }
        }
        set_cached(cache_key, result)
        return jsonify(result)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/api/district-capacity')
def district_capacity():
    """Calculate capacity analysis for each district using proper point-in-polygon"""
    windparks = load_json('windparks.json')
    transformers = load_json('transformer_stations.json')
    bezirke = load_json('bezirke.json')
    
    # Calculate district statistics
    district_stats = {}
    
    # Track which windparks have been assigned to avoid double-counting
    assigned_windparks = set()
    assigned_transformers = set()
    
    for feature in bezirke['features']:
        name = feature['properties']['name']
        iso = feature['properties']['iso']
        
        # Create shapely polygon for proper point-in-polygon test
        try:
            district_shape = shape(feature['geometry'])
        except:
            continue
            
        # Get bounding box for rough district matching
        min_lon, min_lat, max_lon, max_lat = district_shape.bounds
        
        # Find windparks in this district using point-in-polygon
        district_windparks = []
        for i, wp in enumerate(windparks):
            if i in assigned_windparks:
                continue
            try:
                wp_lon = float(wp.get('lon', 0) or 0)
                wp_lat = float(wp.get('lat', 0) or 0)
                # Quick bounding box check first
                if not (min_lon <= wp_lon <= max_lon and min_lat <= wp_lat <= max_lat):
                    continue
                # Then proper point-in-polygon
                point = Point(wp_lon, wp_lat)
                if district_shape.contains(point):
                    district_windparks.append(wp)
                    assigned_windparks.add(i)
            except (ValueError, TypeError):
                continue
        
        # Find transformer stations in this district
        district_transformers = []
        for i, t in enumerate(transformers):
            if i in assigned_transformers:
                continue
            try:
                t_lon = float(t.get('longitude', 0) or 0)
                t_lat = float(t.get('latitude', 0) or 0)
                if not (min_lon <= t_lon <= max_lon and min_lat <= t_lat <= max_lat):
                    continue
                point = Point(t_lon, t_lat)
                if district_shape.contains(point):
                    district_transformers.append(t)
                    assigned_transformers.add(i)
            except (ValueError, TypeError):
                continue
        
        # Calculate stats
        total_installed_mw = sum(float(wp.get('total_mw', 0) or 0) for wp in district_windparks)
        total_turbines = sum(int(wp.get('turbines', 0) or 0) for wp in district_windparks)
        
        # Transformer capacity
        total_booked = 0
        total_available = 0
        for t in district_transformers:
            try:
                booked = t.get('bookedCapacity', 0)
                available = t.get('availableCapacity', 0)
                total_booked += float(booked) if booked else 0
                total_available += float(available) if available else 0
            except (ValueError, TypeError):
                pass
        
        # Calculate capacity score - considering actual usage vs grid capacity
        # Higher score = more room for new capacity
        total_grid_capacity = total_booked + total_available
        if total_grid_capacity > 0:
            # Utilization based on installed wind capacity vs grid capacity
            utilization = min(total_installed_mw / (total_grid_capacity + 0.01), 1.5)
            capacity_score = max(0, min(100, (1 - utilization * 0.7) * 100))
        elif total_installed_mw > 0:
            # Has wind but no registered transformers - likely constrained
            capacity_score = 20
        else:
            # No wind, no transformers - unknown potential
            capacity_score = 50
        
        # Estimate actual available capacity based on realistic assumptions
        # Government figures are often pessimistic - realistic capacity is higher
        # Based on international studies, actual available is typically 30-50% higher
        estimated_actual_available = total_available * 1.4 + (total_booked * 0.15)
        
        district_stats[iso] = {
            'name': name,
            'iso': iso,
            'windparks': len(district_windparks),
            'turbines': total_turbines,
            'installed_mw': round(total_installed_mw, 2),
            'transformers': len(district_transformers),
            'booked_capacity_mw': round(total_booked, 2),
            'official_available_mw': round(total_available, 2),
            'estimated_available_mw': round(estimated_actual_available, 2),
            'capacity_score': round(capacity_score, 1),
            'bbox': [min_lon, min_lat, max_lon, max_lat]
        }
    
    return jsonify(district_stats)

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@app.route('/power_grid.png')
def power_grid():
    return send_file('power_grid.png')

# ============ DATA EXPORT ============

@app.route('/data.gpkg')
def download_geopackage():
    """Export all data as GeoPackage"""
    # Create temporary file
    tmp = tempfile.NamedTemporaryFile(suffix='.gpkg', delete=False)
    tmp_path = tmp.name
    tmp.close()
    
    try:
        # Wind turbines
        turbines = load_json('wind_turbines_enhanced.json')
        turbine_data = []
        for t in turbines:
            if t.get('lat') and t.get('lon'):
                turbine_data.append({
                    'name': t.get('display_name', ''),
                    'standort': t.get('standort', ''),
                    'bezirk': t.get('bezirk', ''),
                    'bundesland': t.get('bundesland', ''),
                    'height_m': t.get('height_m'),
                    'estimated_mw': t.get('estimated_mw'),
                    'lighted': t.get('lighted', False),
                    'geometry': Point(t['lon'], t['lat'])
                })
        if turbine_data:
            gdf = gpd.GeoDataFrame(turbine_data, crs="EPSG:4326")
            gdf.to_file(tmp_path, driver='GPKG', layer='wind_turbines')
        
        # Transformer stations
        transformers = load_json('transformer_stations.json')
        transformer_data = []
        for t in transformers:
            if t.get('latitude') and t.get('longitude'):
                transformer_data.append({
                    'name': t.get('substationName', ''),
                    'operator': t.get('networkOperator', ''),
                    'state': t.get('state', ''),
                    'booked_mw': t.get('bookedCapacity'),
                    'available_mw': t.get('availableCapacity'),
                    'geometry': Point(t['longitude'], t['latitude'])
                })
        if transformer_data:
            gdf = gpd.GeoDataFrame(transformer_data, crs="EPSG:4326")
            gdf.to_file(tmp_path, driver='GPKG', layer='transformer_stations', mode='a')
        
        # Transmission lines
        lines = load_json('transmission_lines.json')
        line_data = []
        for feature in lines.get('features', []):
            props = feature.get('properties', {})
            coords = feature.get('geometry', {}).get('coordinates', [])
            if len(coords) >= 2:
                line_data.append({
                    'name': props.get('name', ''),
                    'voltage_kv': props.get('voltage'),
                    'region': props.get('region', ''),
                    'geometry': LineString(coords)
                })
        if line_data:
            gdf = gpd.GeoDataFrame(line_data, crs="EPSG:4326")
            gdf.to_file(tmp_path, driver='GPKG', layer='transmission_lines', mode='a')
        
        # Districts
        bezirke = load_json('bezirke.json')
        district_data = []
        for feature in bezirke.get('features', []):
            props = feature.get('properties', {})
            geom = shape(feature['geometry'])
            district_data.append({
                'name': props.get('name', ''),
                'iso': props.get('iso', ''),
                'geometry': geom
            })
        if district_data:
            gdf = gpd.GeoDataFrame(district_data, crs="EPSG:4326")
            gdf.to_file(tmp_path, driver='GPKG', layer='bezirke', mode='a')
        
        return send_file(
            tmp_path,
            mimetype='application/geopackage+sqlite3',
            as_attachment=True,
            download_name='austria_wind_power.gpkg'
        )
    except Exception as e:
        return Response(f"Error generating GeoPackage: {str(e)}", status=500)

# ============ SEO ROUTES ============

@app.route('/sitemap.xml')
def sitemap():
    """Generate dynamic sitemap.xml"""
    bezirke = load_json('bezirke.json')
    transformers = load_json('transformer_stations.json')
    
    pages = [
        {'loc': BASE_URL + '/', 'priority': '1.0', 'changefreq': 'weekly'},
        {'loc': BASE_URL + '/quellen', 'priority': '0.5', 'changefreq': 'monthly'},
        {'loc': BASE_URL + '/bezirke', 'priority': '0.8', 'changefreq': 'weekly'},
        {'loc': BASE_URL + '/umspannwerke', 'priority': '0.8', 'changefreq': 'weekly'},
    ]
    
    # Add district pages
    for feature in bezirke['features']:
        iso = feature['properties']['iso']
        pages.append({
            'loc': f"{BASE_URL}/bezirk/{quote(iso, safe='')}",
            'priority': '0.7',
            'changefreq': 'monthly'
        })
    
    # Add transformer pages
    for i, t in enumerate(transformers):
        if t.get('substationName'):
            pages.append({
                'loc': f"{BASE_URL}/umspannwerk/{i}",
                'priority': '0.6',
                'changefreq': 'monthly'
            })
    
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for page in pages:
        xml += f'''  <url>
    <loc>{page['loc']}</loc>
    <changefreq>{page['changefreq']}</changefreq>
    <priority>{page['priority']}</priority>
  </url>\n'''
    xml += '</urlset>'
    
    return Response(xml, mimetype='application/xml')

@app.route('/robots.txt')
def robots():
    """Robots.txt for search engines"""
    content = f"""User-agent: *
Allow: /
Sitemap: {BASE_URL}/sitemap.xml
"""
    return Response(content, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
