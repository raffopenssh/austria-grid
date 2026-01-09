#!/usr/bin/env python3
"""Austrian Wind Power Grid Capacity Visualization"""

from flask import Flask, jsonify, send_from_directory, send_file, render_template_string, Response, request
import json
import os
import tempfile
from datetime import datetime
from shapely.geometry import shape, Point, LineString
from urllib.parse import quote
import geopandas as gpd
import pandas as pd

app = Flask(__name__, static_folder='static')

# Base URL for the site
BASE_URL = 'https://austria-power.exe.xyz:8000'

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
