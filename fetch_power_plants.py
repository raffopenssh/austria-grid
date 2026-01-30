#!/usr/bin/env python3
"""
Fetch all power plants in Austria from OpenStreetMap.
Creates a comprehensive dataset of all generation assets.
"""

import requests
import json
import os

DATA_DIR = '/home/exedev/austria-grid/data'

def fetch_osm_power_plants():
    """Fetch all power plants and generators from OSM."""
    
    # Query for all power plants
    query = """
    [out:json][timeout:120];
    area["ISO3166-1"="AT"]->.austria;
    (
      // Power plants
      node["power"="plant"](area.austria);
      way["power"="plant"](area.austria);
      relation["power"="plant"](area.austria);
      
      // Large generators (solar farms, etc)
      way["power"="generator"]["generator:output:electricity"](area.austria);
      relation["power"="generator"]["generator:output:electricity"](area.austria);
    );
    out center;
    """
    
    print("Fetching power plants from OSM...")
    response = requests.post(
        "https://overpass-api.de/api/interpreter",
        data=query,
        timeout=120
    )
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return None
    
    data = response.json()
    elements = data.get('elements', [])
    print(f"Found {len(elements)} power generation elements")
    
    return elements

def parse_capacity(value):
    """Parse capacity string to MW."""
    if not value:
        return None
    
    value = str(value).strip().lower()
    
    # Remove common suffixes and parse
    try:
        if 'gw' in value:
            return float(value.replace('gw', '').strip()) * 1000
        elif 'mw' in value:
            return float(value.replace('mw', '').replace('p', '').strip())
        elif 'kw' in value:
            return float(value.replace('kw', '').replace('p', '').strip()) / 1000
        elif 'w' in value:
            return float(value.replace('w', '').strip()) / 1000000
        else:
            # Try parsing as number (assume MW)
            num = float(value)
            if num > 10000:  # Probably kW
                return num / 1000
            return num
    except:
        return None

def categorize_source(tags):
    """Categorize power plant by source type."""
    source = tags.get('plant:source', tags.get('generator:source', '')).lower()
    
    if 'hydro' in source or 'water' in source:
        plant_type = tags.get('generator:type', tags.get('plant:type', '')).lower()
        if 'pump' in plant_type or 'pump' in source:
            return 'hydro_pumped'
        elif 'reservoir' in plant_type or 'dam' in source:
            return 'hydro_reservoir'
        else:
            return 'hydro_run_of_river'
    elif 'solar' in source or 'photovoltaic' in source:
        return 'solar'
    elif 'wind' in source:
        return 'wind'
    elif 'gas' in source:
        return 'gas'
    elif 'coal' in source:
        return 'coal'
    elif 'oil' in source:
        return 'oil'
    elif 'biomass' in source or 'biogas' in source or 'bio' in source:
        return 'biomass'
    elif 'waste' in source or 'müll' in tags.get('name', '').lower():
        return 'waste'
    elif 'nuclear' in source:
        return 'nuclear'
    elif 'geothermal' in source:
        return 'geothermal'
    else:
        return 'other'

def process_power_plants(elements):
    """Process OSM elements into power plant features."""
    plants = []
    
    for elem in elements:
        tags = elem.get('tags', {})
        
        # Get coordinates
        if elem['type'] == 'node':
            lat, lon = elem.get('lat'), elem.get('lon')
        else:
            # Use center for ways/relations
            center = elem.get('center', {})
            lat, lon = center.get('lat'), center.get('lon')
        
        if not lat or not lon:
            continue
        
        # Get name
        name = tags.get('name', tags.get('operator', 'Unknown'))
        
        # Get capacity
        capacity_str = tags.get('plant:output:electricity', 
                               tags.get('generator:output:electricity', ''))
        capacity_mw = parse_capacity(capacity_str)
        
        # Get source type
        source_type = categorize_source(tags)
        
        # Skip very small installations (< 0.1 MW) unless they have a name
        if capacity_mw is not None and capacity_mw < 0.1 and name == 'Unknown':
            continue
        
        plants.append({
            'type': 'Feature',
            'properties': {
                'id': elem.get('id'),
                'osm_type': elem['type'],
                'name': name,
                'source': source_type,
                'capacity_mw': capacity_mw,
                'operator': tags.get('operator', ''),
                'voltage': tags.get('voltage', ''),
                'raw_output': capacity_str,
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [lon, lat]
            }
        })
    
    return plants

def save_power_plants(plants):
    """Save power plants to GeoJSON file."""
    
    # Group by source type for statistics
    by_source = {}
    for p in plants:
        src = p['properties']['source']
        if src not in by_source:
            by_source[src] = {'count': 0, 'capacity_mw': 0}
        by_source[src]['count'] += 1
        if p['properties']['capacity_mw']:
            by_source[src]['capacity_mw'] += p['properties']['capacity_mw']
    
    print("\nPower plants by source:")
    for src, stats in sorted(by_source.items()):
        print(f"  {src}: {stats['count']} plants, {stats['capacity_mw']:.1f} MW")
    
    geojson = {
        'type': 'FeatureCollection',
        'metadata': {
            'source': 'OpenStreetMap',
            'description': 'All power plants in Austria',
            'count': len(plants),
            'by_source': by_source,
        },
        'features': plants
    }
    
    output_path = os.path.join(DATA_DIR, 'all_power_plants.json')
    with open(output_path, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"\nSaved {len(plants)} power plants to {output_path}")
    return output_path

def main():
    elements = fetch_osm_power_plants()
    if not elements:
        print("Failed to fetch data")
        return
    
    plants = process_power_plants(elements)
    save_power_plants(plants)

if __name__ == '__main__':
    main()
