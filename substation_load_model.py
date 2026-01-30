#!/usr/bin/env python3
"""
Substation Load Model for Austrian Grid.

This model estimates the load on each substation (Umspannwerk/UW) based on:
1. Nearby generation capacity (hydro, wind plants)
2. Network topology (connected transmission lines)
3. Regional load distribution
4. Cross-border flows at border substations

The model uses a simplified power flow approach where:
- Each substation is a node with generation, load, and through-flow
- Load is estimated as: (generation_in - consumption + imports) / capacity
"""

import json
import sqlite3
import math
from datetime import datetime, timezone
import os

# Paths
DATA_DIR = '/home/exedev/austria-grid/data'
DB_PATH = f'{DATA_DIR}/entsoe_data.db'

# Load static data
def load_json(filename):
    with open(f'{DATA_DIR}/{filename}', 'r') as f:
        return json.load(f)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371  # Earth's radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

class SubstationLoadModel:
    def __init__(self):
        self.substations = []
        self.hydro_plants = []
        self.wind_turbines = []
        self.transmission_lines = []
        self.cross_border = []
        self.generation_data = {}
        self.load_data = {}
        self.crossborder_data = {}
        
        # Regional factors (approximate population/industry distribution)
        # Higher values = more consumption in that region
        self.regional_load_factors = {
            'Wien': 2.5,        # Vienna - highest consumption
            'Oberösterreich': 1.5,  # Industrial
            'Steiermark': 1.2,
            'Niederösterreich': 1.3,
            'Salzburg': 0.8,
            'Tirol': 0.7,
            'Kärnten': 0.6,
            'Vorarlberg': 0.5,
            'Burgenland': 0.4,
        }
        
        # Border substations and their countries
        self.border_substations = {
            'Wien Südost': ['SK', 'HU'],
            'Bisamberg': ['CZ'],
            'Dürnrohr': ['CZ'],
            'St. Peter': ['DE'],
            'Westtirol': ['DE', 'CH', 'IT'],
            'Lienz': ['IT'],
            'Obersielach': ['SI'],
            'Kainachtal': ['SI'],
            'Ernsthofen': ['DE'],
        }
        
    def load_static_data(self):
        """Load static infrastructure data."""
        # Substations from OSM
        osm_subs = load_json('osm_substations.json')
        for feature in osm_subs.get('features', []):
            props = feature['properties']
            coords = feature['geometry']['coordinates']
            # Handle both Point and Polygon geometries
            if feature['geometry']['type'] == 'Point':
                lon, lat = coords
            else:
                # For polygons, use centroid approximation
                if isinstance(coords[0][0], list):
                    coords = coords[0]
                lon = sum(c[0] for c in coords) / len(coords)
                lat = sum(c[1] for c in coords) / len(coords)
            
            voltage = props.get('voltage', 380)
            try:
                voltage_str = str(voltage).replace('kV', '').replace('000', '').split(';')[0]
                voltage = int(voltage_str)
                # Normalize voltage values
                if voltage > 1000:
                    voltage = voltage // 1000  # 380000 -> 380
            except:
                voltage = 380
            
            # Estimate capacity based on voltage
            # 380kV substations: ~1500-3000 MVA
            # 220kV substations: ~500-1000 MVA
            if voltage >= 380:
                capacity = 2000
            elif voltage >= 220:
                capacity = 750
            else:
                capacity = 300
            
            self.substations.append({
                'id': props.get('id', f"sub_{len(self.substations)}"),
                'name': props.get('name', 'Unknown'),
                'lat': lat,
                'lon': lon,
                'voltage': voltage,
                'capacity_mva': capacity,
                'operator': props.get('operator', ''),
            })
        
        # Also load transformer stations from the original data
        try:
            transformers = load_json('transformer_stations.json')
            for t in transformers:
                if t.get('latitude') and t.get('longitude'):
                    # Check if not already in list (by proximity)
                    is_duplicate = False
                    for s in self.substations:
                        if haversine_distance(t['latitude'], t['longitude'], s['lat'], s['lon']) < 1:
                            is_duplicate = True
                            # Update with more info if available
                            if t.get('substationName'):
                                s['name'] = t['substationName']
                            break
                    
                    if not is_duplicate:
                        self.substations.append({
                            'id': t.get('substationId', f"trans_{len(self.substations)}"),
                            'name': t.get('substationName', 'Unknown'),
                            'lat': t['latitude'],
                            'lon': t['longitude'],
                            'voltage': 110,  # Most are 110kV
                            'capacity_mva': 300,
                            'operator': t.get('operator', ''),
                            'available_capacity': t.get('availableCapacity'),
                            'booked_capacity': t.get('bookedCapacity'),
                        })
        except:
            pass
        
        # Hydropower plants
        hydro = load_json('hydropower_plants.json')
        for feature in hydro.get('features', []):
            props = feature['properties']
            coords = feature['geometry']['coordinates']
            capacity = props.get('mw', 0) or props.get('capacity_mw', 0) or 0
            self.hydro_plants.append({
                'name': props.get('name', f"{props.get('river', 'Unknown')} {props.get('type', '')}"),
                'lat': coords[1],
                'lon': coords[0],
                'capacity_mw': capacity,
                'type': props.get('type', 'run-of-river'),
            })
        
        # Wind turbines  
        try:
            wind = load_json('wind_turbines_enhanced.json')
            for t in wind:
                if t.get('lat') and t.get('lon'):
                    self.wind_turbines.append({
                        'lat': t['lat'],
                        'lon': t['lon'],
                        'capacity_mw': t.get('estimated_mw', 3.0),
                    })
        except Exception as e:
            print(f"Error loading wind turbines: {e}")
        
        # Transmission lines
        try:
            lines = load_json('osm_transmission_lines.json')
            for feature in lines.get('features', []):
                props = feature['properties']
                self.transmission_lines.append({
                    'voltage': props.get('voltage', 380),
                    'geometry': feature['geometry'],
                })
        except:
            pass
        
        # Cross-border connections
        try:
            cb = load_json('cross_border_connections.json')
            for feature in cb.get('features', []):
                props = feature['properties']
                coords = feature['geometry']['coordinates']
                # Get Austrian end of connection
                if len(coords) >= 2:
                    # Find which end is in Austria (roughly lat 46-49, lon 9.5-17)
                    for c in coords:
                        if 46 <= c[1] <= 49 and 9.5 <= c[0] <= 17:
                            self.cross_border.append({
                                'name': props.get('name', 'Unknown'),
                                'lat': c[1],
                                'lon': c[0],
                                'capacity_mw': props.get('capacity_mw', 1000),
                                'country': props.get('to_country', 'Unknown'),
                            })
                            break
        except:
            pass
        
        print(f"Loaded: {len(self.substations)} substations, {len(self.hydro_plants)} hydro plants, "
              f"{len(self.wind_turbines)} wind turbines, {len(self.cross_border)} cross-border points")
    
    def load_live_data(self):
        """Load latest data from ENTSO-E API or database."""
        import requests
        
        API_KEY = '35efd923-6969-4470-b2bd-0155b2254346'
        
        # Try to get live data from API first
        try:
            response = requests.get(f'http://localhost:8000/api/entsoe/generation', timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.generation_data = data.get('generation', {})
                print(f"Got generation from API: {sum(self.generation_data.values()):.0f} MW")
        except Exception as e:
            print(f"API error: {e}, falling back to database")
            self.generation_data = {}
        
        # Get load from API
        try:
            # Use total generation + net imports as proxy for load
            response = requests.get(f'http://localhost:8000/api/entsoe/cross-border-flows', timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.crossborder_data = {}
                for country, flow in data.get('flows', {}).items():
                    self.crossborder_data[country] = {
                        'import': flow.get('import_mw', 0),
                        'export': flow.get('export_mw', 0)
                    }
                net_imports = data.get('net_position_mw', 0)
                total_gen = sum(self.generation_data.values())
                self.load_data['total'] = total_gen + net_imports if total_gen > 0 else 7000
                print(f"Cross-border net position: {net_imports:.0f} MW")
        except Exception as e:
            print(f"Crossborder API error: {e}")
            self.load_data['total'] = 7000
            self.crossborder_data = {}
        
        # Fall back to database if API didn't work
        if not self.generation_data and os.path.exists(DB_PATH):
            conn = sqlite3.connect(DB_PATH)
            
            # Get most recent non-zero generation values
            gen_df = conn.execute('''
                SELECT psr_type, value_mw
                FROM generation g1
                WHERE value_mw > 0 
                AND timestamp = (
                    SELECT MAX(timestamp) FROM generation g2 
                    WHERE g2.psr_type = g1.psr_type AND g2.value_mw > 0
                )
            ''').fetchall()
            
            self.generation_data = {row[0]: row[1] for row in gen_df}
            
            # Latest load
            load_row = conn.execute('''
                SELECT load_mw FROM load 
                WHERE load_mw > 0
                ORDER BY timestamp DESC LIMIT 1
            ''').fetchone()
            
            if load_row:
                self.load_data['total'] = load_row[0]
            
            conn.close()
        
        print(f"Live data: Generation {sum(self.generation_data.values()):.0f} MW, "
              f"Load {self.load_data.get('total', 0):.0f} MW")
    
    def assign_generation_to_substations(self):
        """Assign generation capacity to nearest substations."""
        for sub in self.substations:
            sub['generation_mw'] = 0
            sub['hydro_mw'] = 0
            sub['wind_mw'] = 0
            sub['connected_plants'] = []
        
        # Assign hydro plants (within 30km of a 380/220kV substation)
        for plant in self.hydro_plants:
            min_dist = float('inf')
            nearest_sub = None
            
            for sub in self.substations:
                if sub['voltage'] >= 220:  # Only high voltage
                    dist = haversine_distance(plant['lat'], plant['lon'], sub['lat'], sub['lon'])
                    if dist < min_dist:
                        min_dist = dist
                        nearest_sub = sub
            
            if nearest_sub and min_dist < 50:  # Within 50km
                cap = plant['capacity_mw']
                nearest_sub['hydro_mw'] += cap
                nearest_sub['generation_mw'] += cap
                nearest_sub['connected_plants'].append(plant['name'])
        
        # Assign wind turbines (within 20km of any substation)
        for turbine in self.wind_turbines:
            min_dist = float('inf')
            nearest_sub = None
            
            for sub in self.substations:
                dist = haversine_distance(turbine['lat'], turbine['lon'], sub['lat'], sub['lon'])
                if dist < min_dist:
                    min_dist = dist
                    nearest_sub = sub
            
            if nearest_sub and min_dist < 30:
                nearest_sub['wind_mw'] += turbine['capacity_mw']
                nearest_sub['generation_mw'] += turbine['capacity_mw']
    
    def assign_load_to_substations(self):
        """Distribute national load to substations based on regional factors."""
        total_load = self.load_data.get('total', 7000)
        
        # Calculate total regional weight
        total_weight = 0
        for sub in self.substations:
            # Determine region based on coordinates (simplified)
            region = self.get_region(sub['lat'], sub['lon'])
            factor = self.regional_load_factors.get(region, 0.5)
            # Weight also by voltage (higher voltage = more through-flow)
            voltage_factor = sub['voltage'] / 110
            sub['load_weight'] = factor * voltage_factor
            total_weight += sub['load_weight']
        
        # Distribute load
        for sub in self.substations:
            sub['load_mw'] = total_load * (sub['load_weight'] / total_weight)
    
    def get_region(self, lat, lon):
        """Determine Austrian region based on coordinates (simplified)."""
        # Very rough approximation based on lat/lon
        if lon > 16 and lat > 48:
            return 'Wien'
        elif lon > 15.5 and lat > 48:
            return 'Niederösterreich'
        elif lon > 13 and lon < 15 and lat > 47.5:
            return 'Oberösterreich'
        elif lon > 14 and lat < 47.5:
            return 'Steiermark'
        elif lon < 11:
            return 'Vorarlberg'
        elif lon < 12.5 and lat < 47.5:
            return 'Tirol'
        elif lon > 12.5 and lon < 14 and lat > 47:
            return 'Salzburg'
        elif lon > 13 and lon < 15 and lat < 47:
            return 'Kärnten'
        elif lon > 16:
            return 'Burgenland'
        return 'Niederösterreich'
    
    def assign_crossborder_flows(self):
        """Assign cross-border flows to border substations."""
        for sub in self.substations:
            sub['crossborder_mw'] = 0
            sub['crossborder_countries'] = []
        
        # Find substations near border crossing points
        for cb in self.cross_border:
            min_dist = float('inf')
            nearest_sub = None
            
            for sub in self.substations:
                if sub['voltage'] >= 220:
                    dist = haversine_distance(cb['lat'], cb['lon'], sub['lat'], sub['lon'])
                    if dist < min_dist:
                        min_dist = dist
                        nearest_sub = sub
            
            if nearest_sub and min_dist < 30:
                country = cb['country']
                if country in self.crossborder_data:
                    flow = self.crossborder_data[country]
                    net_flow = flow['import'] - flow['export']  # Positive = import
                    nearest_sub['crossborder_mw'] += net_flow
                    nearest_sub['crossborder_countries'].append(country)
    
    def calculate_substation_load(self):
        """Calculate load percentage for each substation."""
        # Apply generation utilization factors from live data
        total_gen = sum(self.generation_data.values())
        
        # Find hydro generation (multiple possible key names)
        hydro_actual = 0
        for k, v in self.generation_data.items():
            k_lower = k.lower()
            if 'hydro' in k_lower or 'water' in k_lower or 'wasser' in k_lower:
                hydro_actual += v
        
        # Find wind generation
        wind_actual = 0
        for k, v in self.generation_data.items():
            k_lower = k.lower()
            if 'wind' in k_lower:
                wind_actual += v
        
        # Calculate utilization factors
        total_hydro_cap = sum(p['capacity_mw'] for p in self.hydro_plants)
        total_wind_cap = sum(t['capacity_mw'] for t in self.wind_turbines)
        
        hydro_factor = hydro_actual / total_hydro_cap if total_hydro_cap > 0 else 0.3
        wind_factor = wind_actual / total_wind_cap if total_wind_cap > 0 else 0.2
        
        print(f"Hydro: {hydro_actual:.0f} MW actual / {total_hydro_cap:.0f} MW capacity = {hydro_factor:.1%}")
        print(f"Wind: {wind_actual:.0f} MW actual / {total_wind_cap:.0f} MW capacity = {wind_factor:.1%}")
        
        print(f"Utilization factors: Hydro {hydro_factor:.1%}, Wind {wind_factor:.1%}")
        
        for sub in self.substations:
            # Current generation at this substation
            actual_gen = (sub['hydro_mw'] * hydro_factor + 
                         sub['wind_mw'] * wind_factor)
            
            # Net power flow through substation
            # Positive = power flowing into substation (from generation or import)
            # Negative = power flowing out (to load or export)
            net_flow = actual_gen - sub['load_mw'] + sub.get('crossborder_mw', 0)
            
            # Load as percentage of capacity
            # We consider both inflow and outflow as load on the substation
            flow_magnitude = abs(net_flow)
            
            # Capacity in MW (convert from MVA assuming power factor 0.9)
            capacity_mw = sub['capacity_mva'] * 0.9
            
            # Calculate load percentage
            if capacity_mw > 0:
                load_pct = (flow_magnitude / capacity_mw) * 100
            else:
                load_pct = 0
            
            sub['actual_generation_mw'] = actual_gen
            sub['net_flow_mw'] = net_flow
            sub['load_percent'] = min(load_pct, 150)  # Cap at 150%
            
            # Determine status
            if load_pct > 80:
                sub['status'] = 'high'
            elif load_pct > 50:
                sub['status'] = 'medium'
            else:
                sub['status'] = 'low'
    
    def get_substation_loads(self):
        """Get list of substations with their load estimates."""
        return [{
            'id': sub['id'],
            'name': sub['name'],
            'lat': sub['lat'],
            'lon': sub['lon'],
            'voltage': sub['voltage'],
            'capacity_mva': sub['capacity_mva'],
            'generation_mw': sub.get('actual_generation_mw', 0),
            'load_mw': sub.get('load_mw', 0),
            'crossborder_mw': sub.get('crossborder_mw', 0),
            'net_flow_mw': sub.get('net_flow_mw', 0),
            'load_percent': sub.get('load_percent', 0),
            'status': sub.get('status', 'unknown'),
            'connected_plants': sub.get('connected_plants', []),
            'crossborder_countries': sub.get('crossborder_countries', []),
        } for sub in self.substations if sub['voltage'] >= 110]
    
    def run(self):
        """Run the full model."""
        print("Loading static infrastructure data...")
        self.load_static_data()
        
        print("\nLoading live ENTSO-E data...")
        self.load_live_data()
        
        print("\nAssigning generation to substations...")
        self.assign_generation_to_substations()
        
        print("\nDistributing load to substations...")
        self.assign_load_to_substations()
        
        print("\nAssigning cross-border flows...")
        self.assign_crossborder_flows()
        
        print("\nCalculating substation loads...")
        self.calculate_substation_load()
        
        return self.get_substation_loads()

def get_substation_loads_json():
    """Get substation loads as JSON (for API endpoint)."""
    model = SubstationLoadModel()
    loads = model.run()
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'substations': loads,
        'summary': {
            'total_substations': len(loads),
            'high_load': sum(1 for s in loads if s['status'] == 'high'),
            'medium_load': sum(1 for s in loads if s['status'] == 'medium'),
            'low_load': sum(1 for s in loads if s['status'] == 'low'),
        }
    }

if __name__ == '__main__':
    model = SubstationLoadModel()
    loads = model.run()
    
    print(f"\n{'='*60}")
    print(f"SUBSTATION LOAD ESTIMATES")
    print(f"{'='*60}")
    
    # Sort by load percentage
    loads.sort(key=lambda x: x['load_percent'], reverse=True)
    
    print(f"\nTop 20 substations by load:")
    print(f"{'Name':<30} {'Voltage':>8} {'Gen MW':>10} {'Load %':>10} {'Status':>10}")
    print("-" * 70)
    
    # Filter to show high-voltage substations
    hv_loads = [s for s in loads if s['voltage'] >= 220]
    
    print(f"\nHigh-voltage (220kV+) substations:")
    print(f"{'Name':<35} {'V':>5} {'Gen':>8} {'Load':>8} {'Flow':>8} {'%':>6} {'Status':>8}")
    print("-" * 85)
    
    for sub in hv_loads[:25]:
        print(f"{sub['name'][:35]:<35} {sub['voltage']:>4}kV {sub['generation_mw']:>7.0f}MW "
              f"{sub['load_mw']:>7.0f}MW {sub['net_flow_mw']:>+7.0f}MW {sub['load_percent']:>5.1f}% {sub['status']:>8}")
