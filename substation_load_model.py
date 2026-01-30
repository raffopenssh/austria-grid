#!/usr/bin/env python3
"""
Sophisticated Substation Load Model for Austrian Grid.

This model estimates the load on each substation (Umspannwerk/UW) by:
1. Loading all power plants with their capacities
2. Estimating current production based on ENTSO-E generation data
3. Assigning production to nearest substations
4. Distributing load based on regional factors
5. Calculating net flow through each substation

The model provides real-time estimates that update with ENTSO-E data.
"""

import json
import sqlite3
import math
from datetime import datetime, timezone
import os
import requests

# Paths
DATA_DIR = '/home/exedev/austria-grid/data'
DB_PATH = f'{DATA_DIR}/entsoe_data.db'

# ENTSO-E generation type mapping to our source categories
ENTSOE_TO_SOURCE = {
    'Hydro Run-of-river and poundage': 'hydro_run_of_river',
    'Hydro Water Reservoir': 'hydro_reservoir', 
    'Hydro Pumped Storage': 'hydro_pumped',
    'Wasserkraft (Laufwasser)': 'hydro_run_of_river',
    'Wasserkraft (Speicher)': 'hydro_reservoir',
    'Wasserkraft (Pumpspeicher)': 'hydro_pumped',
    'Wind Onshore': 'wind',
    'Wind Offshore': 'wind',
    'Solar': 'solar',
    'Fossil Gas': 'gas',
    'Erdgas': 'gas',
    'Gas': 'gas',
    'Fossil Hard coal': 'coal',
    'Fossil Oil': 'oil',
    'Biomass': 'biomass',
    'Biomasse': 'biomass',
    'Waste': 'waste',
    'Abfall': 'waste',
    'Geothermal': 'geothermal',
    'Geothermie': 'geothermal',
    'Other renewable': 'other',
    'Other': 'other',
    'Andere': 'other',
    'Andere erneuerbare': 'other',
}

def load_json(filename):
    with open(f'{DATA_DIR}/{filename}', 'r') as f:
        return json.load(f)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


class PowerPlant:
    """Represents a power plant with current production estimate."""
    def __init__(self, feature):
        props = feature['properties']
        coords = feature['geometry']['coordinates']
        
        self.id = props.get('id', '')
        self.name = props.get('name', 'Unknown')
        self.source = props.get('source', 'other')
        self.capacity_mw = props.get('capacity_mw') or props.get('mw') or 0
        self.lon = coords[0]
        self.lat = coords[1]
        self.operator = props.get('operator', '')
        
        # Current production (will be estimated)
        self.current_production_mw = 0
        self.utilization_factor = 0
        
        # Assigned substation
        self.assigned_substation = None
        
    def estimate_production(self, utilization_factors):
        """Estimate current production based on source utilization factor."""
        factor = utilization_factors.get(self.source, 0.3)
        self.utilization_factor = factor
        self.current_production_mw = self.capacity_mw * factor
        return self.current_production_mw


class Substation:
    """Represents a substation with load calculations."""
    def __init__(self, data, source='osm'):
        if source == 'osm':
            props = data['properties']
            coords = data['geometry']['coordinates']
            if data['geometry']['type'] == 'Point':
                self.lon, self.lat = coords
            else:
                # Centroid for polygons
                if isinstance(coords[0][0], list):
                    coords = coords[0]
                self.lon = sum(c[0] for c in coords) / len(coords)
                self.lat = sum(c[1] for c in coords) / len(coords)
            
            self.id = props.get('id', f"osm_{data.get('id', '')}")
            self.name = props.get('name', '')
            self.voltage = self._parse_voltage(props.get('voltage', 380))
            self.operator = props.get('operator', '')
        else:
            # From transformer stations JSON
            self.id = data.get('substationId', '')
            self.name = data.get('substationName', '')
            self.lat = data.get('latitude', 0)
            self.lon = data.get('longitude', 0)
            self.voltage = 110
            self.operator = data.get('operator', '')
        
        # Estimate capacity based on voltage
        if self.voltage >= 380:
            self.capacity_mva = 2000
        elif self.voltage >= 220:
            self.capacity_mva = 750
        else:
            self.capacity_mva = 300
        
        # Power flow components
        self.generation_mw = 0
        self.load_mw = 0
        self.crossborder_mw = 0
        self.net_flow_mw = 0
        self.load_percent = 0
        self.status = 'unknown'
        
        # Connected plants
        self.connected_plants = []
        self.plants_by_source = {}
        
    def _parse_voltage(self, v):
        """Parse voltage string to integer kV."""
        try:
            v_str = str(v).replace('kV', '').split(';')[0].strip()
            v_int = int(float(v_str))
            if v_int > 1000:
                v_int = v_int // 1000
            return v_int
        except:
            return 380
    
    def add_plant(self, plant):
        """Add a power plant to this substation."""
        self.connected_plants.append(plant)
        plant.assigned_substation = self
        
        # Track by source
        src = plant.source
        if src not in self.plants_by_source:
            self.plants_by_source[src] = []
        self.plants_by_source[src].append(plant)
    
    def calculate_generation(self):
        """Sum up generation from all connected plants."""
        self.generation_mw = sum(p.current_production_mw for p in self.connected_plants)
        return self.generation_mw
    
    def get_generation_breakdown(self):
        """Get generation breakdown by source."""
        breakdown = {}
        for src, plants in self.plants_by_source.items():
            total = sum(p.current_production_mw for p in plants)
            if total > 0:
                breakdown[src] = {
                    'production_mw': total,
                    'capacity_mw': sum(p.capacity_mw for p in plants),
                    'plant_count': len(plants),
                }
        return breakdown


class SubstationLoadModel:
    def __init__(self):
        self.power_plants = []
        self.substations = []
        self.generation_data = {}
        self.load_data = {}
        self.crossborder_data = {}
        self.utilization_factors = {}
        
        # Regional load factors
        self.regional_load_factors = {
            'Wien': 2.5,
            'Oberösterreich': 1.5,
            'Steiermark': 1.2,
            'Niederösterreich': 1.3,
            'Salzburg': 0.8,
            'Tirol': 0.7,
            'Kärnten': 0.6,
            'Vorarlberg': 0.5,
            'Burgenland': 0.4,
        }
        
        # Border regions for cross-border flow assignment
        self.border_regions = {
            'DE': {'lat_range': (47.5, 48.8), 'lon_range': (9.5, 13.0)},
            'CZ': {'lat_range': (48.5, 49.0), 'lon_range': (14.5, 17.0)},
            'SK': {'lat_range': (47.8, 48.5), 'lon_range': (16.5, 17.5)},
            'HU': {'lat_range': (46.8, 47.8), 'lon_range': (16.0, 17.5)},
            'SI': {'lat_range': (46.3, 47.0), 'lon_range': (13.5, 16.0)},
            'IT': {'lat_range': (46.3, 47.3), 'lon_range': (10.0, 13.0)},
            'CH': {'lat_range': (46.8, 47.5), 'lon_range': (9.5, 10.5)},
        }
        
    def load_power_plants(self):
        """Load all power plants from multiple data sources and merge."""
        plants_by_location = {}  # Key: (rounded lat, lon) to avoid duplicates
        
        # 1. Load from all_power_plants.json (OSM data)
        try:
            data = load_json('all_power_plants.json')
            for feature in data.get('features', []):
                plant = PowerPlant(feature)
                if plant.capacity_mw and plant.capacity_mw > 0:
                    key = (round(plant.lat, 3), round(plant.lon, 3))
                    if key not in plants_by_location or plant.capacity_mw > plants_by_location[key].capacity_mw:
                        plants_by_location[key] = plant
        except Exception as e:
            print(f"Could not load all_power_plants.json: {e}")
        
        # 2. Load hydropower with better categorization (pumped storage, reservoir)
        try:
            hydro = load_json('hydropower_plants.json')
            for feature in hydro.get('features', []):
                props = feature['properties']
                plant_type = props.get('type', '').lower()
                
                # Determine source type
                if 'pump' in plant_type:
                    source = 'hydro_pumped'
                elif 'speicher' in plant_type:
                    source = 'hydro_reservoir'
                else:
                    source = 'hydro_run_of_river'
                
                feature['properties']['capacity_mw'] = props.get('mw', 0)
                feature['properties']['source'] = source
                feature['properties']['name'] = props.get('name', f"{props.get('river', '')} {plant_type}")
                
                plant = PowerPlant(feature)
                if plant.capacity_mw > 0:
                    key = (round(plant.lat, 3), round(plant.lon, 3))
                    # Prefer this data if it has better categorization
                    if key not in plants_by_location or source != 'hydro_run_of_river':
                        plants_by_location[key] = plant
        except Exception as e:
            print(f"Could not load hydropower_plants.json: {e}")
        
        # 3. Load wind turbines
        try:
            wind = load_json('wind_turbines_enhanced.json')
            for t in wind:
                if t.get('lat') and t.get('lon'):
                    feature = {
                        'properties': {
                            'name': t.get('name', 'Windkraftanlage'),
                            'capacity_mw': t.get('estimated_mw', 3.0),
                            'source': 'wind',
                        },
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [t['lon'], t['lat']]
                        }
                    }
                    plant = PowerPlant(feature)
                    key = (round(plant.lat, 3), round(plant.lon, 3))
                    if key not in plants_by_location:
                        plants_by_location[key] = plant
        except Exception as e:
            print(f"Could not load wind_turbines_enhanced.json: {e}")
        
        self.power_plants = list(plants_by_location.values())
        
        # Count by source
        self.capacity_by_source = {}
        for p in self.power_plants:
            if p.source not in self.capacity_by_source:
                self.capacity_by_source[p.source] = {'count': 0, 'capacity': 0}
            self.capacity_by_source[p.source]['count'] += 1
            self.capacity_by_source[p.source]['capacity'] += p.capacity_mw
        
        print(f"Loaded {len(self.power_plants)} power plants:")
        for src, stats in sorted(self.capacity_by_source.items()):
            print(f"  {src}: {stats['count']} plants, {stats['capacity']:.0f} MW")
    
    def load_substations(self):
        """Load substations from OSM data."""
        try:
            osm_subs = load_json('osm_substations.json')
            for feature in osm_subs.get('features', []):
                sub = Substation(feature, source='osm')
                if sub.voltage >= 110:
                    self.substations.append(sub)
        except:
            pass
        
        # Also load transformer stations
        try:
            transformers = load_json('transformer_stations.json')
            existing_coords = set((round(s.lat, 3), round(s.lon, 3)) for s in self.substations)
            
            for t in transformers:
                if t.get('latitude') and t.get('longitude'):
                    coord_key = (round(t['latitude'], 3), round(t['longitude'], 3))
                    if coord_key not in existing_coords:
                        sub = Substation(t, source='transformer')
                        self.substations.append(sub)
                        existing_coords.add(coord_key)
        except:
            pass
        
        print(f"Loaded {len(self.substations)} substations")
    
    def load_live_data(self):
        """Load current generation data from ENTSO-E API."""
        try:
            response = requests.get('http://localhost:8000/api/entsoe/generation', timeout=15)
            if response.status_code == 200:
                data = response.json()
                self.generation_data = data.get('generation', {})
                print(f"Live generation: {sum(self.generation_data.values()):.0f} MW")
        except Exception as e:
            print(f"Could not fetch live generation: {e}")
            self.generation_data = {}
        
        # Get cross-border flows
        try:
            response = requests.get('http://localhost:8000/api/entsoe/cross-border-flows', timeout=15)
            if response.status_code == 200:
                data = response.json()
                for country, flow in data.get('flows', {}).items():
                    self.crossborder_data[country] = {
                        'import': flow.get('import_mw', 0),
                        'export': flow.get('export_mw', 0),
                        'net': flow.get('net_mw', 0),
                    }
        except Exception as e:
            print(f"Could not fetch cross-border flows: {e}")
        
        # Calculate total load (generation + net imports)
        total_gen = sum(self.generation_data.values())
        net_imports = sum(f.get('net', 0) for f in self.crossborder_data.values())
        self.load_data['total'] = total_gen + net_imports if total_gen > 0 else 7000
        
        print(f"Total load estimate: {self.load_data['total']:.0f} MW")
    
    def calculate_utilization_factors(self):
        """Calculate utilization factors to match ENTSO-E totals exactly."""
        
        # Map ENTSO-E generation to our source categories
        entsoe_by_source = {}
        for entsoe_type, value in self.generation_data.items():
            src = ENTSOE_TO_SOURCE.get(entsoe_type, 'other')
            if src not in entsoe_by_source:
                entsoe_by_source[src] = 0
            entsoe_by_source[src] += value
        
        print("\nENTSO-E generation by source:")
        for src, gen in sorted(entsoe_by_source.items()):
            print(f"  {src}: {gen:.0f} MW")
        
        # Calculate utilization factors to match ENTSO-E exactly
        print("\nCalibrated utilization factors:")
        for src, stats in self.capacity_by_source.items():
            capacity = stats['capacity']
            entsoe_gen = entsoe_by_source.get(src, 0)
            
            if capacity > 0 and entsoe_gen > 0:
                # Calculate factor that makes our production match ENTSO-E
                factor = entsoe_gen / capacity
                # Cap at 1.0 (100% utilization) but allow slight overage for rounding
                factor = min(factor, 1.05)
                self.utilization_factors[src] = factor
                print(f"  {src}: {entsoe_gen:.0f} MW / {capacity:.0f} MW = {factor:.1%}")
            elif capacity > 0:
                # No ENTSO-E data for this source, use defaults
                defaults = {
                    'hydro_run_of_river': 0.05,
                    'hydro_reservoir': 0.10,
                    'hydro_pumped': 0.02,
                    'wind': 0.04,
                    'solar': 0.0,
                    'gas': 0.28,
                    'coal': 0.0,
                    'biomass': 0.80,
                    'waste': 0.36,
                    'other': 0.30,
                }
                self.utilization_factors[src] = defaults.get(src, 0.1)
            else:
                self.utilization_factors[src] = 0
        
        # Store ENTSO-E totals for validation
        self.entsoe_by_source = entsoe_by_source
    
    def estimate_plant_production(self):
        """Estimate current production for each plant, calibrated to ENTSO-E."""
        # First pass: calculate raw production
        production_by_source = {}
        for plant in self.power_plants:
            plant.estimate_production(self.utilization_factors)
            src = plant.source
            if src not in production_by_source:
                production_by_source[src] = 0
            production_by_source[src] += plant.current_production_mw
        
        # Second pass: adjust to match ENTSO-E exactly
        print("\nProduction calibration:")
        for src, model_prod in production_by_source.items():
            entsoe_prod = self.entsoe_by_source.get(src, 0)
            if model_prod > 0 and entsoe_prod > 0:
                # Calculate adjustment factor
                adjustment = entsoe_prod / model_prod
                if abs(adjustment - 1.0) > 0.01:  # Only adjust if > 1% difference
                    print(f"  {src}: adjusting {model_prod:.0f} -> {entsoe_prod:.0f} MW (factor: {adjustment:.3f})")
                    # Apply adjustment to all plants of this source
                    for plant in self.power_plants:
                        if plant.source == src:
                            plant.current_production_mw *= adjustment
                            plant.utilization_factor *= adjustment
        
        # Calculate final total
        total_production = sum(p.current_production_mw for p in self.power_plants)
        entsoe_total = sum(self.generation_data.values())
        
        print(f"\nFinal production: {total_production:.0f} MW (ENTSO-E: {entsoe_total:.0f} MW)")
        
        # Validate
        if entsoe_total > 0:
            match_pct = (total_production / entsoe_total) * 100
            print(f"Match: {match_pct:.1f}%")
        
        return total_production
    
    def assign_plants_to_substations(self):
        """Assign each power plant to the nearest suitable substation."""
        # Build spatial index of substations by voltage
        hv_substations = [s for s in self.substations if s.voltage >= 220]
        mv_substations = [s for s in self.substations if s.voltage >= 110]
        
        for plant in self.power_plants:
            # Large plants (> 50 MW) connect to HV, others to nearest
            if plant.capacity_mw > 50:
                candidates = hv_substations
                max_dist = 50
            else:
                candidates = mv_substations
                max_dist = 30
            
            min_dist = float('inf')
            nearest = None
            
            for sub in candidates:
                dist = haversine_distance(plant.lat, plant.lon, sub.lat, sub.lon)
                if dist < min_dist:
                    min_dist = dist
                    nearest = sub
            
            if nearest and min_dist < max_dist:
                nearest.add_plant(plant)
        
        # Count assignments
        assigned = sum(1 for p in self.power_plants if p.assigned_substation)
        print(f"Assigned {assigned}/{len(self.power_plants)} plants to substations")
    
    def calculate_generation_per_substation(self):
        """Calculate total generation at each substation."""
        for sub in self.substations:
            sub.calculate_generation()
    
    def distribute_load(self):
        """Distribute national load to substations."""
        total_load = self.load_data.get('total', 7000)
        
        # Calculate total weight
        total_weight = 0
        for sub in self.substations:
            region = self._get_region(sub.lat, sub.lon)
            factor = self.regional_load_factors.get(region, 0.5)
            voltage_factor = sub.voltage / 110
            sub._load_weight = factor * voltage_factor
            total_weight += sub._load_weight
        
        # Distribute load
        for sub in self.substations:
            sub.load_mw = total_load * (sub._load_weight / total_weight)
    
    def _get_region(self, lat, lon):
        """Determine region from coordinates."""
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
        for country, flow in self.crossborder_data.items():
            if country not in self.border_regions:
                continue
            
            region = self.border_regions[country]
            net_flow = flow.get('net', 0)
            
            # Find substations in border region
            border_subs = []
            for sub in self.substations:
                if (region['lat_range'][0] <= sub.lat <= region['lat_range'][1] and
                    region['lon_range'][0] <= sub.lon <= region['lon_range'][1] and
                    sub.voltage >= 220):
                    border_subs.append(sub)
            
            # Distribute flow among border substations
            if border_subs:
                flow_per_sub = net_flow / len(border_subs)
                for sub in border_subs:
                    sub.crossborder_mw += flow_per_sub
    
    def calculate_substation_loads(self):
        """Calculate final load percentage for each substation."""
        for sub in self.substations:
            # Net flow = generation - load + imports
            sub.net_flow_mw = sub.generation_mw - sub.load_mw + sub.crossborder_mw
            
            # Load is the magnitude of power flowing through
            flow_magnitude = abs(sub.net_flow_mw)
            capacity_mw = sub.capacity_mva * 0.9  # Power factor
            
            if capacity_mw > 0:
                sub.load_percent = min((flow_magnitude / capacity_mw) * 100, 150)
            else:
                sub.load_percent = 0
            
            # Status based on load percentage
            if sub.load_percent > 80:
                sub.status = 'high'
            elif sub.load_percent > 50:
                sub.status = 'medium'
            else:
                sub.status = 'low'
    
    def get_results(self):
        """Get results as list of dictionaries."""
        results = []
        for sub in self.substations:
            if sub.voltage >= 110:
                # Get top connected plants
                top_plants = sorted(sub.connected_plants, 
                                   key=lambda p: p.current_production_mw, 
                                   reverse=True)[:5]
                
                results.append({
                    'id': sub.id,
                    'name': sub.name,
                    'lat': sub.lat,
                    'lon': sub.lon,
                    'voltage': sub.voltage,
                    'capacity_mva': sub.capacity_mva,
                    'generation_mw': sub.generation_mw,
                    'load_mw': sub.load_mw,
                    'crossborder_mw': sub.crossborder_mw,
                    'net_flow_mw': sub.net_flow_mw,
                    'load_percent': sub.load_percent,
                    'status': sub.status,
                    'plant_count': len(sub.connected_plants),
                    'connected_plants': [
                        {
                            'name': p.name,
                            'source': p.source,
                            'capacity_mw': p.capacity_mw,
                            'production_mw': p.current_production_mw,
                            'utilization': p.utilization_factor,
                        }
                        for p in top_plants
                    ],
                    'generation_breakdown': sub.get_generation_breakdown(),
                })
        
        return results
    
    def get_all_plants(self):
        """Get all power plants with current production."""
        return [
            {
                'id': p.id,
                'name': p.name,
                'source': p.source,
                'lat': p.lat,
                'lon': p.lon,
                'capacity_mw': p.capacity_mw,
                'production_mw': p.current_production_mw,
                'utilization': p.utilization_factor,
                'substation': p.assigned_substation.name if p.assigned_substation else None,
            }
            for p in self.power_plants
        ]
    
    def run(self):
        """Run the complete model."""
        print("="*60)
        print("SUBSTATION LOAD MODEL")
        print("="*60)
        
        print("\n1. Loading power plants...")
        self.load_power_plants()
        
        print("\n2. Loading substations...")
        self.load_substations()
        
        print("\n3. Loading live ENTSO-E data...")
        self.load_live_data()
        
        print("\n4. Calculating utilization factors...")
        self.calculate_utilization_factors()
        
        print("\n5. Estimating plant production...")
        self.estimate_plant_production()
        
        print("\n6. Assigning plants to substations...")
        self.assign_plants_to_substations()
        
        print("\n7. Calculating generation per substation...")
        self.calculate_generation_per_substation()
        
        print("\n8. Distributing load...")
        self.distribute_load()
        
        print("\n9. Assigning cross-border flows...")
        self.assign_crossborder_flows()
        
        print("\n10. Calculating substation loads...")
        self.calculate_substation_loads()
        
        return self.get_results()


def get_substation_loads_json():
    """Get substation loads as JSON (for API endpoint)."""
    model = SubstationLoadModel()
    loads = model.run()
    
    # Get summary stats
    high_load = sum(1 for s in loads if s['status'] == 'high')
    medium_load = sum(1 for s in loads if s['status'] == 'medium')
    
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'substations': loads,
        'power_plants': model.get_all_plants(),
        'utilization_factors': model.utilization_factors,
        'summary': {
            'total_substations': len(loads),
            'high_load': high_load,
            'medium_load': medium_load,
            'low_load': len(loads) - high_load - medium_load,
            'total_plants': len(model.power_plants),
            'total_generation_mw': sum(s['generation_mw'] for s in loads),
            'total_load_mw': sum(s['load_mw'] for s in loads),
        }
    }


if __name__ == '__main__':
    model = SubstationLoadModel()
    results = model.run()
    
    print("\n" + "="*60)
    print("TOP SUBSTATIONS BY LOAD")
    print("="*60)
    
    # Sort by load
    results.sort(key=lambda x: x['load_percent'], reverse=True)
    
    # Show top 20
    print(f"\n{'Name':<35} {'V':>5} {'Gen':>8} {'Load':>8} {'%':>6} {'Plants':>6}")
    print("-"*75)
    
    for sub in results[:20]:
        if sub['name']:
            print(f"{sub['name'][:35]:<35} {sub['voltage']:>4}kV {sub['generation_mw']:>7.0f}MW "
                  f"{sub['load_mw']:>7.0f}MW {sub['load_percent']:>5.1f}% {sub['plant_count']:>6}")
