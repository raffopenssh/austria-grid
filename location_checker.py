#!/usr/bin/env python3
"""
Location Checker for Wind/Solar Installation Feasibility.
Provides information about grid connection possibilities at a given location.
"""

import json
import math
from typing import Dict, List, Optional

DATA_DIR = '/home/exedev/austria-grid/data'

def load_json(filename):
    with open(f'{DATA_DIR}/{filename}', 'r') as f:
        return json.load(f)

def parse_capacity(val):
    """Parse capacity value, handling German number format."""
    if not val:
        return 0
    try:
        return float(str(val).replace(',', '.'))
    except:
        return 0

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_region(lat: float, lon: float) -> str:
    """Determine Austrian region from coordinates."""
    if lon > 16.1 and lat > 48.1 and lat < 48.35:
        return 'Wien'
    elif lon > 15.5 and lat > 47.5:
        return 'Niederösterreich'
    elif lon > 13 and lon < 15 and lat > 47.5:
        return 'Oberösterreich'
    elif lon > 14 and lon < 16.5 and lat < 47.5:
        return 'Steiermark'
    elif lon < 10.3:
        return 'Vorarlberg'
    elif lon < 12.5 and lat < 47.5:
        return 'Tirol'
    elif lon > 12.5 and lon < 14 and lat > 47 and lat < 48:
        return 'Salzburg'
    elif lon > 13 and lon < 15 and lat < 47:
        return 'Kärnten'
    elif lon > 16:
        return 'Burgenland'
    return 'Niederösterreich'

# Regional capacity factors (approximate annual averages)
WIND_CAPACITY_FACTORS = {
    'Burgenland': 0.28,      # Best wind in Austria
    'Niederösterreich': 0.25,
    'Wien': 0.20,
    'Steiermark': 0.22,
    'Oberösterreich': 0.20,
    'Kärnten': 0.18,
    'Salzburg': 0.15,
    'Tirol': 0.15,
    'Vorarlberg': 0.15,
}

SOLAR_CAPACITY_FACTORS = {
    'Burgenland': 0.12,
    'Niederösterreich': 0.11,
    'Wien': 0.11,
    'Steiermark': 0.11,
    'Oberösterreich': 0.10,
    'Kärnten': 0.12,        # Good sunshine
    'Salzburg': 0.10,
    'Tirol': 0.11,
    'Vorarlberg': 0.10,
}

# Average sunshine hours per year
SUNSHINE_HOURS = {
    'Burgenland': 2000,
    'Niederösterreich': 1900,
    'Wien': 1900,
    'Steiermark': 1850,
    'Oberösterreich': 1700,
    'Kärnten': 2000,
    'Salzburg': 1700,
    'Tirol': 1800,
    'Vorarlberg': 1650,
}


class LocationChecker:
    def __init__(self):
        self.transformers = []
        self.substations = []
        self.wind_turbines = []
        self.solar_plants = []
        self.load_data()
    
    def load_data(self):
        """Load all relevant data."""
        # Load transformer stations (with grid operator and capacity)
        try:
            data = load_json('transformer_stations.json')
            for t in data:
                if t.get('latitude') and t.get('longitude'):
                    self.transformers.append({
                        'name': t.get('substationName', 'Unknown'),
                        'lat': t['latitude'],
                        'lon': t['longitude'],
                        'operator': t.get('networkOperator', 'Unknown'),
                        'available_mw': parse_capacity(t.get('availableCapacity')),
                        'booked_mw': parse_capacity(t.get('bookedCapacity')),
                        'contact': t.get('contact', ''),
                        'website': t.get('website', ''),
                    })
        except Exception as e:
            print(f"Error loading transformers: {e}")
        
        # Load OSM substations
        try:
            data = load_json('osm_substations.json')
            for f in data.get('features', []):
                props = f['properties']
                coords = f['geometry']['coordinates']
                if f['geometry']['type'] == 'Point':
                    lon, lat = coords
                else:
                    lon = sum(c[0] for c in coords[0]) / len(coords[0])
                    lat = sum(c[1] for c in coords[0]) / len(coords[0])
                
                voltage = props.get('voltage', 110)
                try:
                    voltage = int(str(voltage).split(';')[0].replace('kV', ''))
                    if voltage > 1000:
                        voltage //= 1000
                except:
                    voltage = 110
                
                self.substations.append({
                    'name': props.get('name', 'Unknown'),
                    'lat': lat,
                    'lon': lon,
                    'voltage': voltage,
                    'operator': props.get('operator', ''),
                })
        except Exception as e:
            print(f"Error loading substations: {e}")
        
        # Load wind turbines
        try:
            data = load_json('wind_turbines_enhanced.json')
            for t in data:
                if t.get('lat') and t.get('lon'):
                    self.wind_turbines.append({
                        'lat': t['lat'],
                        'lon': t['lon'],
                        'capacity_mw': t.get('estimated_mw', 3.0),
                        'name': t.get('name', 'Wind Turbine'),
                    })
        except Exception as e:
            print(f"Error loading wind turbines: {e}")
        
        # Load solar from power plants
        try:
            data = load_json('all_power_plants.json')
            for f in data.get('features', []):
                if f['properties'].get('source') == 'solar':
                    coords = f['geometry']['coordinates']
                    self.solar_plants.append({
                        'lat': coords[1],
                        'lon': coords[0],
                        'capacity_mw': f['properties'].get('capacity_mw', 0),
                    })
        except Exception as e:
            print(f"Error loading solar: {e}")
    
    def check_location(self, lat: float, lon: float) -> Dict:
        """Check feasibility of wind/solar installation at given location."""
        
        region = get_region(lat, lon)
        
        # Find nearest transformers with capacity
        nearby_transformers = []
        for t in self.transformers:
            dist = haversine_distance(lat, lon, t['lat'], t['lon'])
            if dist < 30:  # Within 30km
                nearby_transformers.append({
                    **t,
                    'distance_km': round(dist, 1),
                })
        
        nearby_transformers.sort(key=lambda x: x['distance_km'])
        
        # Find nearest HV substations (220kV+)
        nearby_hv = []
        for s in self.substations:
            if s['voltage'] >= 220:
                dist = haversine_distance(lat, lon, s['lat'], s['lon'])
                if dist < 50:
                    nearby_hv.append({
                        **s,
                        'distance_km': round(dist, 1),
                    })
        
        nearby_hv.sort(key=lambda x: x['distance_km'])
        
        # Count nearby installations
        wind_nearby = sum(1 for t in self.wind_turbines 
                        if haversine_distance(lat, lon, t['lat'], t['lon']) < 10)
        wind_capacity_nearby = sum(t['capacity_mw'] for t in self.wind_turbines 
                                  if haversine_distance(lat, lon, t['lat'], t['lon']) < 10)
        
        solar_nearby = sum(1 for s in self.solar_plants 
                         if haversine_distance(lat, lon, s['lat'], s['lon']) < 10)
        solar_capacity_nearby = sum((s['capacity_mw'] or 0) for s in self.solar_plants 
                                   if haversine_distance(lat, lon, s['lat'], s['lon']) < 10)
        
        # Calculate grid connection difficulty
        best_transformer = nearby_transformers[0] if nearby_transformers else None
        
        if best_transformer:
            if best_transformer['available_mw'] > 10 and best_transformer['distance_km'] < 5:
                connection_difficulty = 'easy'
                connection_color = '#00e676'
            elif best_transformer['available_mw'] > 5 and best_transformer['distance_km'] < 15:
                connection_difficulty = 'medium'
                connection_color = '#ffc107'
            elif best_transformer['available_mw'] > 0:
                connection_difficulty = 'challenging'
                connection_color = '#ff9800'
            else:
                connection_difficulty = 'difficult'
                connection_color = '#ff5252'
        else:
            connection_difficulty = 'unknown'
            connection_color = '#888'
        
        # Regional factors
        wind_cf = WIND_CAPACITY_FACTORS.get(region, 0.20)
        solar_cf = SOLAR_CAPACITY_FACTORS.get(region, 0.11)
        sunshine = SUNSHINE_HOURS.get(region, 1800)
        
        # Estimate annual production
        # For a typical 10 kW rooftop solar
        solar_10kw_annual_kwh = 10 * solar_cf * 8760
        # For a typical 3 MW wind turbine
        wind_3mw_annual_mwh = 3 * wind_cf * 8760
        
        return {
            'location': {
                'lat': lat,
                'lon': lon,
                'region': region,
            },
            'grid_connection': {
                'difficulty': connection_difficulty,
                'color': connection_color,
                'nearest_transformer': best_transformer,
                'nearby_transformers': nearby_transformers[:5],
                'nearby_hv_substations': nearby_hv[:3],
                'grid_operator': best_transformer['operator'] if best_transformer else 'Unknown',
            },
            'nearby_installations': {
                'wind_turbines': wind_nearby,
                'wind_capacity_mw': round(wind_capacity_nearby, 1),
                'solar_plants': solar_nearby,
                'solar_capacity_mw': round(solar_capacity_nearby, 1),
            },
            'regional_factors': {
                'wind_capacity_factor': wind_cf,
                'solar_capacity_factor': solar_cf,
                'sunshine_hours_year': sunshine,
            },
            'estimates': {
                'solar_10kw_annual_kwh': round(solar_10kw_annual_kwh),
                'solar_10kw_annual_eur': round(solar_10kw_annual_kwh * 0.08),  # ~8ct/kWh feed-in
                'wind_3mw_annual_mwh': round(wind_3mw_annual_mwh),
                'wind_3mw_annual_eur': round(wind_3mw_annual_mwh * 80),  # ~80€/MWh
            },
            'recommendations': self._get_recommendations(
                region, connection_difficulty, wind_cf, solar_cf, wind_nearby
            ),
        }
    
    def _get_recommendations(self, region, difficulty, wind_cf, solar_cf, wind_nearby):
        """Generate recommendations based on analysis."""
        recs = []
        
        # Solar recommendations
        if solar_cf >= 0.11:
            recs.append({
                'type': 'solar',
                'rating': 'good',
                'text': f'Gute Sonneneinstrahlung in {region} ({solar_cf*100:.0f}% Kapazitätsfaktor)',
            })
        
        # Wind recommendations
        if wind_cf >= 0.25:
            recs.append({
                'type': 'wind',
                'rating': 'excellent',
                'text': f'Ausgezeichnete Windverhältnisse ({wind_cf*100:.0f}% Kapazitätsfaktor)',
            })
        elif wind_cf >= 0.20:
            recs.append({
                'type': 'wind',
                'rating': 'good',
                'text': f'Gute Windverhältnisse ({wind_cf*100:.0f}% Kapazitätsfaktor)',
            })
        else:
            recs.append({
                'type': 'wind',
                'rating': 'moderate',
                'text': f'Mäßige Windverhältnisse ({wind_cf*100:.0f}% Kapazitätsfaktor)',
            })
        
        # Grid connection
        if difficulty == 'easy':
            recs.append({
                'type': 'grid',
                'rating': 'good',
                'text': 'Einfacher Netzanschluss möglich (nahe Kapazität verfügbar)',
            })
        elif difficulty == 'difficult':
            recs.append({
                'type': 'grid',
                'rating': 'warning',
                'text': 'Netzanschluss könnte schwierig sein - Kapazitätsengpass',
            })
        
        # Existing installations
        if wind_nearby > 5:
            recs.append({
                'type': 'info',
                'rating': 'info',
                'text': f'{wind_nearby} Windkraftanlagen im Umkreis von 10 km - etablierter Standort',
            })
        
        return recs
    
    def get_district_summary(self, district_name: str) -> Dict:
        """Get summary statistics for a district."""
        # This would need district boundary data to implement properly
        pass


def check_location_api(lat: float, lon: float) -> Dict:
    """API function to check a location."""
    checker = LocationChecker()
    return checker.check_location(lat, lon)


if __name__ == '__main__':
    import sys
    
    # Test with a location in Burgenland (good for wind)
    lat, lon = 47.85, 16.5
    if len(sys.argv) >= 3:
        lat, lon = float(sys.argv[1]), float(sys.argv[2])
    
    checker = LocationChecker()
    result = checker.check_location(lat, lon)
    
    print(f"\n{'='*60}")
    print(f"LOCATION CHECK: {lat}, {lon}")
    print(f"{'='*60}")
    
    print(f"\nRegion: {result['location']['region']}")
    print(f"\nGrid Connection: {result['grid_connection']['difficulty'].upper()}")
    
    if result['grid_connection']['nearest_transformer']:
        t = result['grid_connection']['nearest_transformer']
        print(f"  Nearest: {t['name']} ({t['distance_km']} km)")
        print(f"  Available: {t['available_mw']:.1f} MW")
        print(f"  Operator: {t['operator']}")
    
    print(f"\nNearby Installations (10 km radius):")
    print(f"  Wind: {result['nearby_installations']['wind_turbines']} turbines, "
          f"{result['nearby_installations']['wind_capacity_mw']} MW")
    print(f"  Solar: {result['nearby_installations']['solar_plants']} plants")
    
    print(f"\nRegional Factors:")
    print(f"  Wind capacity factor: {result['regional_factors']['wind_capacity_factor']*100:.0f}%")
    print(f"  Solar capacity factor: {result['regional_factors']['solar_capacity_factor']*100:.0f}%")
    print(f"  Sunshine hours/year: {result['regional_factors']['sunshine_hours_year']}")
    
    print(f"\nEstimated Annual Production:")
    print(f"  10 kW Solar: {result['estimates']['solar_10kw_annual_kwh']:,} kWh "
          f"(~{result['estimates']['solar_10kw_annual_eur']:,} €)")
    print(f"  3 MW Wind: {result['estimates']['wind_3mw_annual_mwh']:,} MWh "
          f"(~{result['estimates']['wind_3mw_annual_eur']:,} €)")
    
    print(f"\nRecommendations:")
    for rec in result['recommendations']:
        print(f"  [{rec['rating'].upper()}] {rec['text']}")
