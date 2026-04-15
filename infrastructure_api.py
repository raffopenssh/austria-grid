#!/usr/bin/env python3
"""Infrastructure GeoJSON API for Austria.

Explodes all infrastructure data into individual GeoJSON features,
filterable by bounding box. Designed for consumption by ML classifiers
working with ortho/CIR/LiDAR imagery.

Every grouped obstacle (e.g. a windpark with 3 turbines in one row)
is exploded into 3 individual Point features.

Data sources & deduplication:
- AustroControl obstacle database → individual points (antennas, wind turbines,
  cable car towers, pylons, cranes, stacks, buildings, etc.)
- Enhanced wind turbine data (AustroControl + igwindkraft.at capacity) → merged
  into obstacle wind turbine features (not a separate layer)
- Windpark registry (igwindkraft.at) → centroid-level park metadata with year,
  turbine model, capacity. Kept separate since it represents the park, not
  individual turbines.
- OSM power plants (solar, hydro, biomass, gas, coal, waste) → individual points.
  OSM wind plants are EXCLUDED (already covered by AustroControl turbines).
- OSM substations (Umspannwerke) → points
- Grid operator transformer stations → points, deduped against OSM substations
- 380kV grid network nodes → points, deduped against above
- 380kV grid network edges → LineStrings
- OSM transmission lines (110-380kV) → LineStrings
- ÖNIP powerline route points → points
- Hydropower plants (curated) → points
- Hydro-grid connection lines → LineStrings
- Cross-border interconnection lines → LineStrings
"""

import json
import math
import os
import re
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def _load(name):
    with open(os.path.join(DATA_DIR, name)) as f:
        return json.load(f)


def _in_bbox(lat, lon, bbox):
    """Check if point is inside bbox (min_lon, min_lat, max_lon, max_lat)."""
    if bbox is None:
        return True
    min_lon, min_lat, max_lon, max_lat = bbox
    try:
        flat, flon = _to_float(lat), _to_float(lon)
    except (ValueError, TypeError):
        return False
    return min_lat <= flat <= max_lat and min_lon <= flon <= max_lon


def _to_float(v):
    """Convert value to float, handling '47,123' comma decimals."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return float(v.replace(',', '.'))
    return float(v)


def _point_feature(lat, lon, properties):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [round(_to_float(lon), 7), round(_to_float(lat), 7)]},
        "properties": properties
    }


def _line_feature(coords, properties):
    """coords = list of [lon, lat]"""
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords},
        "properties": properties
    }


def _parse_bbox(bbox_str):
    """Parse 'min_lon,min_lat,max_lon,max_lat' string."""
    if not bbox_str:
        return None
    parts = [float(x.strip()) for x in bbox_str.split(',')]
    if len(parts) != 4:
        raise ValueError("bbox must be min_lon,min_lat,max_lon,max_lat")
    return tuple(parts)


def _line_bbox_intersects(coords, bbox):
    """Check if any [lon,lat] coord is in bbox, or bounding boxes overlap."""
    if bbox is None:
        return True
    min_lon, min_lat, max_lon, max_lat = bbox
    for c in coords:
        if min_lat <= c[1] <= max_lat and min_lon <= c[0] <= max_lon:
            return True
    lats = [c[1] for c in coords]
    lons = [c[0] for c in coords]
    if max(lats) < min_lat or min(lats) > max_lat:
        return False
    if max(lons) < min_lon or min(lons) > max_lon:
        return False
    return True


def _points_bbox_intersects(points_dicts, bbox):
    """Check if any {'lat','lon'} dict point is in or near bbox."""
    if bbox is None:
        return True
    min_lon, min_lat, max_lon, max_lat = bbox
    for p in points_dicts:
        if min_lat <= p['lat'] <= max_lat and min_lon <= p['lon'] <= max_lon:
            return True
    lats = [p['lat'] for p in points_dicts]
    lons = [p['lon'] for p in points_dicts]
    return not (max(lats) < min_lat or min(lats) > max_lat or
                max(lons) < min_lon or min(lons) > max_lon)


def _dedup_key(lat, lon, precision=4):
    """Create a spatial dedup key by rounding coords."""
    return (round(lat, precision), round(lon, precision))


# ---------------------------------------------------------------------------
# Build enhanced wind turbine lookup (AustroControl + igwindkraft capacity)
# ---------------------------------------------------------------------------

def _build_wind_enhancement_index():
    """Build lat/lon → enhanced data lookup from wind_turbines_enhanced.json."""
    data = _load('wind_turbines_enhanced.json')
    idx = {}
    for t in data:
        key = _dedup_key(t['lat'], t['lon'])
        idx[key] = {
            "estimated_capacity_mw": t.get('estimated_mw'),
            "display_name": t.get('display_name') or t.get('name'),
            "lighted": t.get('lighted'),
        }
    return idx


def _build_windpark_match_index():
    """Build comprehensive matching index for enriching obstacle turbines
    with windpark registry data.

    Strategy:
    1. Exact spatial match at ~100m grid (3-decimal rounding + neighbors)
    2. Name-based fuzzy match as fallback

    The windparks.json has one row per turbine, so each entry is an
    individual turbine position with park-level metadata.
    """
    data = _load('windparks.json')
    spatial = {}     # (lat_3, lon_3) → meta
    by_name = {}     # normalized name → [meta, ...]

    for wp in data:
        lat, lon = wp.get('lat'), wp.get('lon')
        info = wp.get('info', '')
        meta = {
            "total_capacity_mw": wp.get('total_mw'),
            "num_turbines": wp.get('turbines'),
            "mw_per_turbine": wp.get('mw_per_turbine'),
            "year_constructed": wp.get('year'),
            "hub_height_m": _extract_number(info, r'Nabenhöhe:\s*(\d+)'),
            "rotor_diameter_m": _extract_number(info, r'Rotordurchmesser:\s*(\d+)'),
            "turbine_model": (_extract_string(info, r'Type:\s*([^-]+)') or '').strip() or None,
        }

        # Spatial index at 3-decimal (~100m) precision with 1-cell neighbors
        if lat and lon:
            try:
                flat, flon = _to_float(lat), _to_float(lon)
                for dlat in (-0.001, 0, 0.001):
                    for dlon in (-0.001, 0, 0.001):
                        k = (round(flat + dlat, 3), round(flon + dlon, 3))
                        if k not in spatial:
                            spatial[k] = meta
            except (ValueError, TypeError):
                pass

        # Name index: collect all entries per name (pick best later)
        name = wp.get('name', '').lower().strip()
        if name:
            by_name[name] = meta  # last one wins (all same park metadata)

    return spatial, by_name


def _normalize_obstacle_name(location):
    """Extract a matchable park name from an obstacle location string.
    e.g. 'LO_ODS_001244 - Windpark Weiden am See' -> 'weiden am see'
    """
    if ' - ' not in location:
        return ''
    name = location.split(' - ', 1)[1].lower().strip()
    # Strip common prefixes
    for prefix in ('windpark ', 'wp ', 'erweiterung windpark ', 'windkraftanlage ',
                   'windkrafanlage ', 'tauernwindpark '):
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Strip common suffixes
    name = re.sub(r'\s*(repowering|erweiterung|teil \d+)\s*$', '', name).strip()
    return name


def _find_park_meta(lat, lon, location, spatial_idx, name_idx):
    """Try to match an obstacle turbine to windpark registry data."""
    # 1. Spatial match (most reliable)
    key = (round(lat, 3), round(lon, 3))
    meta = spatial_idx.get(key)
    if meta:
        return meta

    # 2. Name match with normalization
    obs_name = _normalize_obstacle_name(location)
    if not obs_name:
        return None

    # Exact
    meta = name_idx.get(obs_name)
    if meta:
        return meta

    # Substring match
    for park_name, pmeta in name_idx.items():
        if obs_name in park_name or park_name in obs_name:
            return pmeta

    # Split on hyphens/spaces and try first significant token (>3 chars)
    # e.g. "andau-halbturn" -> try "andau", "halbturn"
    tokens = re.split(r'[-/ ]+', obs_name)
    tokens = [t for t in tokens if len(t) > 3 and t not in (
        'teil', 'erweiterung', 'repowering', 'windpark')]
    # Strip trailing roman numerals / numbers
    tokens = [re.sub(r'\s*(i{1,4}|iv|v|vi{0,3}|\d+)\s*$', '', t).strip()
              for t in tokens]
    tokens = [t for t in tokens if t]

    for token in tokens:
        for park_name, pmeta in name_idx.items():
            if token in park_name:
                return pmeta

    return None


# ---------------------------------------------------------------------------
# Obstacle features (AustroControl) – the core layer
# ---------------------------------------------------------------------------

def _obstacle_category(type_en):
    mapping = {
        'Antenna': 'telecom',
        'Windmill farm': 'wind_energy',
        'Windpower plant': 'wind_energy',
        'Cable car': 'cable_infrastructure',
        'Transmission line': 'power_line',
        'Catenary': 'cable_infrastructure',
        'Pole': 'structure',
        'Building': 'structure',
        'Stack': 'industrial',
        'Mast': 'structure',
        'Crane': 'industrial',
        'Cooling tower': 'industrial',
        'Bridge': 'structure',
        'Tramway': 'cable_infrastructure',
        'Other': 'other',
        'Church': 'structure',
        'Tower': 'structure',
        'Dome': 'structure',
        'Flare stack': 'industrial',
        'Power plant': 'power_generation',
    }
    return mapping.get(type_en, 'other')


def _load_obstacle_features(bbox):
    """Explode obstacles into individual point + line features.

    - point/point_grouped/surface → one Point per coordinate row
    - curve/curve_grouped → one LineString per obstacle + one Point per tower
    Wind turbine points are enriched with capacity from igwindkraft.at data.
    """
    obstacles = _load('obstacles_all.json')
    wind_idx = _build_wind_enhancement_index()
    park_spatial, park_by_name = _build_windpark_match_index()
    features = []

    for obs in obstacles:
        type_en = obs.get('type_en', '')
        type_de = obs.get('type_de', '')
        is_wind = type_en in ('Windmill farm', 'Windpower plant')

        base_props = {
            "source": "austrocontrol_obstacles",
            "obstacle_id": obs['id'],
            "region": obs['region'],
            "district": obs['district'],
            "location": obs['location'],
            "type": type_en,
            "type_de": type_de,
            "category": _obstacle_category(type_en),
        }

        geom_type = obs['geometry_type']
        points = obs['points']

        if 'curve' in geom_type:
            non_span = [p for p in points if not p.get('is_span')]
            span_pts = [p for p in points if p.get('is_span')]

            if len(non_span) >= 2 and _points_bbox_intersects(non_span, bbox):
                coords = [[round(p['lon'], 7), round(p['lat'], 7)] for p in non_span]
                line_props = {
                    **base_props,
                    "geometry_class": "line",
                    "num_towers": len(non_span),
                    "max_elev_m": obs['max_elev_m'],
                    "max_height_agl_m": obs['max_height_agl_m'],
                    "horizontal_extent": obs.get('horizontal_extent'),
                }
                if span_pts:
                    line_props["span_height_agl_m"] = max(
                        (p['height_agl_m'] for p in span_pts if p.get('height_agl_m')),
                        default=None)
                features.append(_line_feature(coords, line_props))

            # Each tower / pylon as individual point
            for i, pt in enumerate(non_span):
                if not _in_bbox(pt['lat'], pt['lon'], bbox):
                    continue
                features.append(_point_feature(pt['lat'], pt['lon'], {
                    **base_props,
                    "geometry_class": "point",
                    "sub_type": "tower" if type_en in ('Cable car', 'Catenary', 'Tramway') else "pylon",
                    "point_index": i,
                    "total_points": len(non_span),
                    "parent_obstacle_id": obs['id'],
                    "elev_m": pt.get('elev_m'),
                    "height_agl_m": pt.get('height_agl_m'),
                    "day_marked": pt.get('day_marked'),
                    "lighted": pt.get('lighted'),
                }))
        else:
            # Point / point_grouped / surface → one feature per coordinate
            real_pts = [p for p in points if not p.get('is_span')]
            for i, pt in enumerate(real_pts):
                if not _in_bbox(pt['lat'], pt['lon'], bbox):
                    continue

                props = {
                    **base_props,
                    "geometry_class": "point",
                    "elev_m": pt.get('elev_m'),
                    "height_agl_m": pt.get('height_agl_m'),
                    "day_marked": pt.get('day_marked'),
                    "lighted": pt.get('lighted'),
                    "horizontal_extent": obs.get('horizontal_extent'),
                    "horizontal_radius_m": obs.get('horizontal_radius_m'),
                }

                if len(real_pts) > 1:
                    props["point_index"] = i
                    props["total_in_group"] = len(real_pts)
                    props["parent_obstacle_id"] = obs['id']
                    props["group_name"] = obs['location']

                # Enrich wind turbines with capacity/year from igwindkraft
                if is_wind:
                    key = _dedup_key(pt['lat'], pt['lon'])
                    enh = wind_idx.get(key, {})
                    if enh:
                        props["estimated_capacity_mw"] = enh.get('estimated_capacity_mw')
                        props["display_name"] = enh.get('display_name')
                    # Match to windpark registry (spatial + name)
                    park_meta = _find_park_meta(
                        pt['lat'], pt['lon'], obs.get('location', ''),
                        park_spatial, park_by_name)
                    if park_meta:
                        props["year_constructed"] = park_meta.get('year_constructed')
                        props["hub_height_m"] = park_meta.get('hub_height_m')
                        props["rotor_diameter_m"] = park_meta.get('rotor_diameter_m')
                        props["turbine_model"] = park_meta.get('turbine_model')
                        props["mw_per_turbine"] = park_meta.get('mw_per_turbine')
                        props["park_total_capacity_mw"] = park_meta.get('total_capacity_mw')
                        props["park_num_turbines"] = park_meta.get('num_turbines')
                        if park_meta.get('rotor_diameter_m'):
                            props["estimated_area_sqm"] = _estimate_windpark_area(
                                park_meta.get('num_turbines', 1),
                                park_meta['rotor_diameter_m'])

                features.append(_point_feature(pt['lat'], pt['lon'], props))

    return features


# ---------------------------------------------------------------------------
# Windpark registry (igwindkraft.at) – park-level centroid
# ---------------------------------------------------------------------------

def _load_windpark_features(bbox):
    """Windpark registry entries as centroid points.
    These represent the park as a whole; individual turbine points come from obstacles.
    """
    data = _load('windparks.json')
    features = []
    for wp in data:
        lat, lon = wp.get('lat'), wp.get('lon')
        if not lat or not lon or not _in_bbox(lat, lon, bbox):
            continue
        info = wp.get('info', '')
        hub_height = _extract_number(info, r'Nabenhöhe:\s*(\d+)')
        rotor_diameter = _extract_number(info, r'Rotordurchmesser:\s*(\d+)')
        turbine_type = (_extract_string(info, r'Type:\s*([^-]+)') or '').strip() or None

        features.append(_point_feature(lat, lon, {
            "source": "igwindkraft_registry",
            "name": wp.get('name'),
            "type": "windpark",
            "category": "wind_energy",
            "num_turbines": wp.get('turbines'),
            "mw_per_turbine": wp.get('mw_per_turbine'),
            "total_capacity_mw": wp.get('total_mw'),
            "year_constructed": wp.get('year'),
            "hub_height_m": hub_height,
            "rotor_diameter_m": rotor_diameter,
            "turbine_model": turbine_type,
            "estimated_area_sqm": _estimate_windpark_area(
                wp.get('turbines', 1), rotor_diameter),
        }))
    return features


# ---------------------------------------------------------------------------
# Power plants (OSM) – excluding wind (covered by obstacles)
# ---------------------------------------------------------------------------

def _load_power_plant_features(bbox):
    """OSM power plants. Wind excluded to avoid duplicating AustroControl turbines."""
    data = _load('all_power_plants_full_context.json')
    features = []
    for feat in data.get('features', []):
        props = feat.get('properties', {})
        source = props.get('source', '')
        if source == 'wind':  # covered by AustroControl obstacles
            continue
        coords = feat.get('geometry', {}).get('coordinates', [])
        if not coords or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        if not _in_bbox(lat, lon, bbox):
            continue
        cadastre = props.get('cadastre') or {}
        context = props.get('context') or {}
        features.append(_point_feature(lat, lon, {
            "source": "osm_power_plants",
            "osm_id": props.get('id'),
            "osm_type": props.get('osm_type'),
            "name": props.get('name'),
            "type": source,
            "category": _plant_category(source),
            "capacity_mw": props.get('capacity_mw'),
            "operator": props.get('operator') or None,
            "voltage": props.get('voltage') or None,
            "parcel_id": cadastre.get('parcel_id'),
            "parcel_area_sqm": cadastre.get('area_sqm'),
            "nearby_buildings": context.get('buildings', {}).get('count'),
            "nearby_roads": context.get('roads', {}).get('count'),
        }))
    return features


def _plant_category(source):
    return {
        'solar': 'solar_energy', 'hydro_run_of_river': 'hydropower',
        'biomass': 'biomass', 'gas': 'fossil', 'coal': 'fossil',
        'waste': 'waste_energy', 'other': 'other',
    }.get(source, 'other')


# ---------------------------------------------------------------------------
# Substations / Umspannwerke – deduplicated across 3 sources
# ---------------------------------------------------------------------------

def _load_substation_features(bbox):
    """Load substations from OSM, grid operators, and 380kV network.
    Dedup by proximity (~200m)."""
    features = []
    seen = set()

    def _add_if_new(lat, lon, props, precision=3):
        key = _dedup_key(lat, lon, precision)
        if key in seen:
            return
        seen.add(key)
        if _in_bbox(lat, lon, bbox):
            features.append(_point_feature(lat, lon, props))

    # 1. 380kV network nodes (highest quality)
    grid = _load('grid_network_380kv.json')
    for node in grid.get('nodes', []):
        lat, lon = node.get('lat'), node.get('lon')
        if not lat or not lon:
            continue
        _add_if_new(lat, lon, {
            "source": "grid_380kv",
            "name": node.get('name'),
            "type": "substation_380kv",
            "category": "substation",
            "voltage": node.get('voltage'),
            "operator": node.get('operator'),
        })

    # 2. OSM substations
    osm = _load('osm_substations.json')
    for feat in osm.get('features', []):
        coords = feat['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        p = feat['properties']
        _add_if_new(lat, lon, {
            "source": "osm_substations",
            "name": p.get('name') or None,
            "type": "substation",
            "category": "substation",
            "substation_type": p.get('substation'),
            "voltage": p.get('voltage'),
            "operator": p.get('operator') or None,
            "osm_id": p.get('osm_id'),
        })

    # 3. Grid operator transformer stations
    stations = _load('transformer_stations.json')
    for s in stations:
        lat, lon = s.get('latitude'), s.get('longitude')
        if not lat or not lon:
            continue
        booked = s.get('bookedCapacity')
        try:
            booked_mw = float(str(booked).replace(',', '.')) if booked else None
        except (ValueError, TypeError):
            booked_mw = None
        _add_if_new(lat, lon, {
            "source": "grid_operator_substations",
            "name": s.get('substationName'),
            "type": "transformer_station",
            "category": "substation",
            "state": s.get('state'),
            "network_operator": s.get('networkOperator'),
            "booked_capacity_mw": booked_mw,
            "available_capacity_mw": s.get('availableCapacity'),
        })

    return features


# ---------------------------------------------------------------------------
# Hydropower plants
# ---------------------------------------------------------------------------

def _load_hydro_features(bbox):
    data = _load('hydropower_plants.json')
    features = []
    for feat in data.get('features', []):
        coords = feat['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        if not _in_bbox(lat, lon, bbox):
            continue
        p = feat['properties']
        features.append(_point_feature(lat, lon, {
            "source": "hydropower_registry",
            "name": p.get('name'),
            "type": p.get('type'),  # Laufkraftwerk, Speicherkraftwerk, Pumpspeicherkraftwerk
            "category": "hydropower",
            "capacity_mw": p.get('mw'),
            "region": p.get('region'),
            "river": p.get('river'),
        }))
    return features


# ---------------------------------------------------------------------------
# Transmission / power lines (OSM + 380kV edges)
# ---------------------------------------------------------------------------

def _load_transmission_line_features(bbox):
    """OSM transmission lines + 380kV grid edges."""
    features = []

    # OSM transmission lines (110–380kV, 1291 lines)
    osm = _load('osm_transmission_lines.json')
    for feat in osm.get('features', []):
        coords = feat['geometry']['coordinates']
        if not _line_bbox_intersects(coords, bbox):
            continue
        p = feat['properties']
        features.append(_line_feature(coords, {
            "source": "osm_transmission_lines",
            "name": p.get('name'),
            "type": "transmission_line",
            "category": "power_line",
            "voltage_kv": p.get('voltage'),
            "voltage_raw": p.get('voltage_raw'),
            "operator": p.get('operator'),
            "cables": p.get('cables'),
            "circuits": p.get('circuits'),
            "ref": p.get('ref'),
            "osm_id": p.get('osm_id'),
        }))

    # 380kV grid network edges (460 edges, high-quality topology)
    grid = _load('grid_network_380kv.json')
    nodes = {n['id']: n for n in grid.get('nodes', [])}
    for edge in grid.get('edges', []):
        coords = edge.get('coordinates', [])
        if not coords:
            continue
        if not _line_bbox_intersects(coords, bbox):
            continue
        from_node = nodes.get(edge.get('from_node'), {})
        to_node = nodes.get(edge.get('to_node'), {})
        features.append(_line_feature(coords, {
            "source": "grid_380kv_edges",
            "name": edge.get('name'),
            "type": "transmission_line_380kv",
            "category": "power_line",
            "voltage_kv": edge.get('voltage'),
            "length_km": edge.get('length_km'),
            "from_substation": from_node.get('name'),
            "to_substation": to_node.get('name'),
        }))

    return features


# ---------------------------------------------------------------------------
# ÖNIP powerline route points
# ---------------------------------------------------------------------------

def _load_onip_features(bbox):
    data = _load('onip_powerlines_points.json')
    features = []
    for feat in data.get('features', []):
        coords = feat['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        if not _in_bbox(lat, lon, bbox):
            continue
        p = feat['properties']
        features.append(_point_feature(lat, lon, {
            "source": "onip_powerlines",
            "type": "powerline_route_point",
            "category": "power_line",
            "voltage_kv": p.get('voltage'),
        }))
    return features


# ---------------------------------------------------------------------------
# Cross-border interconnections
# ---------------------------------------------------------------------------

def _load_crossborder_features(bbox):
    data = _load('cross_border_connections.json')
    features = []
    for feat in data.get('features', []):
        coords = feat['geometry']['coordinates']
        if not _line_bbox_intersects(coords, bbox):
            continue
        p = feat['properties']
        features.append(_line_feature(coords, {
            "source": "cross_border",
            "name": p.get('name'),
            "type": "cross_border_interconnection",
            "category": "power_line",
            "from_country": p.get('from_country'),
            "to_country": p.get('to_country'),
            "voltage_kv": p.get('voltage'),
            "capacity_mw": p.get('capacity_mw'),
            "direction": p.get('direction'),
        }))
    return features


# ---------------------------------------------------------------------------
# Hydro-grid connection lines
# ---------------------------------------------------------------------------

def _load_hydro_connection_features(bbox):
    data = _load('hydro_grid_connections.json')
    features = []
    for feat in data.get('features', []):
        coords = feat['geometry']['coordinates']
        if not _line_bbox_intersects(coords, bbox):
            continue
        p = feat['properties']
        features.append(_line_feature(coords, {
            "source": "hydro_grid_connections",
            "type": "hydro_grid_connection",
            "category": "hydropower",
            "plant_mw": p.get('plant_mw'),
            "plant_type": p.get('plant_type'),
            "plant_region": p.get('plant_region'),
            "distance_km": p.get('distance_km'),
        }))
    return features


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_number(text, pattern):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None


def _extract_string(text, pattern):
    m = re.search(pattern, text)
    return m.group(1) if m else None


def _estimate_windpark_area(num_turbines, rotor_diameter):
    if not num_turbines or num_turbines < 1:
        return None
    rd = rotor_diameter or 90
    spacing = rd * 5
    if num_turbines == 1:
        return int(math.pi * (rd * 2) ** 2)
    side = math.ceil(math.sqrt(num_turbines))
    return int(side * spacing * side * spacing)


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------

AVAILABLE_LAYERS = {
    "obstacles":           "AustroControl obstacles – individual points for antennas, wind turbines, cable car towers, pylons, cranes, stacks, buildings, etc. Curves (cable cars, catenary, transmission lines) also emitted as LineStrings.",
    "windparks":           "Windpark registry (igwindkraft.at) – park-level centroid with year, turbine model, rotor diameter, hub height, capacity per turbine.",
    "power_plants":        "OSM power plants – solar, hydro, biomass, gas, coal, waste. Individual points with capacity, operator, cadastre, nearby context.",
    "substations":         "Substations / Umspannwerke – deduplicated across OSM, grid operators, 380kV network. Includes voltage, capacity, operator.",
    "hydropower":          "Major hydropower plants – curated list with MW, type (Laufkraftwerk/Speicher/Pumpspeicher), river.",
    "transmission_lines":  "Transmission lines – OSM (110-380kV) + 380kV grid edges. LineStrings with voltage, operator, length.",
    "onip_powerlines":     "ÖNIP powerline route points – planned/existing high-voltage route waypoints.",
    "cross_border":        "Cross-border interconnection lines – AT↔DE/CZ/SK/HU/SI/IT/CH with capacity.",
    "hydro_connections":   "Hydro-grid connection lines – inferred links between major hydro plants and substations.",
}

AVAILABLE_CATEGORIES = [
    "wind_energy", "solar_energy", "hydropower", "biomass", "fossil",
    "waste_energy", "substation", "telecom", "power_line",
    "cable_infrastructure", "structure", "industrial",
    "power_generation", "other",
]

_LAYER_LOADERS = {
    "obstacles":          _load_obstacle_features,
    "windparks":          _load_windpark_features,
    "power_plants":       _load_power_plant_features,
    "substations":        _load_substation_features,
    "hydropower":         _load_hydro_features,
    "transmission_lines": _load_transmission_line_features,
    "onip_powerlines":    _load_onip_features,
    "cross_border":       _load_crossborder_features,
    "hydro_connections":  _load_hydro_connection_features,
}


def get_infrastructure_geojson(bbox_str=None, layers=None, categories=None):
    """Return all infrastructure as a GeoJSON FeatureCollection.

    Args:
        bbox_str: "min_lon,min_lat,max_lon,max_lat" or None for all Austria
        layers: comma-separated layer names, or None for all
        categories: comma-separated category names, or None for all

    Returns:
        GeoJSON FeatureCollection dict
    """
    bbox = _parse_bbox(bbox_str)

    layer_set = None
    if layers:
        layer_set = {l.strip() for l in layers.split(',')}

    cat_set = None
    if categories:
        cat_set = {c.strip() for c in categories.split(',')}

    features = []
    for name, loader in _LAYER_LOADERS.items():
        if layer_set is not None and name not in layer_set:
            continue
        features.extend(loader(bbox))

    if cat_set:
        features = [f for f in features if f['properties'].get('category') in cat_set]

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total_features": len(features),
            "bbox": list(bbox) if bbox else None,
            "layers": list(layer_set) if layer_set else list(AVAILABLE_LAYERS.keys()),
            "categories_filter": list(cat_set) if cat_set else None,
        }
    }


def get_infrastructure_stats():
    """Return summary statistics about available infrastructure data."""
    result = get_infrastructure_geojson()
    from collections import Counter
    by_source = Counter()
    by_category = Counter()
    by_type = Counter()
    for f in result['features']:
        p = f['properties']
        by_source[p.get('source', 'unknown')] += 1
        by_category[p.get('category', 'unknown')] += 1
        by_type[p.get('type', 'unknown')] += 1

    return {
        "total_features": result['metadata']['total_features'],
        "by_source": dict(by_source.most_common()),
        "by_category": dict(by_category.most_common()),
        "by_type": dict(by_type.most_common(40)),
        "available_layers": AVAILABLE_LAYERS,
        "available_categories": AVAILABLE_CATEGORIES,
    }


if __name__ == '__main__':
    stats = get_infrastructure_stats()
    print(json.dumps(stats, indent=2, ensure_ascii=False))
