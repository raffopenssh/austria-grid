#!/usr/bin/env python3
"""Shared district capacity computation, used by the JSON API and the SEO pages."""

import json
import os
import time

from shapely.geometry import shape, Point

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

_cache = {'stats': None, 'ts': 0}
CACHE_TTL = 3600  # seconds


def _load(filename):
    with open(os.path.join(DATA_DIR, filename), 'r') as f:
        return json.load(f)


def compute_district_stats():
    """Capacity analysis per district (point-in-polygon). Cached for 1h."""
    now = time.time()
    if _cache['stats'] is not None and now - _cache['ts'] < CACHE_TTL:
        return _cache['stats']

    windparks = _load('windparks.json')
    transformers = _load('transformer_stations.json')
    bezirke = _load('bezirke.json')

    district_stats = {}
    assigned_windparks = set()   # row indices (one row per turbine in source data)
    assigned_park_ids = set()    # unique park ids, to avoid multi-counting park totals
    assigned_transformers = set()

    for feature in bezirke['features']:
        name = feature['properties']['name']
        iso = feature['properties']['iso']
        try:
            district_shape = shape(feature['geometry'])
        except Exception:
            continue

        min_lon, min_lat, max_lon, max_lat = district_shape.bounds

        district_windparks = []
        for i, wp in enumerate(windparks):
            if i in assigned_windparks:
                continue
            try:
                wp_lon = float(wp.get('lon', 0) or 0)
                wp_lat = float(wp.get('lat', 0) or 0)
                if not (min_lon <= wp_lon <= max_lon and min_lat <= wp_lat <= max_lat):
                    continue
                if district_shape.contains(Point(wp_lon, wp_lat)):
                    assigned_windparks.add(i)
                    # Source data has one row per turbine, each carrying the
                    # park's totals. Count each park only once (in the district
                    # of its first matched turbine).
                    pid = wp.get('id', wp.get('name'))
                    if pid not in assigned_park_ids:
                        assigned_park_ids.add(pid)
                        district_windparks.append(wp)
            except (ValueError, TypeError):
                continue

        district_transformers = []
        for i, t in enumerate(transformers):
            if i in assigned_transformers:
                continue
            try:
                t_lon = float(t.get('longitude', 0) or 0)
                t_lat = float(t.get('latitude', 0) or 0)
                if not (min_lon <= t_lon <= max_lon and min_lat <= t_lat <= max_lat):
                    continue
                if district_shape.contains(Point(t_lon, t_lat)):
                    district_transformers.append({**t, '_idx': i})
                    assigned_transformers.add(i)
            except (ValueError, TypeError):
                continue

        total_installed_mw = sum(float(wp.get('total_mw', 0) or 0) for wp in district_windparks)
        total_turbines = sum(int(wp.get('turbines', 0) or 0) for wp in district_windparks)

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

        total_grid_capacity = total_booked + total_available
        if total_grid_capacity > 0:
            utilization = min(total_installed_mw / (total_grid_capacity + 0.01), 1.5)
            capacity_score = max(0, min(100, (1 - utilization * 0.7) * 100))
        elif total_installed_mw > 0:
            capacity_score = 20
        else:
            capacity_score = 50

        estimated_actual_available = total_available * 1.4 + (total_booked * 0.15)

        district_stats[iso] = {
            'name': name,
            'iso': iso,
            'windparks': len(district_windparks),
            'windpark_list': [
                {'name': wp.get('name'), 'turbines': wp.get('turbines'),
                 'total_mw': wp.get('total_mw'), 'year': wp.get('year')}
                for wp in district_windparks
            ],
            'transformer_list': [
                {'idx': t['_idx'], 'name': t.get('substationName'),
                 'operator': t.get('networkOperator'),
                 'available_mw': t.get('availableCapacity'),
                 'booked_mw': t.get('bookedCapacity')}
                for t in district_transformers
            ],
            'turbines': total_turbines,
            'installed_mw': round(total_installed_mw, 2),
            'transformers': len(district_transformers),
            'booked_capacity_mw': round(total_booked, 2),
            'official_available_mw': round(total_available, 2),
            'estimated_available_mw': round(estimated_actual_available, 2),
            'capacity_score': round(capacity_score, 1),
            'bbox': [min_lon, min_lat, max_lon, max_lat],
        }

    _cache['stats'] = district_stats
    _cache['ts'] = now
    return district_stats


def api_district_stats():
    """Stats without internal list fields (backwards-compatible API shape)."""
    full = compute_district_stats()
    out = {}
    for iso, s in full.items():
        out[iso] = {k: v for k, v in s.items() if k not in ('windpark_list', 'transformer_list')}
    return out
