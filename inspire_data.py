#!/usr/bin/env python3
"""INSPIRE geodata downloader and spatial checker for Austrian protected areas,
wind exclusion zones, and Natura 2000 sites.

Data sources:
- Schutzgebiete Österreich 2025 (protected areas, nationwide)
- Windkraft Ausschlusszone OÖ (wind exclusion zones, Upper Austria)
- Natura 2000 OÖ (Natura 2000 sites, Upper Austria)

All data is cached as GeoJSON in data/inspire/ and re-downloaded if older than 30 days.
Uses shapely STRtree spatial index for efficient point-in-polygon checks.
"""

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from shapely import STRtree
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "data" / "inspire"
CACHE_MAX_AGE_DAYS = 30
BATCH_SIZE = 1000
REQUEST_TIMEOUT = 120  # seconds per HTTP request
PROXIMITY_BUFFER_M = 500  # metres – "within 500 m" threshold

# WFS endpoints and layer configs
WFS_SOURCES = {
    "protected_areas": {
        "url": "https://haleconnect.com/ows/services/org.711.16e83536-6c1f-4ca3-bfac-98bdd3e22ff8_wfs",
        "typename": "ps:ProtectedSite",
        "version": "2.0.0",
        "output_format": "application/json",
        "srs": "EPSG:4326",
        "cache_file": "protected_areas.geojson",
        "expected_count": 4686,
        "count_param": "count",       # WFS 2.0 uses 'count'
        "start_param": "startIndex",
    },
    "wind_exclusion": {
        "url": "https://ags.doris.at/arcgis/services/HVD/MapServer/WFSServer",
        "typename": "HVD:Windkraft_Ausschlusszone",
        "version": "2.0.0",
        "output_format": "GEOJSON",
        "srs": "EPSG:4326",
        "cache_file": "wind_exclusion_ooe.geojson",
        "expected_count": None,
        "count_param": "count",
        "start_param": "startIndex",
        "swap_coords": True,  # DORIS WFS returns [lat,lon] instead of [lon,lat]
    },
    "natura2000": {
        "url": "https://ags.doris.at/arcgis/services/HVD/MapServer/WFSServer",
        "typename": "HVD:Europaschutzgebiete_Natura2000",
        "version": "2.0.0",
        "output_format": "GEOJSON",
        "srs": "EPSG:4326",
        "cache_file": "natura2000_ooe.geojson",
        "expected_count": None,
        "count_param": "count",
        "start_param": "startIndex",
        "swap_coords": True,  # DORIS WFS returns [lat,lon] instead of [lon,lat]
    },
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: int = REQUEST_TIMEOUT) -> bytes:
    """Fetch URL with a simple retry."""
    req = urllib.request.Request(url, headers={"User-Agent": "austria-grid-inspire/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as exc:
        logger.warning("HTTP request failed (%s), retrying once: %s", exc, url[:120])
        time.sleep(2)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()


# ---------------------------------------------------------------------------
# WFS download with pagination
# ---------------------------------------------------------------------------

def _build_wfs_url(cfg: dict, start_index: int = 0) -> str:
    params = {
        "service": "WFS",
        "version": cfg["version"],
        "request": "GetFeature",
        "typeNames": cfg["typename"],
        "outputFormat": cfg["output_format"],
        "srsName": cfg["srs"],
        cfg["count_param"]: str(BATCH_SIZE),
        cfg["start_param"]: str(start_index),
    }
    return cfg["url"] + "?" + urllib.parse.urlencode(params)


def _download_wfs(key: str) -> dict:
    """Download all features from a WFS endpoint using pagination.
    Returns a GeoJSON FeatureCollection dict.
    """
    cfg = WFS_SOURCES[key]
    all_features: list[dict] = []
    start_index = 0
    empty_retries = 0

    logger.info("Downloading %s from %s ...", cfg["typename"], cfg["url"][:60])

    while True:
        url = _build_wfs_url(cfg, start_index)
        logger.debug("  fetching startIndex=%d", start_index)

        try:
            raw = _http_get(url)
        except Exception as exc:
            logger.error("Failed to download %s at offset %d: %s", key, start_index, exc)
            break

        # Parse JSON
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # haleconnect sometimes returns XML error; try to detect
            text_snippet = raw[:500].decode("utf-8", errors="replace")
            if "ExceptionReport" in text_snippet or "<" in text_snippet[:5]:
                logger.error("WFS returned XML error for %s: %s", key, text_snippet[:200])
            else:
                logger.error("Invalid JSON from %s at offset %d", key, start_index)
            break

        features = data.get("features", [])

        if not features:
            # haleconnect can return empty on first try – retry once
            if empty_retries < 1 and start_index == 0:
                logger.warning("Empty response for %s, retrying ...", key)
                empty_retries += 1
                time.sleep(3)
                continue
            logger.info("  no more features at offset %d – done.", start_index)
            break

        all_features.extend(features)
        logger.info("  got %d features (total so far: %d)", len(features), len(all_features))

        if len(features) < BATCH_SIZE:
            # Last page
            break

        start_index += len(features)
        time.sleep(0.5)  # be polite

    logger.info("Downloaded %d features for %s", len(all_features), key)

    return {
        "type": "FeatureCollection",
        "features": all_features,
        "_download_ts": datetime.utcnow().isoformat(),
        "_source": cfg["url"],
        "_typename": cfg["typename"],
    }


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

def _cache_path(key: str) -> Path:
    return CACHE_DIR / WFS_SOURCES[key]["cache_file"]


def _cache_is_fresh(key: str) -> bool:
    p = _cache_path(key)
    if not p.exists():
        return False
    mtime = datetime.fromtimestamp(p.stat().st_mtime)
    return (datetime.now() - mtime) < timedelta(days=CACHE_MAX_AGE_DAYS)


def _load_cache(key: str) -> Optional[dict]:
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Cache read failed for %s: %s", key, exc)
        return None


def _save_cache(key: str, data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(key)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info("Cached %s → %s (%.1f MB)", key, p, p.stat().st_size / 1e6)


def _ensure_data(key: str) -> dict:
    """Return cached GeoJSON or download fresh."""
    if _cache_is_fresh(key):
        data = _load_cache(key)
        if data and data.get("features"):
            logger.debug("Using cached %s (%d features)", key, len(data["features"]))
            return data

    # Download
    try:
        data = _download_wfs(key)
    except Exception as exc:
        logger.error("Download failed for %s: %s", key, exc)
        # Fallback to stale cache
        data = _load_cache(key)
        if data:
            logger.warning("Using stale cache for %s", key)
            return data
        return {"type": "FeatureCollection", "features": []}

    if data.get("features"):
        _save_cache(key, data)
    else:
        # If download returned nothing, try stale cache
        stale = _load_cache(key)
        if stale and stale.get("features"):
            logger.warning("Download empty for %s, using stale cache", key)
            return stale

    return data


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _extract_name(feature: dict) -> str:
    """Best-effort name extraction from various property schemas."""
    props = feature.get("properties", {})
    # INSPIRE ProtectedSite
    for field in ("text", "name", "siteName", "BEZEICHNUNG", "NAME",
                  "Bezeichnung", "Name", "OBJECTID"):
        val = props.get(field)
        if val and str(val).strip():
            return str(val).strip()
    # Fall back to localId / gml_id
    for field in ("localId", "gml_id", "identifier"):
        val = props.get(field)
        if val:
            return str(val)
    return "unknown"


def _extract_type(feature: dict) -> str:
    """Extract type/designation from feature properties."""
    props = feature.get("properties", {})
    for field in ("designationScheme", "siteDesignation", "type", "TYP",
                  "Typ", "SCHUTZGEBIETSTYP", "Schutzgebietstyp"):
        val = props.get(field)
        if val and str(val).strip():
            return str(val).strip()
    return "protected_area"


def _swap_coords(coords):
    """Recursively swap [lat,lon] -> [lon,lat] in coordinate arrays."""
    if isinstance(coords, (list, tuple)):
        if len(coords) >= 2 and isinstance(coords[0], (int, float)):
            # It's a coordinate pair: swap
            return [coords[1], coords[0]] + list(coords[2:])
        else:
            return [_swap_coords(c) for c in coords]
    return coords


def _safe_shape(geom_dict: dict, swap: bool = False):
    """Convert GeoJSON geometry to shapely, handling nulls.
    
    Args:
        swap: If True, swap [lat,lon] -> [lon,lat] (for DORIS WFS)
    """
    if not geom_dict:
        return None
    try:
        if swap:
            geom_dict = dict(geom_dict)
            geom_dict["coordinates"] = _swap_coords(geom_dict["coordinates"])
        geom = shape(geom_dict)
        if geom.is_empty:
            return None
        if not geom.is_valid:
            geom = geom.buffer(0)
        return geom
    except Exception as exc:
        logger.debug("Could not parse geometry: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Spatial index (lazy, module-level singletons)
# ---------------------------------------------------------------------------

class _SpatialLayer:
    """Holds geometries + metadata with an STRtree index."""

    def __init__(self):
        self.geoms: list = []       # shapely geometries
        self.meta: list[dict] = []  # parallel metadata list
        self.tree: Optional[STRtree] = None

    def build(self, features: list[dict], name_fn=_extract_name, type_fn=_extract_type, swap_coords: bool = False):
        geoms = []
        meta = []
        for feat in features:
            geom = _safe_shape(feat.get("geometry"), swap=swap_coords)
            if geom is None:
                continue
            # Explode MultiPolygons into individual polygons for better spatial indexing
            if geom.geom_type == 'MultiPolygon':
                for sub_geom in geom.geoms:
                    if sub_geom.is_empty:
                        continue
                    geoms.append(sub_geom)
                    meta.append({
                        "name": name_fn(feat),
                        "type": type_fn(feat),
                        "properties": feat.get("properties", {}),
                    })
            else:
                geoms.append(geom)
                meta.append({
                    "name": name_fn(feat),
                    "type": type_fn(feat),
                    "properties": feat.get("properties", {}),
                })
        self.geoms = geoms
        self.meta = meta
        if geoms:
            self.tree = STRtree(geoms)
        logger.info("Built spatial index with %d geometries", len(geoms))

    def query_point(self, point: Point, buffer_m: float = 0):
        """Return list of (index, distance_m) for geometries that contain
        or are within buffer_m metres of the point.

        Uses a rough degree→metre conversion at Austrian latitudes (~47°N):
          1° lat ≈ 111 320 m,  1° lon ≈ 111 320 × cos(47°) ≈ 75 900 m.
        We convert buffer_m to a degree-based envelope for the tree query,
        then compute proper approximate distances.
        """
        if not self.tree or not self.geoms:
            return []

        # Approximate conversion at ~47° N
        lat_deg_per_m = 1.0 / 111_320.0
        lon_deg_per_m = 1.0 / 75_900.0

        results = []

        if buffer_m > 0:
            # Create a rectangular buffer in degrees
            buf_lat = buffer_m * lat_deg_per_m
            buf_lon = buffer_m * lon_deg_per_m
            search_geom = point.buffer(1)  # unit circle
            # Scale to an ellipse approximating buffer_m in both axes
            from shapely.affinity import scale
            search_geom = scale(point.buffer(buf_lat), xfact=buf_lon / buf_lat, yfact=1.0)
        else:
            search_geom = point

        idxs = self.tree.query(search_geom)
        for idx in idxs:
            geom = self.geoms[idx]
            if geom.contains(point):
                results.append((idx, 0.0))
            elif buffer_m > 0:
                # Compute approximate distance in metres using nearest_points
                from shapely.ops import nearest_points
                p1, p2 = nearest_points(point, geom)
                dlat = abs(p1.y - p2.y) * 111_320
                dlon = abs(p1.x - p2.x) * 75_900
                dist_m = (dlat**2 + dlon**2) ** 0.5

                if dist_m <= buffer_m:
                    results.append((idx, dist_m))

        return results


# Module-level singletons (lazy-loaded)
_protected_layer: Optional[_SpatialLayer] = None
_wind_excl_layer: Optional[_SpatialLayer] = None
_natura2000_layer: Optional[_SpatialLayer] = None


def _get_protected_layer() -> _SpatialLayer:
    global _protected_layer
    if _protected_layer is None:
        _protected_layer = _SpatialLayer()
        data = _ensure_data("protected_areas")
        _protected_layer.build(data.get("features", []))
    return _protected_layer


def _get_wind_excl_layer() -> _SpatialLayer:
    global _wind_excl_layer
    if _wind_excl_layer is None:
        _wind_excl_layer = _SpatialLayer()
        data = _ensure_data("wind_exclusion")
        _wind_excl_layer.build(data.get("features", []), swap_coords=True)
    return _wind_excl_layer


def _get_natura2000_layer() -> _SpatialLayer:
    global _natura2000_layer
    if _natura2000_layer is None:
        _natura2000_layer = _SpatialLayer()
        data = _ensure_data("natura2000")
        _natura2000_layer.build(data.get("features", []), swap_coords=True)
    return _natura2000_layer


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_protected_areas() -> list[dict]:
    """Load all Austrian protected areas.

    Returns list of dicts:
        {
            "name": str,
            "type": str,
            "geometry": shapely geometry,
            "properties": dict (raw WFS properties),
        }
    """
    layer = _get_protected_layer()
    return [
        {
            "name": layer.meta[i]["name"],
            "type": layer.meta[i]["type"],
            "geometry": layer.geoms[i],
            "properties": layer.meta[i]["properties"],
        }
        for i in range(len(layer.geoms))
    ]


def load_wind_exclusion_zones() -> list[dict]:
    """Load Upper Austria wind exclusion zones.

    Returns list of dicts:
        {
            "name": str,
            "type": str,
            "geometry": shapely geometry,
            "properties": dict,
        }
    """
    layer = _get_wind_excl_layer()
    return [
        {
            "name": layer.meta[i]["name"],
            "type": layer.meta[i]["type"],
            "geometry": layer.geoms[i],
            "properties": layer.meta[i]["properties"],
        }
        for i in range(len(layer.geoms))
    ]


def load_natura2000() -> list[dict]:
    """Load Upper Austria Natura 2000 sites."""
    layer = _get_natura2000_layer()
    return [
        {
            "name": layer.meta[i]["name"],
            "type": layer.meta[i]["type"],
            "geometry": layer.geoms[i],
            "properties": layer.meta[i]["properties"],
        }
        for i in range(len(layer.geoms))
    ]


def check_point_in_zones(lat: float, lon: float) -> dict:
    """Check whether a point falls inside protected areas, wind exclusion
    zones, or Natura 2000 sites.

    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)

    Returns:
        {
            "protected_area": None or {"name": str, "type": str, "distance_m": float},
            "wind_exclusion": True/False  (only meaningful for Upper Austria),
            "natura2000": None or {"name": str, "distance_m": float},
        }
    """
    point = Point(lon, lat)  # GeoJSON order: (lon, lat)
    result = {
        "protected_area": None,
        "wind_exclusion": False,
        "natura2000": None,
    }

    # --- Protected areas (nationwide, 500 m buffer) ---
    try:
        pa_layer = _get_protected_layer()
        hits = pa_layer.query_point(point, buffer_m=PROXIMITY_BUFFER_M)
        if hits:
            # Pick closest
            best_idx, best_dist = min(hits, key=lambda x: x[1])
            result["protected_area"] = {
                "name": pa_layer.meta[best_idx]["name"],
                "type": pa_layer.meta[best_idx]["type"],
                "distance_m": round(best_dist, 1),
            }
    except Exception as exc:
        logger.error("Protected areas check failed: %s", exc)

    # --- Wind exclusion zones (OÖ only) ---
    try:
        we_layer = _get_wind_excl_layer()
        hits = we_layer.query_point(point, buffer_m=0)
        result["wind_exclusion"] = len(hits) > 0
    except Exception as exc:
        logger.error("Wind exclusion check failed: %s", exc)

    # --- Natura 2000 (OÖ, 500 m buffer) ---
    try:
        n2k_layer = _get_natura2000_layer()
        hits = n2k_layer.query_point(point, buffer_m=PROXIMITY_BUFFER_M)
        if hits:
            best_idx, best_dist = min(hits, key=lambda x: x[1])
            result["natura2000"] = {
                "name": n2k_layer.meta[best_idx]["name"],
                "distance_m": round(best_dist, 1),
            }
    except Exception as exc:
        logger.error("Natura 2000 check failed: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Cache management utilities
# ---------------------------------------------------------------------------

def refresh_all_caches(force: bool = False) -> dict:
    """Download / refresh all datasets.

    Args:
        force: If True, re-download even if cache is fresh.

    Returns:
        dict with dataset names and feature counts.
    """
    global _protected_layer, _wind_excl_layer, _natura2000_layer
    # Invalidate in-memory caches
    _protected_layer = None
    _wind_excl_layer = None
    _natura2000_layer = None

    counts = {}
    for key in WFS_SOURCES:
        if force:
            # Remove cached file to force re-download
            p = _cache_path(key)
            if p.exists():
                p.unlink()
        data = _ensure_data(key)
        n = len(data.get("features", []))
        counts[key] = n
        logger.info("%s: %d features", key, n)
    return counts


def cache_status() -> dict:
    """Return status of each cache file."""
    status = {}
    for key in WFS_SOURCES:
        p = _cache_path(key)
        if p.exists():
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            age_days = (datetime.now() - mtime).total_seconds() / 86400
            try:
                with open(p, "r") as f:
                    data = json.load(f)
                n_features = len(data.get("features", []))
            except Exception:
                n_features = -1
            status[key] = {
                "path": str(p),
                "size_mb": round(p.stat().st_size / 1e6, 2),
                "age_days": round(age_days, 1),
                "fresh": age_days < CACHE_MAX_AGE_DAYS,
                "features": n_features,
            }
        else:
            status[key] = {"path": str(p), "exists": False}
    return status


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="INSPIRE geodata manager")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("download", help="Download / refresh all datasets")
    sub.add_parser("status", help="Show cache status")

    p_check = sub.add_parser("check", help="Check a lat/lon point")
    p_check.add_argument("lat", type=float)
    p_check.add_argument("lon", type=float)

    args = parser.parse_args()

    if args.cmd == "download":
        counts = refresh_all_caches(force="--force" in sys.argv)
        for k, v in counts.items():
            print(f"  {k}: {v} features")

    elif args.cmd == "status":
        for k, v in cache_status().items():
            print(f"  {k}: {v}")

    elif args.cmd == "check":
        result = check_point_in_zones(args.lat, args.lon)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    else:
        parser.print_help()
