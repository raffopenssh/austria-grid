#!/usr/bin/env python3
"""Re-fetch cadastral data for plants that got null results."""

import json
import requests
import time
from datetime import datetime
from pathlib import Path

# Configuration
CADASTRE_API_URL = "https://strassen-blockade-at.exe.xyz/api/bbox"
INPUT_FILE = "data/all_power_plants_with_cadastre.json"
OUTPUT_FILE = "data/all_power_plants_with_cadastre.json"
LOG_FILE = "refetch_nulls.log"
REQUEST_TIMEOUT = 60
RETRY_DELAY = 2
MAX_RETRIES = 3
BBOX_DELTA = 0.001

def log(message):
    """Log message to both console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, "a") as f:
        f.write(log_message + "\n")

def point_in_polygon(point_lon, point_lat, polygon_coords):
    """Check if a point is inside a polygon using ray casting algorithm."""
    if polygon_coords and isinstance(polygon_coords[0][0][0], list):
        coords = polygon_coords[0][0]
    elif polygon_coords and isinstance(polygon_coords[0][0], (int, float)):
        coords = polygon_coords[0]
    else:
        return False
    
    inside = False
    n = len(coords)
    p1_lon, p1_lat = coords[0]
    
    for i in range(1, n + 1):
        p2_lon, p2_lat = coords[i % n]
        if point_lat > min(p1_lat, p2_lat):
            if point_lat <= max(p1_lat, p2_lat):
                if point_lon <= max(p1_lon, p2_lon):
                    if p1_lat != p2_lat:
                        x_intersect = (point_lat - p1_lat) * (p2_lon - p1_lon) / (p2_lat - p1_lat) + p1_lon
                    if p1_lon == p2_lon or point_lon <= x_intersect:
                        inside = not inside
        p1_lon, p1_lat = p2_lon, p2_lat
    
    return inside

def get_cadastre_info(lon, lat, retries=MAX_RETRIES):
    """Fetch cadastre info for given coordinates using bbox API."""
    for attempt in range(retries):
        try:
            minlon = lon - BBOX_DELTA
            maxlon = lon + BBOX_DELTA
            minlat = lat - BBOX_DELTA
            maxlat = lat + BBOX_DELTA
            
            response = requests.get(
                CADASTRE_API_URL,
                params={
                    "layer": "parcels",
                    "minlon": minlon,
                    "minlat": minlat,
                    "maxlon": maxlon,
                    "maxlat": maxlat,
                    "limit": 100
                },
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("features"):
                    for feature in data["features"]:
                        geom = feature.get("geometry", {})
                        coords = geom.get("coordinates", [])
                        
                        if point_in_polygon(lon, lat, coords):
                            props = feature.get("properties", {})
                            return {
                                "parcel_id": props.get("parcel_id"),
                                "parcel_number": props.get("gnr"),
                                "cadastral_community": props.get("kg"),
                                "ez": props.get("ez"),
                                "area_sqm": props.get("area_sqm"),
                                "status": props.get("rstatus"),
                                "parcel_lat": props.get("lat"),
                                "parcel_lon": props.get("lon")
                            }
                    return None
                else:
                    return None
            elif response.status_code == 404:
                return None
            else:
                log(f"  API returned status {response.status_code}, attempt {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY)
        except requests.exceptions.Timeout:
            log(f"  Timeout on attempt {attempt + 1}/{retries}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            log(f"  Error on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
    
    return None

def main():
    log("Starting null value re-fetch process...")
    
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    
    # Find all features with null cadastre
    null_indices = []
    for idx, feature in enumerate(data["features"]):
        if feature["properties"].get("cadastre") is None:
            null_indices.append(idx)
    
    total_nulls = len(null_indices)
    log(f"Found {total_nulls} plants with null cadastre data")
    
    if total_nulls == 0:
        log("Nothing to re-fetch!")
        return
    
    success_count = 0
    still_null_count = 0
    
    for i, idx in enumerate(null_indices, 1):
        feature = data["features"][idx]
        lon, lat = feature["geometry"]["coordinates"]
        plant_name = feature["properties"].get("name", "Unnamed")
        plant_id = feature["properties"]["id"]
        
        if i % 10 == 0 or i == 1:
            log(f"Re-fetching {i}/{total_nulls}: {plant_name} (ID: {plant_id})")
        
        cadastre_info = get_cadastre_info(lon, lat)
        
        if cadastre_info:
            feature["properties"]["cadastre"] = cadastre_info
            success_count += 1
            log(f"  ✓ Found parcel: {cadastre_info.get('parcel_number')} (ID: {cadastre_info.get('parcel_id')})")
        else:
            still_null_count += 1
        
        time.sleep(0.2)
        
        # Save progress every 50 plants
        if i % 50 == 0:
            log(f"Checkpoint: {i}/{total_nulls} processed. Saving...")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(data, f, indent=2)
    
    # Final save
    log(f"Saving final results to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    # Summary
    log("\n" + "="*60)
    log("RE-FETCH COMPLETE")
    log("="*60)
    log(f"Total nulls re-fetched: {total_nulls}")
    log(f"Successfully found: {success_count} ({success_count/total_nulls*100:.1f}%)")
    log(f"Still null: {still_null_count} ({still_null_count/total_nulls*100:.1f}%)")
    log("="*60)

if __name__ == "__main__":
    main()
