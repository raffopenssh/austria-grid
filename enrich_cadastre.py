#!/usr/bin/env python3
"""Enrich power plant data with cadastral/parcel information."""

import json
import requests
import time
from datetime import datetime
from pathlib import Path

# Configuration
CADASTRE_API_URL = "https://strassen-blockade-at.exe.xyz/api/bbox"
INPUT_FILE = "data/all_power_plants.json"
OUTPUT_FILE = "data/all_power_plants_with_cadastre.json"
LOG_FILE = "cadastre_enrichment.log"
REQUEST_TIMEOUT = 60  # 60 seconds timeout for the API
RETRY_DELAY = 2  # seconds between retries
MAX_RETRIES = 3
BBOX_DELTA = 0.001  # Small bounding box around the point (approximately 100m)

def log(message):
    """Log message to both console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, "a") as f:
        f.write(log_message + "\n")

def point_in_polygon(point_lon, point_lat, polygon_coords):
    """Check if a point is inside a polygon using ray casting algorithm."""
    # Handle MultiPolygon - take first polygon
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
            # Create a small bounding box around the point
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
                    # Find the parcel that contains this point
                    for feature in data["features"]:
                        geom = feature.get("geometry", {})
                        coords = geom.get("coordinates", [])
                        
                        if point_in_polygon(lon, lat, coords):
                            # Return only the relevant properties
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
                    # Point not in any parcel
                    return None
                else:
                    # No parcels in this area
                    return None
            elif response.status_code == 404:
                # No cadastre data for this location
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
    log("Starting cadastre enrichment process...")
    
    # Check if we have a checkpoint file to resume from
    if Path(OUTPUT_FILE).exists():
        log(f"Found checkpoint file {OUTPUT_FILE}, resuming...")
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
    else:
        # Load power plants data
        log(f"Loading power plants from {INPUT_FILE}...")
        with open(INPUT_FILE, "r") as f:
            data = json.load(f)
    
    total = len(data["features"])
    
    # Count how many are already processed
    already_processed = sum(1 for f in data["features"] if "cadastre" in f["properties"])
    if already_processed > 0:
        log(f"Already processed: {already_processed}/{total}")
        log(f"Remaining: {total - already_processed}")
    else:
        log(f"Found {total} power plants to process")
    
    # Process each power plant
    success_count = 0
    no_data_count = 0
    error_count = 0
    skipped_count = 0
    
    for idx, feature in enumerate(data["features"], 1):
        # Skip if already processed
        if "cadastre" in feature["properties"]:
            skipped_count += 1
            if feature["properties"]["cadastre"] is not None:
                success_count += 1
            else:
                no_data_count += 1
            continue
        
        lon, lat = feature["geometry"]["coordinates"]
        plant_name = feature["properties"].get("name", "Unnamed")
        plant_id = feature["properties"]["id"]
        
        if idx % 10 == 0 or idx == 1 or skipped_count == 0:
            log(f"Processing {idx}/{total}: {plant_name} (ID: {plant_id})")
        
        # Fetch cadastre info
        cadastre_info = get_cadastre_info(lon, lat)
        
        if cadastre_info:
            # Add cadastre info to the feature properties
            feature["properties"]["cadastre"] = cadastre_info
            success_count += 1
            
            # Log interesting finds
            if cadastre_info.get("parcel_number"):
                log(f"  ✓ Found parcel: {cadastre_info.get('parcel_number')} (ID: {cadastre_info.get('parcel_id')})")
        elif cadastre_info is None:
            no_data_count += 1
            feature["properties"]["cadastre"] = None
        else:
            error_count += 1
            feature["properties"]["cadastre"] = None
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.2)
        
        # Save progress every 100 plants
        if idx % 100 == 0:
            log(f"Progress checkpoint: {idx}/{total} processed. Saving...")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(data, f, indent=2)
    
    # Final save
    log(f"Saving final results to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    # Summary
    log("\n" + "="*60)
    log("ENRICHMENT COMPLETE")
    log("="*60)
    log(f"Total plants: {total}")
    log(f"Skipped (already processed): {skipped_count}")
    log(f"Successfully enriched: {success_count} ({success_count/total*100:.1f}%)")
    log(f"No cadastre data: {no_data_count} ({no_data_count/total*100:.1f}%)")
    log(f"Errors: {error_count} ({error_count/total*100:.1f}%)")
    log(f"Output file: {OUTPUT_FILE}")
    log("="*60)

if __name__ == "__main__":
    main()
