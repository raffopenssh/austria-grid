#!/usr/bin/env python3
"""Enrich power plant data with buildings, roads, hazard zones, and other contextual data."""

import json
import requests
import time
from datetime import datetime
from pathlib import Path
from collections import Counter

# Configuration
API_URL = "https://strassen-blockade-at.exe.xyz/api/bbox"
INPUT_FILE = "data/all_power_plants_with_cadastre.json"
OUTPUT_FILE = "data/all_power_plants_full_context.json"
LOG_FILE = "full_context_enrichment.log"
REQUEST_TIMEOUT = 60
RETRY_DELAY = 2
MAX_RETRIES = 3

# Search radius in degrees (approximately 100m at Austria's latitude)
SEARCH_RADIUS = 0.001

# Layers to fetch
LAYERS = {
    "buildings": {"limit": 20, "radius": 0.001},  # Nearby buildings
    "roads": {"limit": 10, "radius": 0.002},      # Nearby roads (larger radius)
    "hazard_zones": {"limit": 10, "radius": 0.005}, # Hazard zones (even larger)
    "hazard_events": {"limit": 10, "radius": 0.01}, # Historical events
    "landslide_events": {"limit": 10, "radius": 0.01},
}

def log(message):
    """Log message to both console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(LOG_FILE, "a") as f:
        f.write(log_message + "\n")

def fetch_layer_data(lon, lat, layer, limit, radius, retries=MAX_RETRIES):
    """Fetch data for a specific layer around the given coordinates."""
    for attempt in range(retries):
        try:
            minlon = lon - radius
            maxlon = lon + radius
            minlat = lat - radius
            maxlat = lat + radius
            
            response = requests.get(
                API_URL,
                params={
                    "layer": layer,
                    "minlon": minlon,
                    "minlat": minlat,
                    "maxlon": maxlon,
                    "maxlat": maxlat,
                    "limit": limit
                },
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("features", [])
            elif response.status_code == 404:
                return []
            else:
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
    
    return []

def summarize_features(features, layer):
    """Summarize features into useful statistics."""
    if not features:
        return None
    
    if layer == "buildings":
        building_types = Counter(f.get("properties", {}).get("building") for f in features)
        return {
            "count": len(features),
            "types": dict(building_types.most_common(5)),
            "has_names": sum(1 for f in features if f.get("properties", {}).get("name"))
        }
    
    elif layer == "roads":
        road_classes = Counter(f.get("properties", {}).get("road_class") for f in features)
        highway_types = Counter(f.get("properties", {}).get("highway") for f in features)
        return {
            "count": len(features),
            "road_classes": dict(road_classes),
            "highway_types": dict(highway_types.most_common(5)),
            "names": [f.get("properties", {}).get("name") for f in features if f.get("properties", {}).get("name")][:5]
        }
    
    elif layer == "hazard_zones":
        zone_types = Counter(f.get("properties", {}).get("zone") for f in features)
        return {
            "count": len(features),
            "zone_types": dict(zone_types),
            "in_hazard_zone": len(features) > 0
        }
    
    elif layer in ["hazard_events", "landslide_events"]:
        event_types = Counter(f.get("properties", {}).get("type") or f.get("properties", {}).get("event_type") for f in features)
        return {
            "count": len(features),
            "event_types": dict(event_types.most_common(5))
        }
    
    else:
        return {"count": len(features)}

def enrich_plant(feature, idx, total):
    """Enrich a single power plant with contextual data."""
    lon, lat = feature["geometry"]["coordinates"]
    plant_name = feature["properties"].get("name", "Unnamed")
    plant_id = feature["properties"]["id"]
    
    if idx % 100 == 0 or idx == 1:
        log(f"Processing {idx}/{total}: {plant_name} (ID: {plant_id})")
    
    context = {}
    
    for layer, config in LAYERS.items():
        features = fetch_layer_data(lon, lat, layer, config["limit"], config["radius"])
        summary = summarize_features(features, layer)
        if summary:
            context[layer] = summary
            if idx % 100 == 0:
                log(f"  {layer}: {summary.get('count', 0)} features")
        
        time.sleep(0.1)  # Small delay between layers
    
    return context

def main():
    log("Starting full context enrichment process...")
    log(f"Layers to fetch: {', '.join(LAYERS.keys())}")
    
    # Load power plants data
    log(f"Loading power plants from {INPUT_FILE}...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    
    total = len(data["features"])
    log(f"Found {total} power plants to enrich")
    
    # Process each power plant
    enriched_count = 0
    
    for idx, feature in enumerate(data["features"], 1):
        # Skip if already has context
        if "context" in feature["properties"]:
            enriched_count += 1
            continue
        
        context = enrich_plant(feature, idx, total)
        feature["properties"]["context"] = context
        enriched_count += 1
        
        # Save progress every 100 plants
        if idx % 100 == 0:
            log(f"Checkpoint: {idx}/{total} processed. Saving...")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(data, f, indent=2)
    
    # Final save
    log(f"Saving final results to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    # Summary
    log("\n" + "="*60)
    log("FULL CONTEXT ENRICHMENT COMPLETE")
    log("="*60)
    log(f"Total plants enriched: {enriched_count}")
    log(f"Output file: {OUTPUT_FILE}")
    
    # Analyze context data
    plants_with_buildings = sum(1 for f in data["features"] 
                                if f["properties"].get("context", {}).get("buildings"))
    plants_in_hazard_zones = sum(1 for f in data["features"] 
                                 if f["properties"].get("context", {}).get("hazard_zones"))
    
    log(f"Plants near buildings: {plants_with_buildings}")
    log(f"Plants in/near hazard zones: {plants_in_hazard_zones}")
    log("="*60)

if __name__ == "__main__":
    main()
