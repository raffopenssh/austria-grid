# Cadastre Enrichment Status

## Overview
Enriching 4,727 power plants in Austria with cadastral/land parcel information.

## Process Details

### API Used
- **Endpoint**: `https://strassen-blockade-at.exe.xyz/api/bbox`
- **Layer**: parcels
- **Method**: Creates a small bounding box around each power plant and finds the parcel containing it

### Data Retrieved
For each power plant, we retrieve:
- `parcel_id`: Unique parcel identifier (e.g., "91005-765")
- `parcel_number`: Parcel number (gnr) (e.g., "765", "196/12")
- `cadastral_community`: KG code (e.g., "91005")
- `ez`: EZ number
- `area_sqm`: Parcel area in square meters
- `status`: Registration status (G/E)
- `parcel_lat/lon`: Parcel centroid coordinates

### Performance
- **Request rate**: ~3-5 seconds per power plant (0.2s delay + API response time)
- **Estimated total time**: ~4-6 hours for all 4,727 plants
- **Checkpoint saves**: Every 100 plants

### Monitoring
Run `./check_cadastre_progress.sh` to see current progress

### Background Execution
The enrichment runs in tmux session `cadastre_enrich`:
- View live: `tmux attach -t cadastre_enrich`
- Detach: `Ctrl+B`, then `D`

### Output
- **File**: `data/all_power_plants_with_cadastre.json`
- **Log**: `cadastre_enrichment.log`

### Next Steps
Once complete, we can:
1. Analyze which municipalities have the most power plants
2. Map power density by cadastral community
3. Cross-reference with land use data
4. Export for further GIS analysis
