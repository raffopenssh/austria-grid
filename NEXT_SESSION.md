# Austria Grid - Next Session Instructions

## Current State (2026-01-30)

### Completed Features

1. **OSM Infrastructure Data**
   - 1,291 transmission line segments (460× 380kV, 831× 220kV)
   - 514 substations (85 HV from OSM + 429 from transformer data)
   - 41 nodes, 460 edges network topology

2. **Power Plant Data** ✅
   - 688 power plants with capacity data (from 4,700+ OSM elements)
   - Total: 26,825 MW capacity
   - By source:
     - Hydro run-of-river: 404 plants, 14,697 MW
     - Gas: 35 plants, 8,005 MW
     - Wind: 121 plants, 2,902 MW
     - Solar: 93 plants, 581 MW
     - Coal: 1 plant, 225 MW
     - Biomass: 24 plants, 113 MW
     - Waste: 6 plants, 277 MW

3. **ENTSO-E Live Data Integration** ✅
   - Real-time generation by source type
   - Day-ahead electricity prices (€/MWh)
   - Cross-border physical flows with DE, CZ, SK, HU, SI, IT, CH
   - SQLite database for historical storage
   - Cron job fetching every 15 minutes

4. **Substation Load Model** ✅
   - Estimates load on each substation based on:
     - Nearby power plants assigned to substations
     - Current production calculated from ENTSO-E utilization
     - Regional load distribution factors
     - Cross-border flows at border substations
   - Displays load percentage with color coding

### Map Layers
- ✅ District heatmap (capacity analysis)
- ✅ Wind turbines (1,578 individual turbines)
- ✅ Transformers (Umspannwerke)
- ✅ UW Lastanzeige (live substation load)
- ✅ Alle Kraftwerke (688 power plants with live production)
- ✅ Transmission lines (OSM 380/220kV)
- ✅ Wasserkraftwerke (156 major hydro)
- ✅ Cross-border connections
- ⚠️ ÖNIP 2030 (approximate georeferencing)

### API Endpoints
- `/api/entsoe/generation` - Live generation by type
- `/api/entsoe/prices` - Day-ahead prices
- `/api/entsoe/cross-border-flows` - Physical flows
- `/api/substation-loads` - Substation load estimates
- `/api/power-plants` - All plants with production

### Potential Improvements
1. **Price history chart**: 24h trend visualization
2. **Generation mix chart**: Real-time pie/donut chart
3. **Individual plant pages**: SEO pages for major plants
4. **Flow animation**: Animate power flow on map
5. **Forecasting**: Day-ahead generation forecast
6. **Grid operator districts**: Replace political Bezirke with Netzgebiete

### Key Commands
```bash
cd /home/exedev/austria-grid
sudo systemctl restart austria-grid
python3 fetch_power_plants.py  # Refresh OSM data
python3 entsoe_fetcher.py fetch 24  # Fetch 24h of data
```

### Live URLs
- App: https://austria-power.exe.xyz:8000/
- GitHub: https://github.com/raffopenssh/austria-grid

### ENTSO-E API
- Key: `35efd923-6969-4470-b2bd-0155b2254346`
- Austria bidding zone: `10YAT-APG------L`
