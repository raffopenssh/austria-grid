# Austria Grid - Next Session Instructions

## Current State (2026-01-30)

### Completed
1. **OSM Transmission Lines**: 1,291 segments (460× 380kV, 831× 220kV)
2. **OSM Substations**: 85 high-voltage substations
3. **Grid Network Topology**: 41 nodes, 460 edges with connectivity
4. **Hydropower**: 156 plants (13.5 GW)
5. **Cross-border Connections**: 9 interconnectors
6. **E-Control Statistics**: Grid lengths by voltage
7. **ENTSO-E Live Data Integration** ✅ (2026-01-30)
   - Real-time generation by source (hydro, wind, solar, gas, etc.)
   - Day-ahead electricity prices (€/MWh)
   - Cross-border physical flows with all neighbors
   - 5-minute cache + auto-refresh

### ENTSO-E API Integration
- **API Key**: `35efd923-6969-4470-b2bd-0155b2254346`
- **Endpoints**:
  - `/api/entsoe/generation` - actual generation by type
  - `/api/entsoe/prices` - day-ahead prices
  - `/api/entsoe/cross-border-flows` - physical flows
  - `/api/entsoe/summary` - combined dashboard

### Layers (cleaned up)
- ✅ District heatmap (capacity analysis)
- ✅ Wind turbines
- ✅ Transformers (Umspannwerke)
- ✅ Transmission lines (OSM 380/220kV)
- ✅ Hydropower plants
- ✅ Cross-border connections
- ⚠️ ÖNIP 2030 (marked as approximate - inaccurate georeferencing)

### TODO: Electricity Districts (Netzgebiete)
- Currently using political districts (Bezirke)
- Should use grid operator districts (Netzgebiete)
- Source: https://stele.at/karte/ or E-Control
- Grid operators: APG, Netz NÖ, Wiener Netze, etc.

### Potential Improvements
1. **Price history chart**: Show 24h price trend in live panel
2. **Generation mix pie chart**: Visual breakdown of current generation
3. **Historical flow visualization**: Animate cross-border flows on map
4. **Load data**: Add consumption data from ENTSO-E
5. **Forecasts**: Day-ahead generation forecasts

### Z-Layer Order (front to back)
1. Popups/tooltips (z-index: 1000+)
2. Transformers/substations (z-index: 500)
3. Wind turbines (z-index: 450)
4. Transmission lines (z-index: 420)
5. Hydropower (z-index: 410)
6. Cross-border (z-index: 405)
7. District heatmap (z-index: 400)
8. Base map tiles

### Key Commands
```bash
cd /home/exedev/austria-grid
sudo systemctl restart austria-grid
git add <files> && git commit -m "message" && git push origin main
```

### Live URLs
- App: https://austria-power.exe.xyz:8000/
- GitHub: https://github.com/raffopenssh/austria-grid

### ENTSO-E API Reference
- Portal: https://transparency.entsoe.eu/
- API docs: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
- Austria bidding zone: `10YAT-APG------L`
- Document types:
  - A75: Actual generation per type
  - A44: Day-ahead prices
  - A11: Physical flows
  - A65: System total load
