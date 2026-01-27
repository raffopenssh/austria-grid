# Austria Grid - TODO

## Data Enhancements (Future Work)

### Hydropower Integration
- [x] Add hydropower plants (156 plants, 13.5 GW total)
  - Source: Austria Groundwater App (groundwater-at.exe.xyz)
  - 86× Laufkraftwerk (run-of-river)
  - 50× Speicherkraftwerk (reservoir)
  - 20× Pumpspeicherkraftwerk (pumped storage)
- [x] Show connection to transmission grid (32 inferred connections for plants >100 MW)
- [ ] Add more plants from https://oesterreichsenergie.at/kraftwerkskarte

### Additional Data Sources
- [x] **Cross-border interconnections**: 9 connections to DE, CZ, HU, SK, SI, IT, CH, LI
- [x] **E-Control Statistics**: Grid lengths by voltage level (2024)
  - 380kV: 3,161 km, 220kV: 3,618 km, 110kV: 11,693 km
- [ ] **ENTSO-E**: Import cross-border flows and grid loading data (needs API key)
  - https://transparency.entsoe.eu
- [ ] **Open Power System Data**: European generation data (Austria not available)
  - https://open-power-system-data.org
- [ ] **ÖNIP (National Infrastructure Plan)**: Planned grid expansions
  - https://www.bmwet.gv.at/dam/jcr:f67c2aa8-4019-4e7b-94ae-e1c847911a05/Integrierter-oesterreichischer-Netzinfrastrukturplan.pdf

### Grid Districts (Netzgebiete)
- [ ] Consider using grid operator districts instead of political districts
- [ ] Reference: https://stele.at/karte/
- [ ] Would better reflect actual grid structure and capacity allocation

### URS Maps (Transmission Corridors)
- [ ] URS_1 to URS_13 maps downloaded from BMWET show planned transmission corridors
- [ ] Could be georeferenced to show planned vs existing grid
- [ ] Files at: /tmp/urs_maps/

### Other Improvements
- [ ] Add groundwater/geothermal stations
- [ ] Show real-time grid loading (if data available)
- [ ] Add renewable energy zones from spatial planning

## Current Data Status

### Transmission Lines (220/380 kV)
- **Source**: OpenStreetMap via Overpass API
- **Count**: 1,291 segments (460× 380kV, 831× 220kV)
- **Quality**: Community-maintained, not officially verified
- **Coverage**: Good match with ÖNIP "Basisnetz Strom 2030"

### Regional Coverage Analysis
380kV:
- Lower Austria/Styria-North: 113 segments
- Vienna/Burgenland-North: 83 segments  
- Tyrol-East/Salzburg-West: 72 segments
- Burgenland-East: 53 segments
- Salzburg-East/Upper Austria-West: 45 segments
- Upper Austria-East/Lower Austria-West: 42 segments
- Tyrol-West: 41 segments
- Vorarlberg: 11 segments (limited - matches plan)

220kV:
- Strong coverage in Tyrol (230 segments)
- Good coverage in Upper Austria/Salzburg (297 segments)
- Adequate coverage elsewhere

## Data Gaps

1. **Line capacities**: OSM doesn't include MW ratings
2. **Grid loading**: No real-time data available
3. **Planned expansions**: ÖNIP shows plans but no digital data
4. **110kV network**: Partially in OSM but not displayed
5. **Interconnectors**: Cross-border capacity data from ENTSO-E needed

## References

- APG (Austrian Power Grid): https://www.apg.at
- E-Control: https://www.e-control.at
- ENTSO-E: https://transparency.entsoe.eu
- ÖNIP: https://www.bmwet.gv.at/Themen/Energie/Strategische-Infrastruktur/Netzinfrastrukturplan
- STELE Platform: https://stele.at
- Open Power System Data: https://open-power-system-data.org
