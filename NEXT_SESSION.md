# Austria Grid - Session Notes

## Changes Made (2026-02-01)

### Persistent ENTSO-E Data Storage for ML Training
1. **Historical data backfill** - 90 days of data being collected
2. **Database indexes** - Efficient time-series queries for ML training
3. **New API endpoints:**
   - `/api/entsoe/history?type=load&days=30&aggregation=daily` - Historical data
   - `/api/entsoe/stats` - Database statistics
4. **Data resolution:** 15 minutes (ENTSO-E standard for Austria)
5. **Cron job:** Runs every 5 minutes to capture latest data

### Current Database Stats
- Generation: 78+ days, 13 PSR types, ~96k records
- Load: 78+ days, ~7.4k records
- Prices: 78+ days, ~7.4k records
- Database size: ~21 MB

---

## Changes Made (2026-01-31 Session 2)

### Standortanalyse UX Improvements
1. **New icon button on map** - Crosshairs icon button next to zoom controls
2. **Clear visual feedback** - Button turns orange and pulses when active
3. **Tooltip instructions** - Shows explanation when mode is enabled
4. **Modal only on click** - Modal opens after clicking location, not before
5. **Toggle behavior** - Click button again to cancel

### Layer Controls Cleanup
**Main layers (always visible):**
- Netzkapazität (Bezirke) - District capacity heatmap
- Windkraftanlagen - Wind turbines  
- Umspannwerke (+Auslastung) - Substations with live load data (merged)
- Hochspannungsleitungen - High voltage lines

**Secondary layers (in expandable section):**
- Wasserkraftwerke
- Alle Kraftwerke (Live)
- Grenzübergänge

**Removed:**
- ÖNIP 2030 (inaccurate georeferencing)
- Separate UW Lastanzeige toggle (merged with Umspannwerke)

### Use Cases Documented
1. **Primary: Find locations for new installations** (Standortanalyse)
2. **Primary: Explore existing wind infrastructure**
3. **Secondary: Check regional grid capacity** 
4. **Secondary: View live energy data**
5. **Tertiary: View detailed infrastructure layers**

---

## Current State (2026-01-31)

### Features Implemented

1. **Live ENTSO-E Data** ✅
   - Real-time generation by source (calibrated to 100% match)
   - Day-ahead electricity prices
   - Cross-border flows with 7 neighbors
   - 5-minute refresh, SQLite storage

2. **Power Plants** ✅
   - 2,296 plants loaded (41.9 GW total capacity)
   - Production calibrated to ENTSO-E totals
   - Color-coded by source type on map
   - Live production estimates

3. **Substation Load Model** ✅
   - 514 substations with load estimates
   - Plants assigned to nearest substation
   - Regional load distribution
   - Cross-border flow assignment

4. **Location Check Tool** ✅ (NEW)
   - Click anywhere on map to analyze location
   - Grid connection feasibility (Easy/Medium/Hard)
   - Nearest substation with available capacity
   - Grid operator identification
   - Wind/Solar production estimates:
     - 10 kW rooftop solar: ~9,600 kWh/year
     - 3 MW wind turbine: ~5,000-7,000 MWh/year
   - Regional capacity factors
   - Nearby installations count
   - Recommendations

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/entsoe/generation` | Live generation by type |
| `/api/entsoe/prices` | Day-ahead prices |
| `/api/entsoe/cross-border-flows` | Physical flows |
| `/api/substation-loads` | Substation load estimates |
| `/api/power-plants` | All plants with production |
| `/api/check-location?lat=X&lon=Y` | Location feasibility check |

### Potential Future Improvements

1. **Wind Atlas Integration**
   - Actual wind speed data at location
   - Historical wind data
   - More accurate capacity factor estimation

2. **Solar Irradiation Data**
   - GHI (Global Horizontal Irradiance)
   - Optimal panel angles
   - Shading analysis

3. **Economics Calculator**
   - Investment cost estimates
   - Payback period calculation
   - Feed-in tariff vs. self-consumption
   - Financing options

4. **Permit Information**
   - Building permit requirements by region
   - Environmental impact assessment needs
   - Grid connection process timeline

5. **Price Forecasting**
   - Day-ahead price predictions
   - Best hours to sell/consume
   - Battery storage optimization

6. **Community Features**
   - Success stories from installers
   - Local installer directory
   - Q&A forum

### Live Site
https://austria-power.exe.xyz:8000/

### Repository
https://github.com/raffopenssh/austria-grid
