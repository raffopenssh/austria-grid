# Cadastre API - Improvement Suggestions

## 1. Point Lookup Endpoint ⭐ (High Priority)

**Current**: Must use bbox API + client-side point-in-polygon
**Suggested**: Direct point lookup endpoint

```
GET /api/cadastre/point?lon=14.0&lat=47.5
```

Response:
```json
{
  "parcel_id": "67302-174",
  "parcel_number": "174",
  "cadastral_community": "67302",
  "ez": "431",
  "area_sqm": 11737.19,
  "status": "E",
  "centroid": {"lon": 14.069183, "lat": 47.501829}
}
```

**Benefits**:
- Eliminates client-side geometry processing
- Reduces response size (no need for full polygon coordinates)
- Much faster for point-based queries (our use case)

## 2. Batch Query Endpoint ⭐⭐ (Critical for Performance)

**Current**: One request per point (4,727 requests = 4-6 hours)
**Suggested**: Batch endpoint

```
POST /api/cadastre/batch
Content-Type: application/json

{
  "points": [
    {"id": "plant_1", "lon": 14.0, "lat": 47.5},
    {"id": "plant_2", "lon": 14.1, "lat": 47.6},
    ...
  ],
  "limit": 1000
}
```

Response:
```json
{
  "results": [
    {"id": "plant_1", "parcel": {...}},
    {"id": "plant_2", "parcel": null}
  ],
  "processed": 1000,
  "found": 987
}
```

**Benefits**:
- 4,727 requests → ~5 requests
- 4-6 hours → ~30 seconds
- Reduces server load significantly

## 3. Response Format Options

Add query parameter for lightweight responses:

```
GET /api/bbox?...&format=properties
```

**Current**: Always returns full GeoJSON with polygon coordinates
**Suggested**: Option to return only properties without geometries

**Benefits**:
- Smaller payloads
- Faster parsing
- Better for non-mapping use cases

## 4. API Documentation Endpoint

```
GET /api/docs
GET /api/cadastre/schema
```

**Benefits**:
- Self-documenting API
- Clear parameter documentation
- Example requests/responses

## 5. Rate Limiting Headers

Add standard rate limit headers:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1234567890
```

**Benefits**:
- Clients can self-regulate
- Prevents accidental overwhelming of API
- Better error handling

## 6. Improved Error Responses

**Current**: Simple 404 or timeout
**Suggested**: Structured error responses

```json
{
  "error": "point_outside_coverage",
  "message": "Coordinates are outside Austria cadastre coverage",
  "coverage": {
    "min_lat": 46.98, "max_lat": 48.22,
    "min_lon": 11.37, "max_lon": 16.40
  }
}
```

**Benefits**:
- Better debugging
- Client can handle errors intelligently
- Provides helpful context

## 7. Caching Headers

Add appropriate cache headers:
```
Cache-Control: public, max-age=86400
ETag: "cadastre-v2024-02"
```

**Benefits**:
- Cadastre data rarely changes
- Clients can cache safely
- Reduces server load

## 8. Query Simplification

**Current**: bbox requires 4 parameters (minlon, minlat, maxlon, maxlat)
**Suggested**: Center + radius option

```
GET /api/bbox?lon=14.0&lat=47.5&radius=100&layer=parcels
```

**Benefits**:
- Simpler for point-based queries
- More intuitive
- Less client-side calculation

## 9. Partial Match Response

For bbox queries that return no exact match:

```json
{
  "features": [],
  "nearest": {
    "parcel_id": "67302-174",
    "distance_meters": 45.3
  }
}
```

**Benefits**:
- Useful for points near parcel boundaries
- Better than empty response
- Helps with data quality issues

## 10. Statistics/Metadata Endpoint Enhancement

**Current**: `/api/cadastre/status` returns basic stats
**Suggested**: Add more useful metadata

```json
{
  "parcels": 43579,
  "coverage": {...},
  "cadastral_communities": 2095,
  "last_updated": "2024-02-15",
  "data_sources": ["BEV"],
  "api_version": "1.0"
}
```

---

## Priority Ranking

1. **🔥 Batch endpoint** - Would reduce our job from 6 hours to <1 minute
2. **⭐ Point lookup endpoint** - Much simpler than bbox + client-side geometry
3. **📦 Response format options** - Reduce bandwidth by 90%+
4. **📖 API documentation** - Improves developer experience
5. **🎛️ Rate limiting headers** - Good API citizenship

## Implementation Complexity

**Easy**: Response format options, caching headers, error messages
**Medium**: Point lookup endpoint, documentation, rate limiting
**Hard**: Batch endpoint (requires architecture changes)

The batch endpoint is hard but has by far the biggest impact.
