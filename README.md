# Wind Dashboard - Realtime Pipeline

A Vue 3 + TypeScript web dashboard for monitoring wind data in real-time, backed by PocketBase with MQTT (Python/Docker) and LoRaWAN (Python/Bare-metal) ingestion services.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Wind Dashboard System                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  FRONTEND LAYER                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Vue 3 Dashboard (Browser)                                │   │
│  │ - Wind direction chart (centered divergence)             │   │
│  │ - Wind speed chart (smoothed + gust)                     │   │
│  │ - KPI cards (avg speed, direction, gust)                 │   │
│  │ - Controls (timeframe, smoothing, source selection)      │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │ HTTP/WebSocket                                 │
│  BACKEND LAYER  │                                                │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ Dashboard Server (Node.js + Express)                     │   │
│  │ - PocketBase bootstrap (collections, security)           │   │
│  │ - REST API for configuration                             │   │
│  │ - WS for realtime subscriptions                           │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │                                                 │
│  DATABASE LAYER │                                                │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ PocketBase (SQLite)                                      │   │
│  │ - measurements collection (source_id, ts, wind data)     │   │
│  │ - public read / authenticated write                      │   │
│  │ - realtime subscriptions                                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
│  INGESTION LAYER (DOCKER)                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ MQTT Ingestion Service (Python)                          │   │
│  │ - Subscribes to MQTT topics                              │   │
│  │ - Validates 1Hz payloads                                 │   │
│  │ - Upserts to measurements collection                     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │

│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Deployment Modes

### Docker (Full Stack)

All services in containers: PocketBase, Dashboard Server, MQTT Ingestion.

```bash
# Setup
cp .env.docker .env
# Edit .env with your PocketBase admin token

# Deploy
docker-compose up -d

# Services
- Dashboard: http://localhost:3000
- PocketBase Admin: http://localhost:8090/_/
- MQTT Broker: localhost:1883

# Cleanup
docker-compose down
```

See [Docker Deployment Guide](#docker-deployment).

### Bare-Metal

Dashboard Server (Node.js) on native OS.

```bash
# Setup
cp config.json.example config.json
# Edit config.json with PocketBase URL and admin token

# Install & build
npm install --workspaces
npm run build --workspaces

# Run Dashboard Server
cd dashboard-server && npm start

# Services
- Dashboard: http://localhost:3000
- PocketBase: http://localhost:8090 (external)
```

See [BARE_METAL_DEPLOYMENT.md](BARE_METAL_DEPLOYMENT.md).

**Note:** LoRaWAN ingestion service will be implemented separately using the lora module.

## Project Structure

```
wind-dashboard/
├── packages/
│   └── shared/                          # Shared types & validation
│       ├── src/
│       │   ├── index.ts
│       │   └── domain/
│       │       └── measurementSchema.ts  # Zod schema for measurements
│       └── package.json
│
├── dashboard-server/                    # REST/WS API server
│   ├── src/
│   │   ├── index.ts
│   │   ├── config/
│   │   │   └── env.ts                   # Environment configuration
│   │   └── pocketbase/
│   │       └── bootstrap.ts             # Collection/index setup
│   ├── Dockerfile
│   └── package.json
│
├── mqtt-ingestion/                      # MQTT subscriber (Python, Docker only)
│   ├── src/
│   │   ├── main.py
│   │   ├── config.py
│   │   └── mqtt_listener.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .dockerignore
│

├── frontend/                            # Vue 3 dashboard
│   ├── src/
│   │   └── main.ts
│   └── package.json
│
├── docker-compose.yml                   # Docker stack definition
├── .env.docker                          # Docker environment template
├── config.json.example                  # Bare-metal config template
├── BARE_METAL_DEPLOYMENT.md             # Setup instructions
└── package.json                         # Workspace root
```

## Configuration

### Measurement Schema

Single `measurements` collection in PocketBase:

```typescript
{
  // Source and timestamp (unique constraint)
  source_id: string,
  ts: Date,

  // GPS (optional)
  gps_lat: number | null,
  gps_lng: number | null,

  // Sensor vehicle motion (optional, for apparent wind)
  sensor_heading_deg: number | null,
  sensor_speed_mps: number | null,

  // Primary: True wind
  true_wind_dir_deg: number [0-360),
  true_wind_speed_mps: number,

  // Secondary: Apparent wind (optional)
  apparent_wind_dir_deg: number | null,
  apparent_wind_speed_mps: number | null
}
```

**Note:** Gust is computed frontend-only; not stored in database.

### Docker Environment Variables

See [.env.docker](.env.docker) for complete list.

**Key variables:**
- `POCKETBASE_URL`: PocketBase connection URL
- `POCKETBASE_ADMIN_TOKEN`: Admin authentication token
- `MQTT_TOPICS`: Topic-to-source mappings (e.g., `wind/sensor1=sensor_1`)
- `DIRECTION_AVERAGE_MODE`: `weighted` (default) or `arithmetic`

### Bare-Metal Configuration

Edit `config.json`:

```json
{
  "pocketbaseUrl": "http://localhost:8090",
  "adminToken": "YOUR_TOKEN",
  "port": 3000,
  "defaultTimeframeMinutes": 15,
  "directionAverageMode": "weighted",
  "defaultSmoothingWindowMinutes": 5,
  "lorawaListenerConfig": {
    "httpPort": 3001,
    "httpHost": "0.0.0.0"
  }
}
```

## API Reference

### Dashboard Server

**Configuration Endpoint**
```
GET /api/config
Returns UI defaults and PocketBase URL
```

**Health Check**
```
GET /health
Returns service health status
```

### LoRaWAN Ingestion Service

**Ingest LoRaWAN Frame**
```
POST /api/lora/frames
Body: { sourceId, timestamp, payload }
```

**Statistics**
```
GET /api/stats
Returns frame ingestion statistics
```

**Health Check**
```
GET /health
Returns service health status
```

## Development

### Prerequisites
- Node.js 18+
- npm 9+
- PocketBase 0.20+ (running separately or in Docker)

### Setup

```bash
# Install workspace dependencies
npm install --workspaces

# Watch for changes
npm run watch --workspaces

# Build all packages
npm run build --workspaces
```

### Development Server

```bash
# Terminal 1: PocketBase
docker run -p 8090:8090 ghcr.io/pocketbase/pocketbase:latest

# Terminal 2: Dashboard (dev mode)
cd dashboard-server
npm run dev
```

Access dashboard at http://localhost:3000

## Testing

### Unit Tests (Phase 8)

```bash
npm test --workspaces
```

Focus areas:
- Circular math (direction averaging, wrap-around)
- Rolling window (smoothing, gust extraction)
- Validation schemas

### Integration Tests (Phase 8)

- MQTT ingest → PocketBase
- LoRaWAN frames → PocketBase
- Frontend subscriptions
- Multi-source visualization

### Manual Verification (Phase 8)

1. **Dashboard bootstrap**: Collections and indexes created
2. **MQTT ingestion**: Synthetic payloads → database
3. **LoRaWAN ingestion**: Test frames → database
4. **Realtime updates**: Dashboard reflects new data without refresh
5. **Direction math**: Edge cases near 359°/1°
6. **Two-source visualization**: Dual averages and scales displayed correctly

## Deployment

### Docker Deployment

```bash
# Build and deploy
docker-compose up -d

# View logs
docker-compose logs -f dashboard-server

# Cleanup
docker-compose down -v
```

### Bare-Metal Deployment

See [BARE_METAL_DEPLOYMENT.md](BARE_METAL_DEPLOYMENT.md) for systemd/launchd setup.

```bash
# Quick start
npm install --workspaces
npm run build --workspaces
cd dashboard-server && npm start
```

## Roadmap

- [x] **Phase 1**: Project setup and service scaffolding
- [ ] **Phase 2**: PocketBase data model and bootstrap
- [ ] **Phase 3**: MQTT ingestion service
- [ ] **Phase 4**: LoRaWAN ingestion service
- [ ] **Phase 5**: Frontend data pipeline and controls
- [ ] **Phase 6**: Wind direction chart
- [ ] **Phase 7**: Wind speed chart
- [ ] **Phase 8**: Verification and operational readiness

## Contributing

1. Follow TypeScript strict mode
2. Use consistent naming (kebab-case for files, camelCase for exports)
3. Add tests for new utilities or business logic
4. Document configuration changes in .env.docker and config.json.example

## Troubleshooting

See [BARE_METAL_DEPLOYMENT.md](BARE_METAL_DEPLOYMENT.md#troubleshooting) for common issues.

## License

[Your License Here]

## References

- [PocketBase Documentation](https://pocketbase.io/docs/)
- [Vue 3 Guide](https://vuejs.org/)
- [ECharts Documentation](https://echarts.apache.org/)
- [MQTT.js](https://github.com/mqttjs/MQTT.js/)
