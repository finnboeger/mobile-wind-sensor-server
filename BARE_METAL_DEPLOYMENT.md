# Bare-Metal Deployment Guide

This guide covers running the Wind Dashboard system on bare-metal (native OS) without Docker.

## Architecture

**Bare-metal deployment includes:**
- **Dashboard Server**: REST/WebSocket API for frontend, PocketBase bootstrap
- **LoRaWAN Ingestion Service**: HTTP listener for incoming LoRaWAN frames
- **Frontend**: Vue 3 + TypeScript web app (served by Dashboard Server)

**External requirement:**
- **PocketBase**: Database with realtime subscriptions (separate deployment or docker container)

**Not used in bare-metal:**
- MQTT Ingestion Service (docker-only)
- MQTT broker

## Prerequisites

### System Requirements
- Node.js 18+ and npm 9+
- PocketBase 0.20+ (running separately)
- Bash or equivalent shell

### Installation Steps

#### 1. Install Node.js

**macOS (Homebrew):**
```bash
brew install node@20
node --version  # verify: v20.x.x
npm --version   # verify: 9.x.x
```

**Linux (Debian/Ubuntu):**
```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

**Windows (via nvm-windows or direct download):**
- Download from https://nodejs.org/
- Or use Chocolatey: `choco install nodejs`

#### 2. Set Up PocketBase

**Option A: Docker (recommended for simplicity)**
```bash
docker run -d \
  --name pocketbase \
  -p 8090:8090 \
  -v pocketbase_data:/pb_data \
  ghcr.io/pocketbase/pocketbase:latest
```

**Option B: Bare-metal**
1. Download PocketBase from https://pocketbase.io/
2. Extract and run:
   ```bash
   ./pocketbase serve --http=0.0.0.0:8090
   ```
3. Access admin console at http://localhost:8090/_/
4. Create admin account or login

#### 3. Configure Wind Dashboard

Copy and edit the configuration file:
```bash
cp config.json.example config.json
```

Edit `config.json`:
```json
{
  "pocketbaseUrl": "http://localhost:8090",
  "adminToken": "PASTE_YOUR_ADMIN_TOKEN_HERE",
  "port": 3000,
  "host": "0.0.0.0",
  "logLevel": "info",
  "defaultTimeframeMinutes": 15,
  "directionAverageMode": "weighted",
  "defaultSmoothingWindowMinutes": 5,
  "lorawaListenerConfig": {
    "httpPort": 3001,
    "httpHost": "0.0.0.0"
  }
}
```

**Key fields:**
- `pocketbaseUrl`: URL where PocketBase is running
- `adminToken`: Authentication token from PocketBase admin console
- `port`: Dashboard Server HTTP port
- `lorawaListenerConfig.httpPort`: LoRaWAN listener HTTP port

#### 4. Get PocketBase Admin Token

1. Open PocketBase admin console at http://localhost:8090/_/
2. Navigate to **Settings** → **API tokens**
3. Create a new token or use the admin token shown in the console
4. Copy the token and paste it in `config.json` as `adminToken`

#### 5. Install Dependencies and Build

```bash
# Install workspace dependencies
npm install --workspaces

# Build all packages
npm run build --workspaces
```

## Running the Services

### Start Dashboard Server

```bash
cd dashboard-server
npm start
```

Expected output:
```
Loading configuration...
Configuration loaded: http://localhost:8090
Bootstrapping PocketBase...
✓ PocketBase bootstrap completed successfully
✓ Dashboard Server listening on http://0.0.0.0:3000
  Health check: http://0.0.0.0:3000/health
  Config endpoint: http://0.0.0.0:3000/api/config
```

Access dashboard at: http://localhost:3000

### Start LoRaWAN Ingestion Service

In a new terminal:
```bash
cd lora-ingestion
npm start
```

Expected output:
```
Loading configuration...
Configuration loaded: http://localhost:8090 (port 3001)
Authenticating with PocketBase...
✓ PocketBase authenticated
✓ LoRaWAN Ingestion Service listening on http://0.0.0.0:3001
  Health check: http://0.0.0.0:3001/health
  Ingest frames: POST http://0.0.0.0:3001/api/lora/frames
  Statistics: http://0.0.0.0:3001/api/stats
```

## Sending LoRaWAN Frames

### HTTP POST to Ingest Endpoint

```bash
curl -X POST http://localhost:3001/api/lora/frames \
  -H "Content-Type: application/json" \
  -d '{
    "sourceId": "lora_sensor_1",
    "timestamp": "2024-01-15T10:30:00Z",
    "payload": {
      "windDir": 245,
      "windSpeed": 8.5,
      "lat": 45.123,
      "lng": -122.456
    }
  }'
```

## System Service Setup (Optional)

Run services as system daemons using systemd (Linux) or launchd (macOS).

### Linux systemd

Create `/etc/systemd/system/wind-dashboard.service`:

```ini
[Unit]
Description=Wind Dashboard Server
After=network.target pocketbase.service
Wants=pocketbase.service

[Service]
Type=simple
User=windbot
WorkingDirectory=/opt/wind-dashboard
ExecStart=/usr/bin/node /opt/wind-dashboard/dashboard-server/dist/index.js
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/wind-lora.service`:

```ini
[Unit]
Description=Wind LoRaWAN Ingestion Service
After=network.target wind-dashboard.service
Wants=wind-dashboard.service

[Service]
Type=simple
User=windbot
WorkingDirectory=/opt/wind-dashboard
ExecStart=/usr/bin/node /opt/wind-dashboard/lora-ingestion/dist/index.js
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable wind-dashboard wind-lora
sudo systemctl start wind-dashboard wind-lora
sudo systemctl status wind-dashboard wind-lora
```

### macOS launchd

Create `~/Library/LaunchAgents/local.windbot.dashboard.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>local.windbot.dashboard</string>
  <key>DaemonNamespace</key>
  <string>local.windbot</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>WorkingDirectory</key>
  <string>/opt/wind-dashboard</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/node</string>
    <string>/opt/wind-dashboard/dashboard-server/dist/index.js</string>
  </array>
  <key>StandardOutPath</key>
  <string>/var/log/wind-dashboard.log</string>
  <key>StandardErrorPath</key>
  <string>/var/log/wind-dashboard.err</string>
</dict>
</plist>
```

Load agent:
```bash
launchctl load ~/Library/LaunchAgents/local.windbot.dashboard.plist
```

## Troubleshooting

### "Cannot find module '@wind-dashboard/shared'"
Make sure dependencies are installed from workspace root:
```bash
npm install --workspaces
```

### "PocketBase bootstrap failed"
1. Verify PocketBase is running and accessible
2. Check `pocketbaseUrl` in config.json
3. Verify `adminToken` is correct in PocketBase admin console

### "EADDRINUSE: address already in use :::3000"
Another service is already using port 3000. Change in config.json:
```json
{
  "port": 3001
}
```

### Dashboard Server won't start on bare-metal
If you get "Missing POCKETBASE_URL environment variable (Docker mode)", the system detected Docker mode instead of bare-metal. This means:
1. You have `POCKETBASE_URL` set as an environment variable
2. Unset it and try again:
   ```bash
   unset POCKETBASE_URL
   npm start
   ```

## Development Mode

For active development with auto-reload:

```bash
# Terminal 1: Watch dashboard-server
cd dashboard-server
npm run watch

# Terminal 2: Run dashboard-server
cd dashboard-server
npm run dev

# Terminal 3: Watch lora-ingestion
cd lora-ingestion
npm run watch

# Terminal 4: Run lora-ingestion
cd lora-ingestion
npm run dev
```

## Verification Checklist

- [ ] PocketBase is running and accessible
- [ ] `config.json` is configured with correct URLs and tokens
- [ ] Dashboard Server starts without errors
- [ ] Dashboard accessible at http://localhost:3000
- [ ] LoRaWAN Ingestion Service starts without errors
- [ ] Health check returns 200: `curl http://localhost:3001/health`
- [ ] LoRaWAN frame ingestion works (send test frame, verify in PocketBase)
- [ ] Dashboard shows real-time data updates
