# MQTT Ingestion Service

Pure Python service that subscribes to MQTT topics and writes wind sensor measurements to PocketBase.

## Overview

- **Language**: Python 3.11+
- **Type**: Docker-only service (not used in bare-metal deployments)
- **Purpose**: Subscribes to MQTT broker, validates sensor payloads, writes to PocketBase measurements collection
- **Framework**: paho-mqtt (MQTT client), requests (HTTP)

## Architecture

```
MQTT Broker
    ↓
[MQTT Topics] (configured via MQTT_TOPICS env var)
    ↓
MqttListener (paho-mqtt client)
    ↓
Payload validation
    ↓
PocketBase HTTP API
    ↓
measurements collection
```

## Configuration

All configuration via environment variables (Docker-only):

```bash
# MQTT Broker
MQTT_HOST=mosquitto           # MQTT broker hostname/IP
MQTT_PORT=1883                # MQTT broker port
MQTT_USER=                     # Optional: MQTT username
MQTT_PASSWORD=                 # Optional: MQTT password

# MQTT Topics (required)
MQTT_TOPICS=wind/sensor1=sensor_1,wind/sensor2=sensor_2
# Format: topic1=source_id1,topic2=source_id2
# The service maps MQTT topics to source IDs for PocketBase

# PocketBase
POCKETBASE_URL=http://pocketbase:8090
POCKETBASE_ADMIN_TOKEN=your_admin_token

# Logging
LOG_LEVEL=info  # debug, info, warn, error
```

## Installation & Running

### Local Development

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MQTT_HOST=localhost
export MQTT_PORT=1883
export MQTT_TOPICS=wind/test=test_sensor
export POCKETBASE_URL=http://localhost:8090
export POCKETBASE_ADMIN_TOKEN=your_token
export LOG_LEVEL=debug

# Run
python -m src.main
```

### Docker

```bash
# Build image
docker build -t wind-mqtt-ingestion .

# Run container
docker run --rm \
  --network wind-network \
  -e MQTT_HOST=mosquitto \
  -e MQTT_PORT=1883 \
  -e MQTT_TOPICS=wind/sensor1=sensor_1 \
  -e POCKETBASE_URL=http://pocketbase:8090 \
  -e POCKETBASE_ADMIN_TOKEN=token \
  -e LOG_LEVEL=info \
  wind-mqtt-ingestion
```

### Docker Compose

Integrated in root `docker-compose.yml`:

```bash
docker-compose up mqtt-ingestion
```

## Message Format

MQTT messages must be valid JSON conforming to the measurement schema:

```json
{
  "source_id": "sensor_1",
  "ts": "2024-04-12T10:30:00Z",
  "true_wind_dir_deg": 245,
  "true_wind_speed_mps": 8.5,
  "gps_lat": 45.123,
  "gps_lng": -122.456,
  "sensor_heading_deg": 180,
  "sensor_speed_mps": 5.2,
  "apparent_wind_dir_deg": 230,
  "apparent_wind_speed_mps": 7.8
}
```

**Required fields:**
- `source_id`: Must match the source_id in MQTT_TOPICS
- `ts`: ISO 8601 timestamp
- `true_wind_dir_deg`: Wind direction [0, 360)
- `true_wind_speed_mps`: Wind speed in m/s

**Optional fields:**
- `gps_lat`, `gps_lng`: GPS coordinates
- `sensor_heading_deg`, `sensor_speed_mps`: Platform motion
- `apparent_wind_dir_deg`, `apparent_wind_speed_mps`: Apparent wind

## Module Overview

### `src/main.py`

Entry point. Handles:
- Configuration loading and validation
- MQTT listener initialization
- Signal handling (SIGINT, SIGTERM)
- Statistics logging (every 30 seconds)

### `src/config.py`

Environment configuration loading:
- `MqttConfig` class: Parses and validates environment variables
- `load_config()`: Factory function

### `src/mqtt_listener.py`

MQTT listener and PocketBase writer:
- `MqttListener` class with paho-mqtt callbacks
- Topic subscription and message handling
- PocketBase upsert (create or update) via REST API
- Statistics tracking

### `src/__init__.py`

Empty module marker file.

## Statistics

Service logs statistics every 30 seconds:

```
[STATS] Received: 150, Processed: 148, Failed: 2
```

- **Received**: Total MQTT messages received
- **Processed**: Successfully written to PocketBase
- **Failed**: Validation or write errors

## Error Handling

- **Invalid JSON**: Logged as warning, counted as failed
- **Unmapped topics**: Logged as warning, ignored
- **PocketBase unreachable**: Logged as error, message failed
- **MQTT connection lost**: Auto-reconnect with 5s backoff

## Dependencies

```
paho-mqtt==2.1.0    # MQTT client
pocketbase==0.17.0    # HTTP client
pydantic==2.12.0     # Data validation (future use)
python-dotenv==1.2.0 # Env file support (optional)
```

## Testing

### Send test message via MQTT

```bash
# Publish to MQTT broker
mosquitto_pub -h mosquitto -t "wind/sensor1" -m '{
  "source_id": "sensor_1",
  "ts": "2024-04-12T10:30:00Z",
  "true_wind_dir_deg": 245,
  "true_wind_speed_mps": 8.5
}'
```

### Verify in PocketBase

1. Login to PocketBase admin at http://localhost:8090/_/
2. Browse to `measurements` collection
3. Check for new records with `source_id="sensor_1"`

## Development

### Code Style

- Python 3.11+
- Follow PEP 8
- Type hints where applicable
- Docstrings for all classes and public methods

### Logging

Controlled by `LOG_LEVEL` environment variable:
- `debug`: Verbose logging for each message
- `info`: Service lifecycle and statistics (default)
- `warn`: Warnings and errors only
- `error`: Errors only

### Adding New MQTT Topics

Edit environment and restart:

```bash
# Add new topic mapping
export MQTT_TOPICS=wind/sensor1=sensor_1,wind/sensor2=sensor_2,wind/fleet/+/wind=fleet_wind

# Restart service
python -m src.main
```

## Troubleshooting

### Connection Issues

```
ERROR: Failed to connect to MQTT broker, return code 1
```

- Check MQTT_HOST and MQTT_PORT
- Verify MQTT broker is running
- Check network connectivity

### PocketBase Write Errors

```
ERROR: PocketBase write error: 401 Client Error
```

- Verify POCKETBASE_ADMIN_TOKEN is correct
- Check POCKETBASE_URL is accessible
- Ensure PocketBase admin user exists

### Invalid Messages

```
WARNING: Invalid JSON from wind/sensor1: ...
```

- Check message format conforms to schema
- Verify timestamps are ISO 8601
- Check wind direction is [0, 360)

## Future Enhancements

- [ ] Wildcard topic support (MQTT topic patterns)
- [ ] Message batching for high-frequency sensors
- [ ] Support for gzip-compressed payloads
- [ ] Dead-letter queue for failed messages
- [ ] Prometheus metrics export
