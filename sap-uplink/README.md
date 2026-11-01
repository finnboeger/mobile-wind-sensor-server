# SAP Sailing Uplink Service

Python service that forwards true wind measurements from PocketBase to
[SAP Sailing Analytics](https://www.sapsailing.com) through the
[putWind API](https://www.sapsailing.com/sailingserver/webservices/api/v1/putWind.html),
so the boats only upload their data once (to our MQTT broker).

## How it works

```
measurements collection (PocketBase, public read)
    ↓  poll every SAP_POLL_SECONDS, in arrival order, per source_id
convert (needs position + true wind)
    ↓
PUT {SAP_URL}/sailingserver/api/v1/wind/putWind
```

- It runs in its own container, so a slow or unreachable SAP server never
  holds up MQTT ingestion.
- For each source it keeps a cursor in `/data/cursor.json`: the `created_at` time
  (when it reached PocketBase) of the newest measurement sent. Because it follows
  arrival rather than `ts`, measurements that arrive late (e.g. buffered on the
  boat during a mobile data gap) are sent too. Records stored before the
  `created_at` field existed have it empty and are only sent by a backfill.
- The cursor moves on once SAP has answered a batch, even if no race took the
  fixes (e.g. outside every race's tracking interval). While SAP or the network
  is down it stays put, so nothing is lost, and a restart resumes where it
  stopped.
- Sources take turns one batch at a time, so a long backlog on one boat doesn't
  delay the others' live data.
- Measurements without a GPS position or true wind are skipped, because
  putWind requires both.
- If SAP answers `400`, that batch is logged and skipped so it can't block the
  source. Any other error is retried with backoff, per source, so one failing
  source doesn't hold up the others.

### Conversion

| putWind field | from measurement |
|---|---|
| `timepoint` | `ts` as epoch milliseconds |
| `position.latitude_deg` / `longitude_deg` | `gps_lat` / `gps_lng` |
| `direction` | `(true_wind_dir_deg + 180) % 360`: SAP stores the direction the wind blows **to** |
| `speedinknots` | `true_wind_speed_mps × 1.94384` |

## Configuration

All configuration via environment variables:

```bash
# SAP Sailing server: the event's server, without the /sailingserver path
SAP_URL=https://my-event.sapsailing.com

# Login with TRACKED_RACE:UPDATE permission on the races. Either user/password...
SAP_USER=
SAP_PASSWORD=
# ...or an access token
SAP_BEARER_TOKEN=

# Which PocketBase sources to upload, and their wind source name in SAP (required)
SAP_SOURCES=sensor_1=windbot1,sensor_2=windbot2

# Races to put the wind into (JSON). Use the tracked race name from the
# AdminConsole's "Tracked races" table: smartphone-tracked races are named
# "<regatta> <race> <fleet>", e.g. "Windbot Test R1 Default".
SAP_RACES='[{"regattaName":"Windbot Test","raceName":"Windbot Test R1 Default"}]'

# Or, instead of SAP_RACES: send to every tracked race the login may update.
# Careful with logins that have admin rights on a shared server: SAP then also
# adds the wind to other people's races.
SAP_ALL_RACES=false

# Optional
SAP_WIND_SOURCE_TYPE=EXPEDITION   # EXPEDITION (measuring instrument) or WEB (shown as "Manual")
SAP_START_FROM=                   # on first run, send what reached PocketBase after this ISO time (default: now)
SAP_POLL_SECONDS=5
SAP_BATCH_SIZE=200                # fixes per request
SAP_REQUEST_TIMEOUT_SECONDS=30
SAP_STATE_FILE=/data/cursor.json

POCKETBASE_URL=http://pocketbase:8090
LOG_LEVEL=info  # debug, info, warn, error
```

`SAP_START_FROM` only applies when there is no saved cursor yet. To start over,
delete the state file.

## Running

### Local development

```bash
./scripts/run-local.sh
```

### Backfill

The running service only sends what reaches PocketBase after its cursor, and
drops fixes while no race is current. Backfill sends the measurements of a
time range once and exits, without touching the cursor. Use it when:

- a race was set up or its tracking started only after it was sailed,
- a race's tracking interval was extended afterwards,
- data is from before the service's first run (including records stored before
  the `created_at` field existed).

```bash
./scripts/run-local.sh --backfill 2027-06-20T09:00:00Z --until 2027-06-20T15:00:00Z
```

- `--backfill FROM` and `--until TO` are ISO times of the measurement (`ts`),
  sending `FROM < ts <= TO`. `--until` defaults to now.
- The fixes go to the configured races (`SAP_RACES` or `SAP_ALL_RACES`). SAP
  only keeps those inside a race's tracking interval.
- It is safe to run again: SAP ignores fixes it already has for that wind
  source.

### Tests

```bash
python -m unittest
```
