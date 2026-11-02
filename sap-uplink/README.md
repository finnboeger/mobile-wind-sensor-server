# SAP Sailing Uplink Service

Python service that forwards true wind measurements from PocketBase to
[SAP Sailing Analytics](https://www.sapsailing.com) through the
[putWind API](https://www.sapsailing.com/sailingserver/webservices/api/v1/putWind.html),
so the boats only upload their data once (to our MQTT broker).

## How it works

```
measurements collection (PocketBase, public read)
    ↓  poll every SAP_POLL_SECONDS, in arrival order
convert (needs position + true wind)
    ↓
PUT {SAP_URL}/sailingserver/api/v1/wind/putWind
```

- It runs in its own container, so a slow or unreachable SAP server never
  holds up MQTT ingestion.
- Every `source_id` in PocketBase is forwarded and used as the wind source ID in
  SAP, so a new boat needs no configuration. Sensors on a test bench can be left
  out with `SAP_EXCLUDE_SOURCES`.
- It keeps one cursor over all sources in `/data/cursor.json`: the `created_at` time
  (when it reached PocketBase) of the newest measurement sent. Because it follows
  arrival rather than `ts`, measurements that arrive late (e.g. buffered on the
  boat during a mobile data gap) are sent too. Records stored before the
  `created_at` field existed have it empty and are only sent by a backfill.
- SAP only adds wind to tracked races: tracking must have been started for
  the race (it then shows up in the AdminConsole's "Tracked races" table), and
  only fixes inside its tracking interval are kept. Fixes sent before a race
  is tracked are lost unless they are backfilled later.
- The cursor moves on once SAP has answered a batch, even if no race took the
  fixes (e.g. outside every race's tracking interval). While SAP or the network
  is down it stays put, so nothing is lost, and a restart resumes where it
  stopped.
- Measurements without a GPS position or true wind are skipped, because
  putWind requires both.
- If SAP answers `400`, that batch is logged and skipped so it can't block the
  uplink. Any other error is retried with backoff.

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

# Access token of a user with TRACKED_RACE:UPDATE permission on the races
# (see "Getting an access token" below)
SAP_BEARER_TOKEN=

# Races to put the wind into (JSON). Use the tracked race name from the
# AdminConsole's "Tracked races" table: smartphone-tracked races are named
# "<regatta> <race> <fleet>", e.g. "Windbot Test R1 Default".
SAP_RACES='[{"regattaName":"Windbot Test","raceName":"Windbot Test R1 Default"}]'

# Or, instead of SAP_RACES: send to every tracked race the login may update.
# Careful with logins that have admin rights on a shared server: SAP then also
# adds the wind to other people's races.
SAP_ALL_RACES=false

# Optional
SAP_EXCLUDE_SOURCES=              # source_ids not to upload, comma-separated (e.g. sensors on a test bench)
SAP_START_FROM=                   # on first run, send what reached PocketBase after this ISO time (default: now)
SAP_POLL_SECONDS=5
SAP_MAX_BATCH_SIZE=200            # records read per round, so also max fixes per request
SAP_REQUEST_TIMEOUT_SECONDS=30
SAP_STATE_FILE=/data/cursor.json

POCKETBASE_URL=http://pocketbase:8090
LOG_LEVEL=info  # debug, info, warn, error
```

`SAP_START_FROM` only applies when there is no saved cursor yet. To start over,
delete the state file.

### Getting an access token

The service only authenticates with an access token. Create one for the user
as described in SAP Sailing's
[access token documentation](https://www.sapsailing.com/security/webservices/api/access_token.html).
The user needs TRACKED_RACE:UPDATE permission on the target races. A wrong or
expired token is not rejected: SAP then treats requests as anonymous and adds
the wind to no race.

## Development

### Tests

```bash
python -m unittest
```
