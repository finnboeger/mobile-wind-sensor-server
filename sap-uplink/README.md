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

# Which races get the wind. Set ONE of these (see "Choosing races" below):

# a) Follow the current race of an event automatically (recommended).
#    The event id is in the event page URL (…#/regatta/races/:eventId=<id>).
SAP_EVENT_ID=<event-uuid>
SAP_COURSE_AREA=Default           # optional: only races on this course area

# b) A fixed list of races (JSON), named as in the AdminConsole's "Tracked races"
#    table: smartphone-tracked races are named "<regatta> <race> <fleet>".
SAP_RACES='[{"regattaName":"Windbot Test","raceName":"Windbot Test R1 Default"}]'

# c) Every tracked race the login may update. Careful with logins that have admin
#    rights on a shared server: SAP then also adds the wind to other people's races.
SAP_ALL_RACES=false

# Optional
SAP_WIND_SOURCE_TYPE=EXPEDITION   # EXPEDITION (measuring instrument) or WEB (shown as "Manual")
SAP_START_FROM=                   # on first run, send what reached PocketBase after this ISO time (default: now)
SAP_POLL_SECONDS=5
SAP_BATCH_SIZE=200                # fixes per request
SAP_REQUEST_TIMEOUT_SECONDS=30
SAP_RACE_REFRESH_SECONDS=30       # how often the current races are looked up (a)
SAP_FINISHED_GRACE_SECONDS=600    # keep sending to a race this long after it finished (a)
SAP_STATE_FILE=/data/cursor.json

POCKETBASE_URL=http://pocketbase:8090
LOG_LEVEL=info  # debug, info, warn, error
```

### Choosing races

With `SAP_EVENT_ID` the service asks SAP for the event's race states
(`/api/v1/events/{id}/racestates`, filtered by `SAP_COURSE_AREA`) every
`SAP_RACE_REFRESH_SECONDS` and sends to every race that

- is tracked (tracking was started, so it shows up under "Tracked races"), and
- hasn't finished, or finished less than `SAP_FINISHED_GRACE_SECONDS` ago.

So the race officer's usual flow (start tracking, set the start, finish) is all
it takes; nothing needs changing on our side between races. SAP itself only
stores fixes inside each race's tracking interval. If several races qualify on
the course area, e.g. because one wasn't finished, all of them get the wind:
they are sailed in the same wind. With no qualifying race, fixes are dropped.
If a lookup fails, the last known races are used.

`SAP_START_FROM` only applies when there is no saved cursor yet. To start over,
remove the volume: `docker compose rm -sf sap-uplink && docker volume rm <project>_sap_uplink_data`.

## Running

### Docker

The service is behind the `sap` compose profile, so it only runs when enabled:

```bash
# in .env
COMPOSE_PROFILES=sap
SAP_URL=...
SAP_USER=...
SAP_PASSWORD=...
SAP_SOURCES=sensor_1=windbot1
SAP_EVENT_ID=...
SAP_COURSE_AREA=...

docker compose up -d
docker compose logs -f sap-uplink
```

To stop uploading after an event, remove `sap` from `COMPOSE_PROFILES` and run
`docker compose stop sap-uplink`.

### Backfill

The running service only sends what reaches PocketBase after its cursor, and
drops fixes while no race is current. Backfill sends the measurements of a
time range once and exits, without touching the cursor. Use it when:

- a race was set up or its tracking started only after it was sailed,
- a race's tracking interval was extended afterwards,
- data is from before the service's first run (including records stored before
  the `created_at` field existed).

```bash
docker compose run --rm sap-uplink python -m src.main \
  --backfill 2027-06-20T09:00:00Z --until 2027-06-20T15:00:00Z
```

Locally: `./scripts/run-local.sh --backfill … --until …`.

- `--backfill FROM` and `--until TO` are ISO times of the measurement (`ts`),
  sending `FROM < ts <= TO`. `--until` defaults to now.
- The fixes go to the configured races. With `SAP_EVENT_ID` that is every
  tracked race on the course area, finished ones included. SAP only keeps
  fixes inside a race's tracking interval.
- It is safe to run again: SAP ignores fixes it already has for that wind
  source.

### Local development

```bash
./scripts/run-local.sh
```

### Tests

```bash
python -m unittest
```
