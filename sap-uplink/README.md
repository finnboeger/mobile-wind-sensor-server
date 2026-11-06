# SAP Sailing Uplink Service

Python service that forwards true wind measurements from PocketBase to
[SAP Sailing Analytics](https://www.sapsailing.com) through the
[putWind API](https://www.sapsailing.com/sailingserver/webservices/api/v1/putWind.html),
so the boats only upload their data once (to our MQTT broker).

## How it works

```
measurements collection (PocketBase, public read)
    ↓  on every new record (realtime) or every SAP_POLL_SECONDS, in arrival order
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
- New measurements are forwarded within about a second: the service
  subscribes to PocketBase realtime (`/api/realtime`, topic `measurements/*`)
  and a new record of a source that isn't excluded triggers an immediate read
  through the cursor. The subscription is only a wake-up signal, so a dropped
  connection loses nothing: it reconnects with backoff, resubscribes and
  catches up from the cursor. Polling every `SAP_POLL_SECONDS` stays as a
  fallback. Set `SAP_REALTIME=false` to only poll.
- Measurements without a GPS position or true wind are skipped, because
  putWind requires both.
- If SAP answers `400`, that batch is logged and skipped so it can't block the
  uplink. Any other error is retried with backoff.

## Settings in PocketBase

The SAP server, access token, event id and course area can be changed while
the service runs, in the `sap_settings` collection (one record, superusers
only, created by the uplink at startup). Its values win over
`SAP_URL`, `SAP_BEARER_TOKEN`, `SAP_EVENT_ID` and `SAP_COURSE_AREA`; empty
fields fall back to those. The uplink re-reads them every 10 seconds, so no
restart is needed. Until it has a server, a token and races to send to, it
waits and keeps its cursor.

The uplink logs in to PocketBase as a superuser to create and read them
(`POCKETBASE_ADMIN_EMAIL` / `POCKETBASE_ADMIN_PASSWORD`).

The environment's token is only used for the environment's server, and its
course area only for its event: a server set in PocketBase needs its own
token there.

The uplink also writes its status to `sap_uplink_status` (one record, publicly
readable, also created at startup) every 5 seconds, also while it waits for
SAP: heartbeat, settings problem, SAP server and
race selection, realtime connection, cursor, when SAP last answered and last
accepted fixes, the last error, the current and tracked races, and counters.

For every race and source that SAP accepted fixes for, `sap_race_uploads`
keeps the first and last accepted fix (one record each, publicly readable).
It only covers fixes sent since it was added.

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
# SAP_URL, SAP_BEARER_TOKEN, SAP_EVENT_ID and SAP_COURSE_AREA can also be set in
# PocketBase, which wins (see "Settings in PocketBase")

# SAP Sailing server: the event's server, without the /sailingserver path
SAP_URL=https://my-event.sapsailing.com

# Access token of a user with TRACKED_RACE:UPDATE permission on the races
# (see "Getting an access token" below)
SAP_BEARER_TOKEN=

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
SAP_EXCLUDE_SOURCES=              # source_ids not to upload, comma-separated (e.g. sensors on a test bench)
SAP_START_FROM=                   # on first run, send what reached PocketBase after this ISO time (default: now)
SAP_REALTIME=true                 # forward new measurements as soon as PocketBase announces them
SAP_POLL_SECONDS=                 # fallback poll interval (default: 30 with realtime, 5 without)
SAP_MAX_BATCH_SIZE=200            # records read per round, so also max fixes per request
SAP_REQUEST_TIMEOUT_SECONDS=30
SAP_RACE_REFRESH_SECONDS=30       # how often the current races are looked up (a)
SAP_STATE_FILE=/data/cursor.json

POCKETBASE_URL=http://pocketbase:8090
POCKETBASE_ADMIN_EMAIL=           # superuser, for the settings in PocketBase
POCKETBASE_ADMIN_PASSWORD=
LOG_LEVEL=info  # debug, info, warning, error
```

### Choosing races

With `SAP_EVENT_ID` the service asks SAP for the event's race states
(`/api/v1/events/{id}/racestates`, filtered by `SAP_COURSE_AREA`) every
`SAP_RACE_REFRESH_SECONDS` and sends to every race that

- is tracked (tracking was started, so it shows up under "Tracked races"), and
- hasn't finished, or was first seen finished less than 10 minutes ago, so
  measurements that arrive late still get in (SAP keeps fixes up to 3 minutes
  after the end).

So the race officer's usual flow (start tracking, set the start, finish) is all
it takes; nothing needs changing on our side between races. SAP itself only
stores fixes inside each race's tracking interval. If several races qualify on
the course area, e.g. because one wasn't finished, all of them get the wind:
they are sailed in the same wind. With no qualifying race, fixes are dropped.
If a lookup fails, the last known races are used. At startup the event is
looked up once: an unknown `SAP_EVENT_ID` stops the service, and a
`SAP_COURSE_AREA` without races is logged with the course areas that have
races.

`SAP_START_FROM` only applies when there is no saved cursor yet. To start over,
remove the volume: `docker compose rm -sf sap-uplink && docker volume rm <project>_sap_uplink_data`.

### Getting an access token

The service only authenticates with an access token. Create one for the user
as described in SAP Sailing's
[access token documentation](https://www.sapsailing.com/security/webservices/api/access_token.html).
The user needs TRACKED_RACE:UPDATE permission on the target races. A wrong or
expired token is not rejected: SAP then treats requests as anonymous and adds
the wind to no race.

## Running

### Docker

The service is behind the `sap` compose profile, so it only runs when enabled:

```bash
# in .env
COMPOSE_PROFILES=sap
SAP_URL=...
SAP_BEARER_TOKEN=...
SAP_EVENT_ID=...
SAP_COURSE_AREA=...

docker compose up -d
docker compose logs -f sap-uplink
```

To stop uploading after an event, remove `sap` from `COMPOSE_PROFILES` and run
`docker compose stop sap-uplink`.

### Backfill

A backfill can also be requested by adding a record to `sap_backfills`
(`regatta_name` and `race_name`, or both empty for every tracked race of the
event; `range_from`, `range_until`; `status` = `pending`). The uplink runs
pending ones one at a time on a thread of its own, next to live forwarding,
and writes progress, counts and the outcome back to the record. Backfills
interrupted by a restart run again.

On the command line:

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

The script creates a virtual environment, reads PocketBase at
`http://localhost:8090` and keeps the cursor in `.state/`. Export the SAP
settings first:

```bash
export SAP_URL=https://my-event.sapsailing.com
export SAP_BEARER_TOKEN=...
export SAP_RACES='[{"regattaName":"...","raceName":"..."}]'
./scripts/run-local.sh
```

### Tests

```bash
python -m unittest
```
