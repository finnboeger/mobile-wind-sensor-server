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

## Development

### Tests

```bash
python -m unittest
```
