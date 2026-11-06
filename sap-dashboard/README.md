# SAP forwarding dashboard

A static Vue page that shows whether wind data gets from PocketBase to SAP
Sailing. It reads PocketBase directly from the browser: `measurements` and the
SAP uplink's own collections (see `sap-uplink/README.md`).

## What it shows

- **Wind data in PocketBase**, per source: age of the latest measurement, how
  long it took to arrive, and age of the latest one SAP could take (position
  and true wind). Read per source, so a sensor that stopped sending stays
  listed with its age; sources quiet for over 24 hours are left out.
- **Uplink service**: heartbeat, settings problem, SAP server, race selection,
  realtime connection, and how far it has forwarded.
- **SAP**: when it last accepted fixes and last answered, the last error, and a
  warning when SAP answers but no current race takes the fixes (usually a
  wrong or expired access token: SAP doesn't refuse it, it just adds the wind
  to no race).
- **Races**: every race that accepted wind or is tracked in the event, current
  races first, then by last accepted fix, with the first and last accepted fix
  and the sources.
- **Backfills** and how they went.

## Settings

⚙ sets the SAP server, access token, event id and course area, after logging
in as a PocketBase superuser. They go to the uplink's `sap_settings` record,
which wins over its `SAP_URL`, `SAP_BEARER_TOKEN`, `SAP_EVENT_ID` and
`SAP_COURSE_AREA`; empty fields fall back to those. The uplink picks them up
within about 10 seconds.

## Backfill

Backfill… (per race, or for every tracked race of the event) asks the uplink to
send a time range again, after logging in. The range is prefilled with the
days of the race's accepted fixes, and the form shows how many measurements it
holds. SAP only keeps fixes inside a race's tracking interval and ignores
fixes it already has, so a generous range is fine. The request goes to
`sap_backfills`; the uplink runs it next to live forwarding.

## Running

With Docker it runs behind the `sap` compose profile, next to the uplink, on
`SAP_DASHBOARD_PORT` (default 3001):

| Variable | |
|---|---|
| `POCKETBASE_PUBLIC_URL` | PocketBase as the browser reaches it |
| `BASE_PATH` (`SAP_DASHBOARD_BASE_PATH` in `.env`) | path prefix, when a proxy routes e.g. `/sap` here without stripping it; empty to serve at the root |

Assets are referenced relatively, so the same build works with or without a
prefix, e.g. behind Traefik with `PathPrefix(`/sap`)`, with or without a
StripPrefix middleware.

Locally: `npm install && npm run dev`. It reads PocketBase from
`public/config.json` (`http://localhost:8090`).
