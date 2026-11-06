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
