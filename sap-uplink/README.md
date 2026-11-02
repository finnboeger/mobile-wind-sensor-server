# SAP Sailing Uplink Service

Python service that forwards true wind measurements from PocketBase to
[SAP Sailing Analytics](https://www.sapsailing.com) through the
[putWind API](https://www.sapsailing.com/sailingserver/webservices/api/v1/putWind.html),
so the boats only upload their data once (to our MQTT broker).

## How it works

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
