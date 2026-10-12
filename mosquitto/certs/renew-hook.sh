#!/bin/sh
# certbot deploy hook: copy the renewed certificate to where Mosquitto reads it
# and reload the broker. Install with
#   certbot certonly --standalone -d "$MQTT_DOMAIN" \
#     --deploy-hook /path/to/mobile-wind-sensor-server/mosquitto/certs/renew-hook.sh
set -eu

CERT_DIR="$(cd "$(dirname "$0")" && pwd)"

cp "$RENEWED_LINEAGE/fullchain.pem" "$CERT_DIR/fullchain.pem"
cp "$RENEWED_LINEAGE/privkey.pem" "$CERT_DIR/privkey.pem"
# The mosquitto image runs the broker as uid/gid 1883
chown 1883:1883 "$CERT_DIR/fullchain.pem" "$CERT_DIR/privkey.pem"
chmod 600 "$CERT_DIR/privkey.pem"

docker kill --signal=HUP wind-mosquitto >/dev/null 2>&1 || true
