#!/bin/sh
# Add or update a Mosquitto user (prompts for the password).
#   ./mosquitto/add-user.sh bot1
# Remember to give the user a topic in mosquitto/config/acl.
set -eu

if [ $# -ne 1 ]; then
  echo "Usage: $0 <username>" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

# The broker runs as uid 1883 and refuses a password file it cannot read
docker compose run --rm --no-deps --entrypoint sh mosquitto -c '
  umask 077
  touch /mosquitto/config/passwd
  mosquitto_passwd /mosquitto/config/passwd "$1"
  chown 1883:1883 /mosquitto/config/passwd
  chmod 600 /mosquitto/config/passwd
' sh "$1"

docker kill --signal=HUP wind-mosquitto >/dev/null 2>&1 || true
