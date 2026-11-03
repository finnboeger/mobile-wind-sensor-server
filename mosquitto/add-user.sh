#!/bin/sh
# Add or update a Mosquitto user.
#   ./mosquitto/add-user.sh bot1                 # prompts for the password
#   MOSQUITTO_USER=bot1 MOSQUITTO_PASSWORD=... ./mosquitto/add-user.sh
# With MOSQUITTO_PASSWORD set it runs without a terminal (e.g. from a script).
# A boat user may publish to the topic named like the user (see config/acl).
set -eu

if [ $# -gt 1 ]; then
  echo "Usage: $0 [username]  (or set MOSQUITTO_USER)" >&2
  exit 1
fi
user="${1:-${MOSQUITTO_USER:-}}"
if [ -z "$user" ]; then
  echo "Usage: $0 [username]  (or set MOSQUITTO_USER)" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

# The broker runs as uid 1883 and refuses a password file it cannot read
script='
  umask 077
  touch /mosquitto/config/passwd
  if [ -n "${MOSQUITTO_PASSWORD:-}" ]; then
    mosquitto_passwd -b /mosquitto/config/passwd "$1" "$MOSQUITTO_PASSWORD"
  else
    mosquitto_passwd /mosquitto/config/passwd "$1"
  fi
  chown 1883:1883 /mosquitto/config/passwd
  chmod 600 /mosquitto/config/passwd
'

if [ -n "${MOSQUITTO_PASSWORD:-}" ]; then
  # Passed through the environment, so it doesn't show up in the host's process list
  docker compose run --rm --no-deps -T -e MOSQUITTO_PASSWORD \
    --entrypoint sh mosquitto -c "$script" sh "$user"
else
  docker compose run --rm --no-deps --entrypoint sh mosquitto -c "$script" sh "$user"
fi

docker kill --signal=HUP wind-mosquitto >/dev/null 2>&1 || true
