#!/bin/sh
set -eu

# BASE_PATH: path prefix the page is served under when the proxy doesn't strip
# it (e.g. /sap); empty to serve at the root
BASE_PATH="${BASE_PATH%/}"
case "$BASE_PATH" in
  ""|/*) ;;
  *) BASE_PATH="/$BASE_PATH" ;;
esac
: "${POCKETBASE_PUBLIC_URL:?Missing POCKETBASE_PUBLIC_URL (the PocketBase URL as the browser reaches it)}"

printf '{ "pocketbaseUrl": "%s" }\n' "$POCKETBASE_PUBLIC_URL" > /usr/share/nginx/html/config.json

{
  echo "server {"
  echo "  listen 80;"
  echo "  root /usr/share/nginx/html;"
  if [ -n "$BASE_PATH" ]; then
    # Assets are relative, so the page needs the trailing slash
    echo "  location = $BASE_PATH { return 301 \$scheme://\$http_host$BASE_PATH/; }"
    echo "  location ^~ $BASE_PATH/ {"
    echo "    rewrite ^$BASE_PATH(/.*)\$ \$1 break;"
    echo "    try_files \$uri /index.html;"
    echo "  }"
  fi
  echo "  location / { try_files \$uri /index.html; }"
  echo "}"
} > /etc/nginx/conf.d/default.conf

exec nginx -g "daemon off;"
