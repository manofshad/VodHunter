#!/bin/sh
set -eu

echo "Applying database migrations before starting public services"
alembic upgrade head

export PUBLIC_API_UPSTREAM=http://127.0.0.1:8000
envsubst '${PUBLIC_API_UPSTREAM}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf
nginx -t

exec "$@"
