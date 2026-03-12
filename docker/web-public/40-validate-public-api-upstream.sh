#!/bin/sh
set -eu

if [ -z "${PUBLIC_API_UPSTREAM:-}" ]; then
    echo "PUBLIC_API_UPSTREAM is required. Set it to the public API internal URL, for example \${api-public.PRIVATE_URL} on DigitalOcean App Platform." >&2
    exit 1
fi
