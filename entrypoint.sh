#!/bin/sh
set -e

# Mount NAS share if NAS_HOST and NAS_SHARE are set
if [ -n "$NAS_HOST" ] && [ -n "$NAS_SHARE" ]; then
    mkdir -p /multimedia
    mount -t cifs \
        "//${NAS_HOST}/${NAS_SHARE}" /multimedia \
        -o "username=${NAS_USER:-guest},password=${NAS_PASS:-},vers=3.0,ro"
    echo "Mounted //${NAS_HOST}/${NAS_SHARE} → /multimedia"
fi

exec python run.py
