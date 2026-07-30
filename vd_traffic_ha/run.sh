#!/usr/bin/with-contenv bashio
set -e

bashio::log.info "Starter VD Trafikhændelser bro..."
exec python3 /main.py /data/options.json
