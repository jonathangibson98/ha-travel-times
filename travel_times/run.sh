#!/command/with-contenv bashio

bashio::log.info "Starting Travel Times..."
cd /app
exec python3 -u /app/main.py
