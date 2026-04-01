#!/bin/bash
set -eu

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
LOGGER_TAG="ws_scraper_safe_reboot"
WS_SCRAPER_SERVICE="ws_scraper.service"
STOP_TIMEOUT_S=45

log() {
    /usr/bin/logger -t "${LOGGER_TAG}" -- "$1"
    printf '%s\n' "$1"
}

log "scheduled reboot starting at $(/bin/date --iso-8601=seconds)"

if /usr/bin/timeout "${STOP_TIMEOUT_S}s" /usr/bin/systemctl stop "${WS_SCRAPER_SERVICE}"; then
    STATE="$(/usr/bin/systemctl is-active "${WS_SCRAPER_SERVICE}" 2>/dev/null || true)"
    log "stop request completed for ${WS_SCRAPER_SERVICE}; state=${STATE}"
else
    RC="$?"
    STATE="$(/usr/bin/systemctl is-active "${WS_SCRAPER_SERVICE}" 2>/dev/null || true)"
    log "stop request did not complete cleanly for ${WS_SCRAPER_SERVICE}; rc=${RC}; state=${STATE}"
fi

/bin/sleep 2
log "requesting host reboot"
exec /usr/bin/systemctl --no-block reboot
