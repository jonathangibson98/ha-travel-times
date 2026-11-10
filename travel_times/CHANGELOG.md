# Changelog

## 1.1.0
- Routes: choose exactly which person goes to which place (no more every-person-to-every-place). Existing sensors are kept.
- Person to person: pick another person as a route's "To" and it follows them live.
- Up to 20 people, 50 places, 200 routes.
- AI place search: pick an ai_task entity in Settings, search for a place, tap Add and it becomes a place (and optionally routes for chosen people).

## 1.0.4
- Fix Waze 403 Forbidden: Waze now blocks non-browser TLS fingerprints, so requests impersonate Chrome/Firefox via curl_cffi.

## 1.0.3
- Fix Waze 410 Gone: Waze retired the www.waze.com routing endpoints; use the routing-livemap-* hosts.

## 1.0.1
- Fix repository URL; CI now checks the published image is publicly pullable.

## 1.0.0
- First release: people x destinations matrix, adaptive Waze polling (fast when moving, slow when parked), MQTT discovery sensors, web UI in the sidebar.
