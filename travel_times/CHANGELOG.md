# Changelog

## 1.3.0
- Groups: one sensor for 2+ people going to the same place. Choose how it reads: longest (everyone has arrived), shortest, or combined total. Attributes list each person plus total time and distance.
- Group members are calculated in the background without creating extra sensors, unless you also add them as normal routes.

## 1.2.0
- New mobile-friendly web UI with a bottom menu: Live, Setup (People / Places / Routes), Find, Settings, Backup.
- Settings are grouped into Waze, Update speed, Arrival & limits and AI place search, with a floating Save bar.
- Backup tab: choose any folder (add-on storage, Share, Media or HA config) with a folder picker, back up now, optional daily/weekly automatic backups, keep-newest-N, restore, download and delete.
- Neat labels and descriptions for the add-on Configuration tab.

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
