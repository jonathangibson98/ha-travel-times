# Changelog

## 1.6.1
- Fix "The /config folder is not available": newer Home Assistant mounts the config folder at /homeassistant. The card installer and the backup folder picker now find it either way.

## 1.6.0
- Dashboard card: a compact, scrollable, resizable card for your dashboards (great for a TV). Shows routes, groups and trackers in one list, grouped by person or place, as a list or as tiles. Drag it to any size in sections view and it scrolls when the content doesn't fit.
- Settings > Dashboard card: one tap installs the card file into /config/www and registers it as a dashboard resource; an installed card is kept up to date by the add-on automatically.
- Sensors now carry tt_kind / tt_from / tt_to attributes so the card (and your own templates) can tell routes, groups and trackers apart. Tap any row to open the sensor.

## 1.5.0
- Groups can go to several places: pick any number of destinations on one group and each gets its own sensor ("Family to Home", "Family to Airport"). No more duplicate groups with the same members. Existing groups migrate automatically and keep their current sensor and history.
- Routes tab is now organised by person. Tap a person's name to track every place for them, or tap individual place / person chips to switch a route on or off. "Clear" removes a person's routes.
- Setup is tidier: People, Places, Groups, Trackers and every Settings section are collapsible cards that show a one-line summary and open only when you tap them.
- Admin only: Home Assistant admins see every menu. Anyone else who opens the add-on sees only the Live page, with no menus and no way to change anything. This is enforced on the server too, not just hidden. Viewers are sent names and times only, never entity ids, addresses, coordinates or settings.
- The add-on now appears in the sidebar for non-admin users (panel_admin is off); the add-on itself decides who sees what. Needs one rebuild/update for the new dependency (websocket-client).

## 1.4.0
- Trackers: new Setup > Trackers tab. Pick 2 people to see how far apart they are, or pick a whole group and every pair is tracked (3 people = 3 pairs, 4 = 6, up to 8 people). Each tracker gets a drive-time sensor and a distance sensor; choose whether they report the farthest pair, the closest pair or the average. Attributes list every pair with road and straight-line distance, and which pair is closest/farthest. Optional: a separate sensor for each pair. Shows live under "Between people".
- Fast address search (ported from the Zone Creator add-on): US Census, OpenStreetMap and Photon are asked at the same time and results show as soon as the first one answers (no more waiting on AI). New "Address (fast)" mode on the Find tab with search-as-you-type, distance from home or a person, and "lat, lon" paste support. AI search is still there as the second mode.
- New 🔎 button on every place to search an address and fill in its coordinates.
- Place addresses typed into Places are now geocoded in parallel with caching instead of one slow lookup at a time, and lookups are forced to IPv4 (fixes hangs on some Docker/HAOS networks).

## 1.3.1
- All sensors now live under one "Travel Times" MQTT device (one list), named "<Person> to <Place>", instead of one device per person. Existing entities keep their ids and history.

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
