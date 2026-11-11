# Travel Times

## Install
1. Needs the Mosquitto broker add-on + MQTT integration (or fill in the MQTT options on the Configuration tab).
2. Copy this folder to the `addons` share as `/addons/travel_times`.
3. Settings > Add-ons > Add-on store > three dots > Check for updates > Local add-ons > Travel Times > Install > Start.
4. Open it from the sidebar (Travel Times).

## Use
- People: name + the person/device_tracker that holds their GPS.
- Places: a zone, "lat, lon", or a street address.
- Routes: each route is one sensor. "From" is a person; "To" is a place or another person (follows them live).
  Existing setups keep their sensors; use "Everyone -> all places" to fill in every combination.
- Groups: one sensor for 2+ people going to the same place (for example you and your partner heading home). The state follows the rule you pick (longest, shortest or combined total); attributes list each person and the total time and distance.
- Trackers: how far apart people are. Pick 2 people for one tracker, or 3+ and every pair is tracked (3 people = 3 pairs, 4 = 6, up to 8).
  Each tracker makes a drive-time sensor and a distance sensor. Choose what they report: the farthest pair (how spread out the group is), the closest pair, or the average.
  Attributes list every pair (drive minutes, road distance, straight-line distance) plus the closest and farthest pair. Tick "Also make a sensor for each pair" for one sensor per combination.
  Each pair needs one Waze call (A to B; the time is the same either way), shared with any existing person-to-person route, and the usual moving/parked update speeds apply.
- Find a place (fast): the Find tab's "Address" mode searches US Census, OpenStreetMap and Photon at the same time and shows results as soon as the first answers. It also does search-as-you-type, businesses, and "lat, lon". Tap Add to make it a place with routes for the people ticked.
  The 🔎 button on a place does the same and fills in that place's coordinates.
- Find a place (AI): choose an `ai_task` entity in Settings, type e.g. "closest Lowe's", pick Near Home or a person, and tap Add.
  The model suggests places, the add-on geocodes the addresses for accurate coordinates, and the place is added with routes for the people ticked.
  Results are only as good as the model's knowledge; a model with web search gives the best results.
- Save. Sensors appear under Settings > Devices & services > MQTT.

## How "real time" works
Every 5s the add-on reads each person's GPS from HA. If someone moved more than the threshold since the last
Waze call, the pair refreshes every "moving" interval (default 60s). Otherwise every "parked" interval (default 10 min).
Inside the arrival radius the sensor is 0 and no Waze call is made. A global cap (default 20 calls/min) protects you from Waze.

## Notes
- Uses Waze's unofficial routing endpoint (same one the HA Waze integration uses). It can change or rate-limit without warning.
- Location freshness is limited by how often the phone reports GPS.

## Backups
The Backup tab saves your people, places, routes and settings as a JSON file. Pick the folder with the folder picker:
add-on storage (default, included in Home Assistant backups), `/share`, `/media` or `/config`.
Turn on daily or weekly automatic backups and choose how many files to keep. Restore replaces your current setup with the chosen file
(the backup folder and schedule are kept). Note: new folder access needs the add-on to be rebuilt/updated once so the share, media and config mappings apply.
