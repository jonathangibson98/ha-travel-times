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
