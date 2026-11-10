# Travel Times

## Install
1. Needs the Mosquitto broker add-on + MQTT integration (or fill in the MQTT options on the Configuration tab).
2. Copy this folder to the `addons` share as `/addons/travel_times`.
3. Settings > Add-ons > Add-on store > three dots > Check for updates > Local add-ons > Travel Times > Install > Start.
4. Open it from the sidebar (Travel Times).

## Use
- People: name + the person/device_tracker that holds their GPS.
- Destinations: a zone, another person (follows them live), "lat, lon", or a street address.
- Save. Sensors appear under Settings > Devices & services > MQTT.

## How "real time" works
Every 5s the add-on reads each person's GPS from HA. If someone moved more than the threshold since the last
Waze call, the pair refreshes every "moving" interval (default 60s). Otherwise every "parked" interval (default 10 min).
Inside the arrival radius the sensor is 0 and no Waze call is made. A global cap (default 20 calls/min) protects you from Waze.

## Notes
- Uses Waze's unofficial routing endpoint (same one the HA Waze integration uses). It can change or rate-limit without warning.
- Location freshness is limited by how often the phone reports GPS.
