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
- Routes: grouped by person. Tap a person's name to track every place for them; tap a chip to turn a single route on or off.
- Groups: 2+ people going to one or more places (for example you and your partner heading home or to the airport). Each destination you pick gets its own sensor. The state follows the rule you pick (longest, shortest or combined total); attributes list each person and the total time and distance.
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

## Who can see what
Home Assistant admins see every menu. Any other Home Assistant user who opens the add-on gets the Live page only. The add-on reads who is asking from the ingress headers and checks that user against Home Assistant's user list (refreshed every minute); the server rejects every other request from non-admins, so it is not just hidden. If Home Assistant can't be asked, or a request carries no user, nobody is treated as admin; check the add-on log for "Could not check admin users".

## Notes
- Uses Waze's unofficial routing endpoint (same one the HA Waze integration uses). It can change or rate-limit without warning.
- Location freshness is limited by how often the phone reports GPS.

## Backups
The Backup tab saves your people, places, routes and settings as a JSON file. Pick the folder with the folder picker:
add-on storage (default, included in Home Assistant backups), `/share`, `/media` or `/config`.
Turn on daily or weekly automatic backups and choose how many files to keep. Restore replaces your current setup with the chosen file
(the backup folder and schedule are kept). Note: new folder access needs the add-on to be rebuilt/updated once so the share, media and config mappings apply.

## Dashboard card
Settings > Dashboard card > **Install card** copies the card into `/config/www/travel-times-card/` and adds it to your dashboard resources
(if `/config/www` didn't exist, restart Home Assistant once; if your dashboards are in YAML mode, add the resource shown yourself).
Then add a **Travel Times** card from the card picker. Every option is in the card's visual editor (Content, Filter, Size & fit,
Text & spacing, Tiles, Look). You can also use YAML. Any option left out uses its default.

```yaml
type: custom:travel-times-card
# content
title: Travel Times
layout: list          # list or tiles
group_by: person      # person, place or none
sort: name            # name or minutes
people: [Jon, MJ]     # optional filter
places: [Home, Work]  # optional filter
entities: []          # optional: only these sensors
show_groups: true
show_trackers: true
show_distance: true
show_via: false
show_members: true
warn_above: 30        # minutes, value turns orange
alert_above: 60       # minutes, value turns red
# size (numbers are px; "50%", "30vw", "20em" also work)
width: 480
height: 270
min_width: 200
max_width: 800
min_height: 120
max_height: 600
aspect_ratio: "16:9"  # height follows the width (ignored when height is set)
align: center         # left, center or right (when narrower than its space)
fit: shrink           # scroll, shrink (text shrinks to fit) or clip
min_font_scale: 0.5   # smallest text size when shrinking
# text and spacing
font_size: 14         # px
font_scale: 1.2       # multiplier, bigger text for a TV
title_scale: 1
value_scale: 1
padding: 8            # px
gap: 8                # px between rows / tiles
row_height: 40        # px, minimum height of each row / tile
border_radius: 12     # px
# tiles
columns: 3
tile_min_width: 136   # px, used when columns is not set
# look
transparent: false
hide_scrollbar: false
```

Sizing tips:
- **Exact px box:** set `width` and `height`. Add `fit: shrink` and the text scales down so everything is visible without scrolling.
- **Keep a ratio:** set `aspect_ratio` (for example `16:9`) and leave `height` empty. Use `width` or `max_width` to choose how wide it is.
- **Sections view:** with no px height or ratio you can drag the card's corner to resize it. With a px height or ratio the card sizes itself.
