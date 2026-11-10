# Travel Times (Home Assistant add-on)

Creates a drive-time sensor (minutes) for every person x destination you set up, and polls Waze
faster while the person is actually moving, so it feels close to live directions.

Entities show up as `sensor.<person>_to_<destination>` with attributes: distance, route, eta, arrived.
