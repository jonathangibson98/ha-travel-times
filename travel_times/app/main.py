#!/usr/bin/env python3
"""Travel Times - near-real-time Waze travel-time sensors for Home Assistant.

* Reads each person's GPS from Home Assistant (person / device_tracker entities)
* Asks Waze for the drive time to every destination you pick
* Polls FAST while someone is moving, SLOW while they sit still
* Publishes one sensor per person+destination through MQTT discovery
* Groups: one sensor for 2+ people going to the same place
* Trackers: how far apart 2+ people are from each other (every pair tracked)
* Fast address search (US Census + OpenStreetMap + Photon, asked in parallel)
"""
import itertools
import json
import logging
import math
import os
import re
import socket
import threading
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests
import urllib3.util.connection as _u3c

# IPv4 only: IPv6 lookups hang on some Docker/HAOS networks (this made address lookups crawl)
_u3c.allowed_gai_family = lambda: socket.AF_INET
try:
    from curl_cffi import requests as cffi_requests
except Exception:  # falls back to plain requests (Waze will likely answer 403)
    cffi_requests = None
try:
    import websocket  # websocket-client: only used to ask Home Assistant who is an admin
except Exception:
    websocket = None
from flask import Flask, Response, jsonify, request

DATA = os.environ.get("TT_DATA", "/data")
CFG_FILE = os.path.join(DATA, "travel_times.json")
GEO_FILE = os.path.join(DATA, "geocache.json")
TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
HA = os.environ.get("TT_HA", "http://supervisor/core/api")
SUP = "http://supervisor"
HDR = {"Authorization": f"Bearer {TOKEN}"}
BASE = "travel_times"
STATUS_TOPIC = f"{BASE}/status"
LOOP_SECONDS = 5
# every sensor (people routes and groups) lives under this one MQTT device
DEVICE = {"identifiers": [BASE], "name": "Travel Times", "manufacturer": "Travel Times add-on",
          "model": "Waze travel time"}


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


OPTIONS = load_json(os.path.join(DATA, "options.json"), {})
logging.basicConfig(
    level=getattr(logging, str(OPTIONS.get("log_level", "info")).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
log = logging.getLogger("travel_times")

WAZE_URLS = {
    "US": "https://routing-livemap-am.waze.com/RoutingManager/routingRequest",
    "EU": "https://routing-livemap-row.waze.com/RoutingManager/routingRequest",
    "AU": "https://routing-livemap-row.waze.com/RoutingManager/routingRequest",
    "IL": "https://routing-livemap-il.waze.com/RoutingManager/routingRequest",
}
WAZE_IMPERSONATE = ["chrome", "firefox"]  # floating targets track the newest browser
WAZE_HEADERS = {"User-Agent": "Mozilla/5.0", "referer": "https://www.waze.com/"}

DEFAULT_SETTINGS = {
    "region": "US",
    "units": "imperial",
    "moving_interval": 60,       # seconds between Waze calls while someone is moving
    "idle_interval": 600,        # seconds between Waze calls while someone sits still
    "move_threshold_m": 250,     # how far they must move to count as "moving"
    "arrival_radius_m": 150,     # inside this distance = arrived, time is 0
    "max_requests_per_min": 20,  # hard cap on Waze calls, all pairs combined
    "avoid_tolls": False,
    "ai_task_entity": "",        # ai_task.* entity used for place search
    "backup_dir": os.path.join(DATA, "backups"),
    "backup_auto": "off",        # off | daily | weekly
    "backup_keep": 10,           # how many backup files to keep in the folder
}

COORD_RE = re.compile(r"^\s*(-?\d{1,3}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)\s*$")
ENTITY_RE = re.compile(r"^[a-z_]+\.[a-z0-9_]+$")

lock = threading.RLock()
cfg = {}
status = {}              # "pid|did" -> live info
geocache = {}
geo_fail = {}
req_times = deque()      # timestamps of recent Waze calls
paused_until = 0.0
mq = None
group_cache = {}         # gid -> last published payload
group_live = {}          # gid -> live info for the UI
tracker_cache = {}       # tid -> last published payload
tracker_live = {}        # tid -> live info for the UI
person_locs = {}         # person id -> (lat, lon) from the latest tick
search_cache = {}        # (query, near, quick) -> (time, results)
_home_cache = {"t": 0.0, "pt": None}


# ----------------------------------------------------------------- helpers
def slug(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_") or "x"


def haversine(a, b):
    r = 6371000.0
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dp = p2 - p1
    dl = math.radians(b[1] - a[1])
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def load_cfg():
    c = load_json(CFG_FILE, {})
    c.setdefault("people", [])
    c.setdefault("destinations", [])
    c.setdefault("groups", [])
    c.setdefault("trackers", [])
    for g in c["groups"]:  # v1.3: a group had one destination ("to"); now it can have several
        if "destinations" not in g:
            to = g.pop("to", None)
            g["destinations"] = [to] if to else []
            if to:
                g["legacy"] = to  # that destination keeps its old sensor id
    if "routes" not in c:  # older versions tracked every person -> every destination
        c["routes"] = [{"from": p["id"], "to": d["id"]} for p in c["people"] for d in c["destinations"]]
    s = dict(DEFAULT_SETTINGS)
    s.update(c.get("settings", {}))
    c["settings"] = s
    return c


def endpoints_of(c):
    """Every person (as a possible destination) and every place, by id."""
    ends = {p["id"]: {"id": p["id"], "name": p["name"], "target": p["entity"], "is_person": True}
            for p in c["people"]}
    for d in c["destinations"]:
        ends[d["id"]] = d
    return ends


def pairs_of(c):
    """Visible routes: {(from_person_id, to_id): (person, endpoint)}; one sensor each."""
    people = {p["id"]: p for p in c["people"]}
    ends = endpoints_of(c)
    out = {}
    for r in c.get("routes", []):
        p, d = people.get(r.get("from")), ends.get(r.get("to"))
        if p and d and p["id"] != d["id"]:
            out[(p["id"], d["id"])] = (p, d)
    return out


def all_pairs(c):
    """Visible routes plus the member routes that group sensors need."""
    out = dict(pairs_of(c))
    people = {p["id"]: p for p in c["people"]}
    ends = endpoints_of(c)
    for g in c.get("groups", []):
        for did in group_dests(g):
            d = ends.get(did)
            for m in g.get("members", []):
                if d and m in people and m != d["id"]:
                    out.setdefault((m, d["id"]), (people[m], d))
    for t in c.get("trackers", []):  # person <-> person: one Waze call per pair, reused if a route exists
        for a, b in tracker_pairs(t):
            if a in people and b in people and (a, b) not in out and (b, a) not in out:
                out[(a, b)] = (people[a], ends[b])
    return out


def tracker_pairs(t):
    """Every pair of members: 2 people = 1 pair, 3 = 3, 4 = 6 ..."""
    return list(itertools.combinations(t.get("members", []), 2))


def pair_status(snap, a, b):
    return snap.get(f"{a}|{b}") or snap.get(f"{b}|{a}") or {}


# ----------------------------------------------------------- Home Assistant
def ha_state(entity_id):
    try:
        r = requests.get(f"{HA}/states/{entity_id}", headers=HDR, timeout=8)
        return r.json() if r.status_code == 200 else None
    except (requests.RequestException, ValueError):
        return None


def ha_coords(entity_id):
    st = ha_state(entity_id)
    if not st:
        return None
    a = st.get("attributes", {})
    try:
        return float(a["latitude"]), float(a["longitude"])
    except (KeyError, TypeError, ValueError):
        return None


def list_entities():
    trackers, zones, ais = [], [], []
    try:
        r = requests.get(f"{HA}/states", headers=HDR, timeout=20)
        r.raise_for_status()
        for st in r.json():
            eid, a = st["entity_id"], st.get("attributes", {})
            item = {"id": eid, "name": a.get("friendly_name", eid)}
            if eid.startswith("zone."):
                zones.append(item)
            elif eid.startswith("ai_task."):
                ais.append(item)
            elif eid.startswith("person.") or ("latitude" in a and "longitude" in a):
                trackers.append(item)
    except Exception as e:
        log.warning("Could not list HA entities: %s", e)
    key = lambda x: x["name"].lower()
    return sorted(trackers, key=key), sorted(zones, key=key), sorted(ais, key=key)


# ---------------------------------------------------------------- geocoding
GEO_UA = "ha-travel-times-addon/1.4 (Home Assistant add-on)"
_nom_lock = threading.Lock()
_nom_last = 0.0


def _get_json(url, params, timeout=8):
    r = requests.get(url, params=params, headers={"User-Agent": GEO_UA}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def geo_census(q, near=None):
    """US street addresses. Exact and free, but US only."""
    data = _get_json("https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
                     {"address": q, "benchmark": "Public_AR_Current", "format": "json"})
    out = []
    for m in data.get("result", {}).get("addressMatches", []):
        name = m["matchedAddress"].title()
        name = re.sub(r",\s*([A-Za-z]{2}),", lambda x: ", " + x.group(1).upper() + ",", name)
        out.append({"name": name.split(",")[0], "address": name,
                    "lat": float(m["coordinates"]["y"]), "lon": float(m["coordinates"]["x"])})
    return out


def geo_nominatim(q, near=None):
    """OpenStreetMap: addresses and business names worldwide. Policy: max 1 request/second."""
    global _nom_last
    with _nom_lock:
        gap = 1.1 - (time.time() - _nom_last)
        if gap > 0:
            time.sleep(gap)
        _nom_last = time.time()
    params = {"q": q, "format": "jsonv2", "limit": "6"}
    if near:  # prefer (not force) results near the reference point
        params.update(viewbox=f"{near[1] - .5},{near[0] + .5},{near[1] + .5},{near[0] - .5}", bounded="0")
    out = []
    for d in _get_json("https://nominatim.openstreetmap.org/search", params):
        label = d.get("display_name", "")
        out.append({"name": d.get("name") or label.split(",")[0], "address": label,
                    "lat": float(d["lat"]), "lon": float(d["lon"])})
    return out


def geo_photon(q, near=None):
    """Photon (OSM): quick, typo tolerant, and can be biased toward a point."""
    params = {"q": q, "limit": "6"}
    if near:
        params.update(lat=f"{near[0]:.5f}", lon=f"{near[1]:.5f}")
    out = []
    for f in _get_json("https://photon.komoot.io/api/", params).get("features", []):
        p = f.get("properties", {})
        street = " ".join(x for x in [p.get("housenumber"), p.get("street")] if x)
        parts = [p.get("name"), street, p.get("city") or p.get("town") or p.get("county"),
                 p.get("state"), p.get("postcode")]
        label = ", ".join(dict.fromkeys(x for x in parts if x))
        lon, lat = f["geometry"]["coordinates"]
        out.append({"name": p.get("name") or street or label.split(",")[0], "address": label,
                    "lat": float(lat), "lon": float(lon)})
    return out


# merge order = priority (Census is the most exact for US street addresses)
GEO_PROVIDERS = [("Census", geo_census), ("Photon", geo_photon), ("OpenStreetMap", geo_nominatim)]
GEO_GRACE = 1.2       # after the first provider answers, wait this long for the others
GEO_DEADLINE = 9.0    # never wait longer than this in total


def search_address(q, near=None, limit=8, quick=False):
    """Ask the geocoders at the same time and answer as soon as the first one has results.
    quick=True asks only Photon (made for search-as-you-type). Returns (results, errors)."""
    q = (q or "").strip()
    m = COORD_RE.match(q)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return [{"name": "Dropped pin", "address": f"{lat:.6f}, {lon:.6f}", "lat": lat, "lon": lon,
                     "dist_m": round(haversine(near, (lat, lon))) if near else None,
                     "source": "coordinates"}], []
    key = (q.lower(), tuple(round(x, 2) for x in near) if near else None, quick)
    hit = search_cache.get(key)
    if hit and time.time() - hit[0] < 600:
        return hit[1], []

    providers = [x for x in GEO_PROVIDERS if x[0] == "Photon"] if quick else GEO_PROVIDERS
    ex = ThreadPoolExecutor(max_workers=len(providers))
    futs = {ex.submit(fn, q, near): name for name, fn in providers}
    got, errors = {}, []
    pending, t0, first = set(futs), time.time(), None
    while pending:
        left = GEO_DEADLINE - (time.time() - t0)
        if first is not None:
            left = min(left, GEO_GRACE - (time.time() - first))
        if left <= 0:
            break
        done, pending = futures_wait(pending, timeout=left, return_when=FIRST_COMPLETED)
        for f in done:
            name = futs[f]
            try:
                got[name] = f.result()
                if got[name] and first is None:
                    first = time.time()
            except Exception as e:
                errors.append(f"{name}: {type(e).__name__} {str(e)[:80]}")
                log.debug("Geocoder %s failed: %s", name, e)
    for f in pending:
        errors.append(f"{futs[f]}: too slow")
    ex.shutdown(wait=False, cancel_futures=True)

    out, seen = [], set()
    for name, _ in providers:
        for r in got.get(name, []):
            k = (round(r["lat"], 4), round(r["lon"], 4))
            if k in seen:
                continue
            seen.add(k)
            r["source"] = name
            r["dist_m"] = round(haversine(near, (r["lat"], r["lon"]))) if near else None
            out.append(r)
    out = out[:limit]
    if out:
        if len(search_cache) > 300:
            search_cache.clear()
        search_cache[key] = (time.time(), out)
    return out, errors


def geocode(addr):
    key = addr.strip().lower()
    with lock:
        if key in geocache:
            return tuple(geocache[key])
        if time.time() < geo_fail.get(key, 0):
            return None
    try:
        res, errs = search_address(addr, limit=1)
        if res:
            pt = (res[0]["lat"], res[0]["lon"])
            with lock:
                geocache[key] = list(pt)
                save_json(GEO_FILE, geocache)
            return pt
        if errs:
            log.warning("Geocode failed for %r: %s", addr, "; ".join(errs))
    except Exception as e:
        log.warning("Geocode failed for %r: %s", addr, e)
    with lock:
        geo_fail[key] = time.time() + 600
    return None


def resolve(target):
    t = (target or "").strip()
    m = COORD_RE.match(t)
    if m:
        return float(m.group(1)), float(m.group(2))
    if ENTITY_RE.match(t):
        return ha_coords(t)
    return geocode(t) if t else None


# --------------------------------------------------------------------- Waze
class RateLimited(Exception):
    pass


def street_name(route):
    """Best effort 'via' name. Never allowed to break a calculation."""
    try:
        names = route.get("streetNames") or []
        totals = {}
        for seg in route.get("results", []):
            idx = seg.get("street")
            if isinstance(idx, int) and 0 <= idx < len(names):
                n = names[idx]
                if isinstance(n, dict):
                    n = n.get("streetName") or n.get("name")
                if n:
                    totals[n] = totals.get(n, 0) + float(seg.get("length", 0))
        return max(totals, key=totals.get) if totals else ""
    except Exception:
        return ""


def waze_get(url, params):
    """Waze now rejects non-browser TLS fingerprints with 403, so impersonate a real browser.
    Tries each target until one is not 403 (Waze has blocked pinned Chrome versions before)."""
    if cffi_requests is None:
        return requests.get(url, params=params, headers=WAZE_HEADERS, timeout=15)
    r = None
    for target in WAZE_IMPERSONATE:
        r = cffi_requests.get(url, params=params, headers=WAZE_HEADERS,
                              impersonate=target, timeout=15)
        if r.status_code != 403:
            break
    return r


def waze_route(origin, dest, s):
    opts = ["AVOID_TRAILS:t"]
    if s.get("avoid_tolls"):
        opts.append("AVOID_TOLL_ROADS:t")
    params = {
        "from": f"x:{origin[1]} y:{origin[0]}",
        "to": f"x:{dest[1]} y:{dest[0]}",
        "at": 0,
        "returnJSON": "true",
        "returnGeometries": "true",
        "returnInstructions": "true",
        "timeout": 60000,
        "nPaths": 1,
        "options": ",".join(opts),
    }
    r = waze_get(WAZE_URLS.get(s["region"], WAZE_URLS["US"]), params)
    if r.status_code == 429:
        raise RateLimited()
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Waze: {j['error']}")
    route = j["alternatives"][0]["response"] if j.get("alternatives") else j["response"]
    res = route["results"]
    secs = sum(float(x.get("crossTime", x.get("cross_time", 0))) for x in res)
    meters = sum(float(x.get("length", 0)) for x in res)
    return secs, meters, street_name(route)


def rate_ok(limit):
    now = time.time()
    while req_times and now - req_times[0] > 60:
        req_times.popleft()
    return len(req_times) < limit


# ------------------------------------------------------------- group sensors
# A group = several people + any number of destinations. One sensor per destination.
def group_dests(g):
    return g.get("destinations") or ([g["to"]] if g.get("to") else [])


def group_key(g, did):
    """Sensor key. The destination a v1.3 group already had keeps its original id."""
    return g["id"] if did == g.get("legacy") else f"{g['id']}_{did}"


def group_keys(c):
    return {group_key(g, d) for g in c.get("groups", []) for d in group_dests(g)}


def group_disc_topic(key):
    return f"homeassistant/sensor/{BASE}_group_{key}/config"


def publish_group_discovery(g, ends):
    for did in group_dests(g):
        d = ends.get(did)
        if not d:
            continue
        key = group_key(g, did)
        payload = {
            "name": f"{g['name']} to {d['name']}",
            "unique_id": f"{BASE}_group_{key}",
            "default_entity_id": f"sensor.{slug(g['name'])}_to_{slug(d['name'])}",
            "state_topic": f"{BASE}/group/{key}/state",
            "json_attributes_topic": f"{BASE}/group/{key}/attrs",
            "availability_topic": STATUS_TOPIC,
            "unit_of_measurement": "min",
            "device_class": "duration",
            "state_class": "measurement",
            "icon": "mdi:account-group",
            "device": DEVICE,
        }
        pub(group_disc_topic(key), json.dumps(payload))


def group_state(c, g, did, snap):
    """(minutes, attrs) for a group going to one destination, or None until every member has a result."""
    people = {p["id"]: p for p in c["people"]}
    d = endpoints_of(c).get(did)
    if not d:
        return None
    members, unit = [], None
    for m in g["members"]:
        if m == did or m not in people:  # someone can't travel to themselves
            continue
        st = snap.get(f"{m}|{did}", {})
        if st.get("minutes") is None:
            return None
        unit = st.get("unit") or unit
        members.append({"name": people[m]["name"], "minutes": st["minutes"],
                        "distance": st.get("distance") or 0, "arrived": bool(st.get("arrived"))})
    if not members:
        return None
    mins = [x["minutes"] for x in members]
    mode = g.get("mode", "max")
    minutes = round({"max": max(mins), "min": min(mins), "sum": sum(mins)}[mode], 1)
    now = datetime.now().astimezone()
    attrs = {
        "tt_kind": "group", "tt_from": g["name"], "tt_to": d["name"],
        "mode": mode, "members": members, "member_count": len(members),
        "total_minutes": round(sum(mins), 1), "longest_minutes": round(max(mins), 1),
        "shortest_minutes": round(min(mins), 1),
        "total_distance": round(sum(x["distance"] for x in members), 1), "distance_unit": unit,
        "arrived": all(x["arrived"] for x in members),
        "eta": (now + timedelta(minutes=minutes if mode != "sum" else max(mins))).isoformat(timespec="seconds"),
        "last_calculated": now.isoformat(timespec="seconds"),
        "destination": d["target"],
    }
    return minutes, attrs


def publish_groups():
    with lock:
        c = json.loads(json.dumps(cfg))
        snap = {k: dict(v) for k, v in status.items()}
    for g in c.get("groups", []):
        for did in group_dests(g):
            res = group_state(c, g, did, snap)
            if not res:
                continue
            key = group_key(g, did)
            minutes, attrs = res
            ckey = json.dumps([minutes, attrs["members"], attrs["mode"]])
            with lock:
                group_live[key] = {"minutes": minutes, "unit": attrs["distance_unit"], "t": time.time(),
                                   "members": attrs["members"], "total_distance": attrs["total_distance"],
                                   "arrived": attrs["arrived"], "mode": attrs["mode"], "error": None}
                if group_cache.get(key) == ckey:
                    continue
                group_cache[key] = ckey
            pub(f"{BASE}/group/{key}/state", str(minutes))
            pub(f"{BASE}/group/{key}/attrs", json.dumps(attrs))


# ------------------------------------------------------------ tracker sensors
# A tracker shows how far apart 2+ people are. Every pair of members is tracked
# (2 people = 1 pair, 3 = 3, 4 = 6 ...). Each tracker gets a drive-time sensor and a
# distance sensor; the mode picks which pair they report (farthest, closest, average).
# Optional: one extra sensor per pair.
TRACKER_MODES = ("max", "min", "avg")


def tracker_disc(tid, kind):
    return f"homeassistant/sensor/{BASE}_tracker_{tid}_{kind}/config"


def tracker_pair_disc(tid, key):
    return f"homeassistant/sensor/{BASE}_tracker_{tid}_p_{key}/config"


def tracker_topics(c):
    """Every discovery topic the trackers in this config own (used to delete stale entities)."""
    out = set()
    for t in c.get("trackers", []):
        out.add(tracker_disc(t["id"], "time"))
        out.add(tracker_disc(t["id"], "dist"))
        if t.get("pair_sensors"):
            for a, b in tracker_pairs(t):
                out.add(tracker_pair_disc(t["id"], f"{a}_{b}"))
    return out


def conv_dist(v, frm, to):
    if v is None or not frm or frm == to:
        return v
    return v * 1.609344 if frm == "mi" else v / 1.609344


def tracker_state(c, t, snap, locs):
    """Summary for one tracker. Always returns the live pair info (straight-line distance works
    before the first Waze answer); minutes/distance stay None until a pair has a result."""
    people = {p["id"]: p for p in c["people"]}
    unit = "mi" if c["settings"]["units"] == "imperial" else "km"
    div = 1609.344 if unit == "mi" else 1000.0
    pairs = []
    for a, b in tracker_pairs(t):
        if a not in people or b not in people:
            continue
        st = pair_status(snap, a, b)
        la, lb = locs.get(a), locs.get(b)
        mins = st.get("minutes")
        dist = conv_dist(st.get("distance"), st.get("unit"), unit) if mins is not None else None
        pairs.append({
            "key": f"{a}_{b}", "a": people[a]["name"], "b": people[b]["name"],
            "minutes": mins, "distance": round(dist, 1) if dist is not None else None,
            "straight_line": round(haversine(la, lb) / div, 1) if la and lb else None,
            "arrived": bool(st.get("arrived")), "error": st.get("error"),
        })
    if not pairs:
        return None
    mode = t.get("mode", "max")

    def agg(vals):
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return round({"max": max(vals), "min": min(vals), "avg": sum(vals) / len(vals)}[mode], 1)

    ready = [x for x in pairs if x["minutes"] is not None]
    far = max(ready, key=lambda x: x["minutes"]) if ready else None
    near = min(ready, key=lambda x: x["minutes"]) if ready else None
    return {
        "name": t["name"], "mode": mode, "unit": unit, "pairs": pairs,
        "minutes": agg(x["minutes"] for x in pairs), "distance": agg(x["distance"] for x in pairs),
        "straight_line": agg(x["straight_line"] for x in pairs),
        "ready": len(ready), "pair_count": len(pairs),
        "together": bool(ready) and all(x["arrived"] for x in ready),
        "farthest_pair": f"{far['a']} & {far['b']}" if far else None,
        "closest_pair": f"{near['a']} & {near['b']}" if near else None,
    }


def publish_tracker_discovery(c, t):
    unit = "mi" if c["settings"]["units"] == "imperial" else "km"
    people = {p["id"]: p for p in c["people"]}
    tid, base = t["id"], f"{BASE}/tracker/{t['id']}"
    common = {"availability_topic": STATUS_TOPIC, "state_class": "measurement", "device": DEVICE,
              "json_attributes_topic": f"{base}/attrs"}
    pub(tracker_disc(tid, "time"), json.dumps({
        **common, "name": f"{t['name']} drive time", "unique_id": f"{BASE}_tracker_{tid}_time",
        "default_entity_id": f"sensor.{slug(t['name'])}_drive_time", "state_topic": f"{base}/time",
        "unit_of_measurement": "min", "device_class": "duration", "icon": "mdi:car-multiple"}))
    pub(tracker_disc(tid, "dist"), json.dumps({
        **common, "name": f"{t['name']} distance", "unique_id": f"{BASE}_tracker_{tid}_dist",
        "default_entity_id": f"sensor.{slug(t['name'])}_distance", "state_topic": f"{base}/dist",
        "unit_of_measurement": unit, "device_class": "distance", "icon": "mdi:map-marker-distance"}))
    if t.get("pair_sensors"):
        for a, b in tracker_pairs(t):
            if a not in people or b not in people:
                continue
            key, an, bn = f"{a}_{b}", people[a]["name"], people[b]["name"]
            pub(tracker_pair_disc(tid, key), json.dumps({
                "name": f"{an} and {bn} drive time", "unique_id": f"{BASE}_tracker_{tid}_p_{key}",
                "default_entity_id": f"sensor.{slug(an)}_and_{slug(bn)}_drive_time",
                "state_topic": f"{base}/pair/{key}/state", "json_attributes_topic": f"{base}/pair/{key}/attrs",
                "availability_topic": STATUS_TOPIC, "unit_of_measurement": "min",
                "device_class": "duration", "state_class": "measurement", "icon": "mdi:map-marker-distance",
                "device": DEVICE}))


def publish_trackers():
    with lock:
        c = json.loads(json.dumps(cfg))
        snap = {k: dict(v) for k, v in status.items()}
        locs = dict(person_locs)
    for t in c.get("trackers", []):
        r = tracker_state(c, t, snap, locs)
        if not r:
            continue
        with lock:
            tracker_live[t["id"]] = {k: r[k] for k in (
                "minutes", "distance", "straight_line", "unit", "mode", "pairs", "ready", "pair_count",
                "together", "farthest_pair", "closest_pair")}
            tracker_live[t["id"]]["t"] = time.time()
        if r["minutes"] is None:
            continue  # nothing calculated yet
        key = json.dumps([r["minutes"], r["distance"], r["straight_line"], r["mode"], r["unit"], r["pairs"]])
        with lock:
            if tracker_cache.get(t["id"]) == key:
                continue
            tracker_cache[t["id"]] = key
        ready = [x for x in r["pairs"] if x["minutes"] is not None]
        base = f"{BASE}/tracker/{t['id']}"
        attrs = {
            "tt_kind": "tracker", "tt_from": t["name"], "distance": r["distance"],
            "mode": r["mode"], "pair_count": r["pair_count"], "pairs_ready": r["ready"],
            "together": r["together"], "distance_unit": r["unit"],
            "straight_line_distance": r["straight_line"],
            "farthest_pair": r["farthest_pair"], "closest_pair": r["closest_pair"],
            "longest_minutes": max(x["minutes"] for x in ready),
            "shortest_minutes": min(x["minutes"] for x in ready),
            "average_minutes": round(sum(x["minutes"] for x in ready) / len(ready), 1),
            "pairs": [{k: v for k, v in x.items() if k != "key"} for x in r["pairs"]],
            "last_calculated": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        pub(f"{base}/time", str(r["minutes"]))
        if r["distance"] is not None:
            pub(f"{base}/dist", str(r["distance"]))
        pub(f"{base}/attrs", json.dumps(attrs))
        if t.get("pair_sensors"):
            for x in ready:
                pub(f"{base}/pair/{x['key']}/state", str(x["minutes"]))
                pub(f"{base}/pair/{x['key']}/attrs", json.dumps({
                    "between": [x["a"], x["b"]], "distance": x["distance"], "distance_unit": r["unit"],
                    "straight_line_distance": x["straight_line"], "arrived": x["arrived"]}))


# --------------------------------------------------------------------- MQTT
def pub(topic, payload, retain=True):
    if mq is not None:
        mq.publish(topic, payload, qos=0, retain=retain)


def disc_topic(pid, did):
    return f"homeassistant/sensor/{BASE}_{pid}_{did}/config"


def publish_discovery(p, d):
    payload = {
        "name": f"{p['name']} to {d['name']}",
        "unique_id": f"{BASE}_{p['id']}_{d['id']}",
        "default_entity_id": f"sensor.{slug(p['name'])}_to_{slug(d['name'])}",
        "state_topic": f"{BASE}/{p['id']}/{d['id']}/state",
        "json_attributes_topic": f"{BASE}/{p['id']}/{d['id']}/attrs",
        "availability_topic": STATUS_TOPIC,
        "unit_of_measurement": "min",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": "mdi:car-clock",
        "device": DEVICE,
    }
    pub(disc_topic(p["id"], d["id"]), json.dumps(payload))


def publish_state(pid, did, st):
    if st.get("minutes") is None:
        return
    pub(f"{BASE}/{pid}/{did}/state", str(st["minutes"]))
    pub(f"{BASE}/{pid}/{did}/attrs", json.dumps(st.get("attrs", {})))


def republish_all():
    with lock:
        c = json.loads(json.dumps(cfg))
        snap = {k: dict(v) for k, v in status.items()}
    for (pid, did), (p, d) in pairs_of(c).items():
        publish_discovery(p, d)
        publish_state(pid, did, snap.get(f"{pid}|{did}", {}))
    ends = endpoints_of(c)
    for g in c.get("groups", []):
        publish_group_discovery(g, ends)
    group_cache.clear()
    publish_groups()
    for t in c.get("trackers", []):
        publish_tracker_discovery(c, t)
    tracker_cache.clear()
    publish_trackers()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker")
        client.publish(STATUS_TOPIC, "online", retain=True)
        republish_all()
    else:
        log.error("MQTT connect refused, rc=%s", rc)


def mqtt_creds():
    host = (OPTIONS.get("mqtt_host") or "").strip()
    if host:
        return dict(host=host, port=int(OPTIONS.get("mqtt_port") or 1883),
                    username=OPTIONS.get("mqtt_user") or None,
                    password=OPTIONS.get("mqtt_password") or None)
    r = requests.get(f"{SUP}/services/mqtt", headers=HDR, timeout=10)
    r.raise_for_status()
    d = r.json()["data"]
    return dict(host=d["host"], port=int(d["port"]),
                username=d.get("username"), password=d.get("password"))


def start_mqtt():
    global mq
    while True:
        try:
            c = mqtt_creds()
            break
        except Exception as e:
            log.error("No MQTT broker found (%s). Install/start Mosquitto or fill in the "
                      "MQTT options. Retrying in 15s.", e)
            time.sleep(15)
    client = mqtt.Client(client_id="travel_times_addon")
    if c["username"]:
        client.username_pw_set(c["username"], c["password"])
    client.will_set(STATUS_TOPIC, "offline", retain=True)
    client.on_connect = on_connect
    client.reconnect_delay_set(1, 60)
    client.connect_async(c["host"], c["port"], 60)
    client.loop_start()
    mq = client


# -------------------------------------------------------------------- engine
def record_result(p, d, st, minutes, dist_m, via, s, arrived):
    now = datetime.now().astimezone()
    unit = "mi" if s["units"] == "imperial" else "km"
    dist = round(dist_m / (1609.344 if unit == "mi" else 1000.0), 1)
    eta = now + timedelta(minutes=minutes)
    st["minutes"] = minutes
    st["distance"] = dist
    st["unit"] = unit
    st["via"] = via
    st["arrived"] = arrived
    st["attrs"] = {
        "tt_kind": "route", "tt_from": p["name"], "tt_to": d["name"],
        "distance": dist,
        "distance_unit": unit,
        "route": via,
        "arrived": arrived,
        "eta": eta.isoformat(timespec="seconds"),
        "last_calculated": now.isoformat(timespec="seconds"),
        "origin": p["entity"],
        "destination": d["target"],
    }
    if (p["id"], d["id"]) in pairs_of(cfg):
        publish_state(p["id"], d["id"], st)


def tick():
    global paused_until
    with lock:
        c = json.loads(json.dumps(cfg))
    s = c["settings"]
    now = time.time()
    routes = all_pairs(c)
    if not routes:
        return

    locs = {p["id"]: ha_coords(p["entity"]) for p in c["people"]}
    with lock:
        person_locs.clear()
        person_locs.update({k: v for k, v in locs.items() if v})
    dcoords = {}
    for (_, did), (_, d) in routes.items():
        if did not in dcoords:
            dcoords[did] = locs[did] if did in locs else resolve(d["target"])

    due = []
    for (pid, did), (p, d) in routes.items():
        loc, dc = locs[pid], dcoords[did]
        key = f"{pid}|{did}"
        with lock:
            st = status.setdefault(key, {})
            if not loc or not dc:
                st["error"] = "no location yet" if not loc else "can't find destination"
                continue
            if haversine(loc, dc) <= s["arrival_radius_m"]:
                if not st.get("arrived"):
                    record_result(p, d, st, 0, 0, "", s, True)
                    st.update(t=now, loc=loc, dc=dc, error=None, fail=0)
                continue
            if now < st.get("retry_at", 0):
                continue
            age = now - st.get("t", 0)
            last, lastd = st.get("loc"), st.get("dc")
            moved = haversine(loc, last) if last else 1e9
            if d.get("is_person") and lastd:  # person -> person: either end moving counts
                moved = max(moved, haversine(dc, lastd))
            first = st.get("t") is None
            if first or age >= s["idle_interval"] or (
                    moved >= s["move_threshold_m"] and age >= s["moving_interval"]):
                due.append((age, p, d, loc, dc, key))

    due.sort(key=lambda x: -x[0])  # most overdue first
    for age, p, d, loc, dc, key in due:
        if now < paused_until or not rate_ok(s["max_requests_per_min"]):
            break
        req_times.append(time.time())
        try:
            secs, meters, via = waze_route(loc, dc, s)
            with lock:
                st = status[key]
                record_result(p, d, st, round(secs / 60.0, 1), meters, via, s, False)
                st.update(t=time.time(), loc=loc, dc=dc, error=None, fail=0)
            log.debug("%s -> %s: %.1f min", p["name"], d["name"], secs / 60.0)
        except RateLimited:
            paused_until = time.time() + 300
            log.warning("Waze said slow down (429). Pausing 5 minutes.")
            break
        except Exception as e:
            with lock:
                st = status[key]
                st["fail"] = st.get("fail", 0) + 1
                st["error"] = str(e)[:120]
                st["retry_at"] = time.time() + min(30 * 2 ** st["fail"], 900)
            log.warning("Route %s -> %s failed: %s", p["name"], d["name"], e)


def worker():
    while True:
        try:
            tick()
        except Exception:
            log.exception("tick crashed")
        try:
            publish_groups()
        except Exception:
            log.exception("group publish crashed")
        try:
            publish_trackers()
        except Exception:
            log.exception("tracker publish crashed")
        try:
            maybe_auto_backup()
        except Exception:
            log.exception("backup check crashed")
        time.sleep(LOOP_SECONDS)


# ------------------------------------------------------------------ backups
STATE_FILE = os.path.join(DATA, "backup_state.json")
BACKUP_RE = re.compile(r"^travel_times_backup_[A-Za-z0-9_\-]+\.json$")
ROOT_LABELS = {DATA: "Add-on storage", "/share": "Share", "/media": "Media", "/config": "HA config"}


def backup_roots():
    return [os.path.realpath(r) for r in (DATA, "/share", "/media", "/config") if os.path.isdir(r)]


def safe_dir(path):
    """Real path if it is inside a folder this add-on may use, else None."""
    if not path:
        return None
    real = os.path.realpath(str(path))
    for r in backup_roots():
        if real == r or real.startswith(r + os.sep):
            return real
    return None


def list_backups(d):
    out = []
    try:
        for e in os.scandir(d):
            if e.is_file() and BACKUP_RE.match(e.name):
                st = e.stat()
                out.append({"name": e.name, "size": st.st_size, "mtime": st.st_mtime})
    except OSError:
        pass
    return sorted(out, key=lambda x: -x["mtime"])


def make_backup(d, keep, why="manual"):
    real = safe_dir(d)
    if not real:
        raise RuntimeError("That folder is not allowed")
    os.makedirs(real, exist_ok=True)
    with lock:
        c = json.loads(json.dumps(cfg))
    now = datetime.now().astimezone()
    name = f"travel_times_backup_{now.strftime('%Y%m%d_%H%M%S')}.json"
    save_json(os.path.join(real, name), {"app": "travel_times", "created": now.isoformat(timespec="seconds"),
                                         "reason": why, "config": c})
    for old in list_backups(real)[max(1, int(keep)):]:
        try:
            os.remove(os.path.join(real, old["name"]))
        except OSError:
            pass
    return name


def maybe_auto_backup():
    with lock:
        s = dict(cfg.get("settings", {}))
    every = {"daily": 86400, "weekly": 604800}.get(s.get("backup_auto"))
    if not every:
        return
    st = load_json(STATE_FILE, {})
    if time.time() - st.get("last", 0) < every:
        return
    try:
        name = make_backup(s["backup_dir"], s["backup_keep"], "auto")
        save_json(STATE_FILE, {"last": time.time()})
        log.info("Automatic backup written: %s", name)
    except Exception as e:
        save_json(STATE_FILE, {"last": time.time() - every + 3600})  # retry in an hour
        log.warning("Automatic backup failed: %s", e)


# ----------------------------------------------------------------- config API
def clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, int(float(v))))
    except (TypeError, ValueError):
        return default


def clean_cfg(body):
    people, dests, routes, seen, idmap = [], [], [], set(), {}

    def new_id(raw):
        raw = str(raw or "")
        if re.fullmatch(r"[a-f0-9]{8}", raw) and raw not in seen:
            seen.add(raw)
            return raw
        while True:
            i = uuid.uuid4().hex[:8]
            if i not in seen:
                seen.add(i)
                return i

    for p in (body.get("people") or [])[:20]:
        name, ent = str(p.get("name", "")).strip()[:40], str(p.get("entity", "")).strip()
        if name and ENTITY_RE.match(ent):
            pid = new_id(p.get("id"))
            idmap[str(p.get("id"))] = pid
            people.append({"id": pid, "name": name, "entity": ent})
    for d in (body.get("destinations") or [])[:50]:
        name, tgt = str(d.get("name", "")).strip()[:40], str(d.get("target", "")).strip()[:200]
        if name and tgt:
            did = new_id(d.get("id"))
            idmap[str(d.get("id"))] = did
            dests.append({"id": did, "name": name, "target": tgt})

    person_ids = {p["id"] for p in people}
    valid_to = person_ids | {d["id"] for d in dests}
    done = set()
    for r in (body.get("routes") or [])[:200]:
        f, t = idmap.get(str(r.get("from"))), idmap.get(str(r.get("to")))
        if f in person_ids and t in valid_to and f != t and (f, t) not in done:
            done.add((f, t))
            routes.append({"from": f, "to": t})

    groups = []
    for g in (body.get("groups") or [])[:20]:
        name = str(g.get("name", "")).strip()[:40]
        raw = g.get("destinations")
        if raw is None:  # old backups / v1.3 groups had a single "to"
            raw = [g.get("to")] if g.get("to") else []
        dl = []
        for x in raw:
            x = idmap.get(str(x))
            if x in valid_to and x not in dl:
                dl.append(x)
        dl = dl[:10]
        members = []
        for m in g.get("members") or []:
            m = idmap.get(str(m))
            if m in person_ids and m not in members:
                members.append(m)
        if name and dl and len(members) >= 2:
            ng = {"id": new_id(g.get("id")), "name": name, "members": members[:8], "destinations": dl,
                  "mode": g.get("mode") if g.get("mode") in ("max", "min", "sum") else "max"}
            legacy = idmap.get(str(g.get("legacy") or (g.get("to") if g.get("destinations") is None else None)))
            if legacy in dl:
                ng["legacy"] = legacy
            groups.append(ng)

    trackers, pname = [], {p["id"]: p["name"] for p in people}
    for t in (body.get("trackers") or [])[:20]:
        members = []
        for m in t.get("members") or []:
            m = idmap.get(str(m))
            if m in person_ids and m not in members:
                members.append(m)
        members = members[:8]
        if len(members) < 2:
            continue
        name = str(t.get("name", "")).strip()[:40] or " & ".join(pname[m] for m in members)[:40]
        trackers.append({"id": new_id(t.get("id")), "name": name, "members": members,
                         "mode": t.get("mode") if t.get("mode") in TRACKER_MODES else "max",
                         "pair_sensors": bool(t.get("pair_sensors"))})

    b = body.get("settings") or {}
    s = dict(DEFAULT_SETTINGS)
    s["region"] = b.get("region") if b.get("region") in WAZE_URLS else "US"
    s["units"] = "metric" if b.get("units") == "metric" else "imperial"
    s["moving_interval"] = clamp(b.get("moving_interval"), 20, 3600, 60)
    s["idle_interval"] = clamp(b.get("idle_interval"), 60, 86400, 600)
    s["idle_interval"] = max(s["idle_interval"], s["moving_interval"])
    s["move_threshold_m"] = clamp(b.get("move_threshold_m"), 50, 5000, 250)
    s["arrival_radius_m"] = clamp(b.get("arrival_radius_m"), 25, 2000, 150)
    s["max_requests_per_min"] = clamp(b.get("max_requests_per_min"), 1, 60, 20)
    s["avoid_tolls"] = bool(b.get("avoid_tolls"))
    ai = str(b.get("ai_task_entity") or "").strip()
    s["ai_task_entity"] = ai if ENTITY_RE.match(ai) and ai.startswith("ai_task.") else ""
    s["backup_dir"] = safe_dir(str(b.get("backup_dir") or "").strip()) or DEFAULT_SETTINGS["backup_dir"]
    s["backup_auto"] = b.get("backup_auto") if b.get("backup_auto") in ("off", "daily", "weekly") else "off"
    s["backup_keep"] = clamp(b.get("backup_keep"), 1, 100, 10)
    return {"people": people, "destinations": dests, "routes": routes, "groups": groups,
            "trackers": trackers, "settings": s}


def apply_cfg(new):
    global cfg
    with lock:
        old = cfg
        old_v, new_v = pairs_of(old), pairs_of(new)
        for key in old_v.keys() - new_v.keys():
            pub(disc_topic(*key), "")  # empty payload = delete the entity
        old_a, new_a = all_pairs(old), all_pairs(new)
        for (pid, did) in old_a.keys() - new_a.keys():
            status.pop(f"{pid}|{did}", None)
        # anything whose source/target changed gets recalculated right away
        for key, (p, d) in new_a.items():
            if key in old_a:
                op, od = old_a[key]
                if op["entity"] != p["entity"] or od["target"] != d["target"]:
                    st = status.get(f"{key[0]}|{key[1]}", {})
                    st.pop("t", None)
                    st.pop("arrived", None)
        for key in group_keys(old) - group_keys(new):  # group, or one of its destinations, removed
            pub(group_disc_topic(key), "")
            group_cache.pop(key, None)
            group_live.pop(key, None)
        for topic in tracker_topics(old) - tracker_topics(new):
            pub(topic, "")  # tracker removed, or its per-pair sensors switched off / members changed
        for tid in {t["id"] for t in old.get("trackers", [])} - {t["id"] for t in new.get("trackers", [])}:
            tracker_cache.pop(tid, None)
            tracker_live.pop(tid, None)
        cfg = new
        save_json(CFG_FILE, new)
    republish_all()


# -------------------------------------------------------------- AI place search
def near_point(near):
    if near and near != "home" and ENTITY_RE.match(near):
        pt = ha_coords(near)
        if pt:
            return pt, "the person's current location"
    if _home_cache["pt"] and time.time() - _home_cache["t"] < 300:
        return _home_cache["pt"], "home"
    try:
        j = requests.get(f"{HA}/config", headers=HDR, timeout=8).json()
        pt = (float(j["latitude"]), float(j["longitude"]))
        _home_cache.update(t=time.time(), pt=pt)
        return pt, "home"
    except Exception:
        return None, ""


def parse_places(data):
    if not isinstance(data, (list, dict)):
        t = re.sub(r"^```(?:json)?|```$", "", str(data).strip(), flags=re.M).strip()
        m = re.search(r"\[.*\]", t, re.S) or re.search(r"\{.*\}", t, re.S)
        if not m:
            raise RuntimeError("The AI did not return any places")
        data = json.loads(m.group(0))
    if isinstance(data, dict):
        data = data.get("places") or data.get("results") or [data]
    return [x for x in data if isinstance(x, dict)]


def valid_pt(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    return (lat, lon) if -90 <= lat <= 90 and -180 <= lon <= 180 and (lat or lon) else None


def find_places(query, entity, near):
    origin, label = near_point(near)
    where = (f"The reference point is {label} at latitude {origin[0]:.5f}, longitude "
             f"{origin[1]:.5f}; prefer places close to it." if origin else "")
    instructions = (
        f"You are a place lookup tool. Find up to 5 real, currently operating places that best match "
        f"this request: {json.dumps(query)}. {where} Reply with ONLY a JSON array, no prose and no "
        f"markdown. Each item needs: \"name\", \"address\" (full street address with city and "
        f"state/country), \"latitude\" and \"longitude\" as numbers. Only include places you are "
        f"confident exist.")
    r = requests.post(f"{HA}/services/ai_task/generate_data", params={"return_response": "true"},
                      headers=HDR, timeout=180,
                      json={"entity_id": entity, "task_name": "travel_times_place_search",
                            "instructions": instructions})
    if r.status_code != 200:
        raise RuntimeError(f"AI Task call failed ({r.status_code}): {r.text[:150]}")
    j = r.json()
    sr = j.get("service_response", j) if isinstance(j, dict) else {}
    items = parse_places(sr.get("data"))
    out = []
    for it in items[:5]:
        name = str(it.get("name") or "").strip()
        addr = str(it.get("address") or "").strip()
        if not name:
            continue
        pt = geocode(addr) if addr else None   # a geocoder is more reliable than model-made coordinates
        pt = pt or valid_pt(it.get("latitude"), it.get("longitude"))
        if not pt:
            continue
        out.append({"name": name, "address": addr, "lat": round(pt[0], 6), "lon": round(pt[1], 6),
                    "dist_m": round(haversine(origin, pt)) if origin else None})
    if not out:
        raise RuntimeError("No places found. Try a more specific search.")
    return out


# ------------------------------------------------------------ who is an admin?
# Ingress tells us WHICH Home Assistant user opened the page (X-Remote-User-* headers). Whether that
# user is an admin comes from HA's own user list. Admins get every menu; everyone else gets Live only.
# This is enforced here on the server, not just hidden in the page, so a viewer can't call the API.
ADMIN_TTL = 60
_admin = {"t": 0.0, "users": None}


def ha_user_list():
    """All HA users (id, username, name, is_owner, group_ids) via the websocket API."""
    if websocket is None:
        raise RuntimeError("websocket-client is not installed")
    ws = websocket.create_connection(os.environ.get("TT_WS", "ws://supervisor/core/websocket"), timeout=8)
    try:
        ws.recv()  # auth_required
        ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(ws.recv()).get("type") != "auth_ok":
            raise RuntimeError("Home Assistant refused the add-on token")
        ws.send(json.dumps({"id": 1, "type": "config/auth/list"}))
        while True:
            m = json.loads(ws.recv())
            if m.get("id") == 1:
                if not m.get("success"):
                    raise RuntimeError(str(m.get("error"))[:100])
                return m.get("result") or []
    finally:
        try:
            ws.close()
        except Exception:
            pass


def admin_users():
    """{lowercase id / username / name -> True} for every active admin. Cached for a minute;
    if HA can't be asked, the last good answer is kept (and with none, nobody counts as admin)."""
    now = time.time()
    if _admin["users"] is not None and now - _admin["t"] < ADMIN_TTL:
        return _admin["users"]
    try:
        keys = set()
        for u in ha_user_list():
            if u.get("is_active", True) and not u.get("system_generated") and (
                    u.get("is_owner") or "system-admin" in (u.get("group_ids") or [])):
                keys.update(str(u[k]).lower() for k in ("id", "username", "name") if u.get(k))
        _admin.update(t=now, users=keys)
    except Exception as e:
        log.warning("Could not check admin users (%s). Viewers keep Live only.", e)
        _admin["t"] = now - ADMIN_TTL + 10  # retry in 10 s
        if _admin["users"] is None:
            _admin["users"] = set()
    return _admin["users"]


def is_admin():
    h = request.headers
    ident = [h.get(k) for k in ("X-Remote-User-Id", "X-Remote-User-Name", "X-Remote-User")]
    ident = [str(x).strip().lower() for x in ident if x and str(x).strip()]
    if not ident:
        return False  # no user attached to the request: fail closed
    admins = admin_users()
    return any(i in admins for i in ident)


# ----------------------------------------------------------- dashboard card
# The card (app/card/travel-times-card.js) is copied into /config/www so Home Assistant serves it at
# /local/..., then registered as a dashboard resource through the websocket API.
CONFIG_DIR = os.environ.get("TT_CONFIG", "/config")
CARD_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "card", "travel-times-card.js")
CARD_DIR = os.path.join(CONFIG_DIR, "www", "travel-times-card")
CARD_DEST = os.path.join(CARD_DIR, "travel-times-card.js")


def card_version(path):
    try:
        with open(path, encoding="utf-8") as f:
            m = re.search(r'TT_CARD_VERSION\s*=\s*"([^"]+)"', f.read(4000))
        return m.group(1) if m else None
    except OSError:
        return None


def card_url():
    return f"/local/travel-times-card/travel-times-card.js?v={card_version(CARD_SRC) or '1'}"


def ha_ws_call(msg_type, **extra):
    """One websocket command to Home Assistant; returns the raw reply message."""
    if websocket is None:
        raise RuntimeError("websocket-client is not installed")
    ws = websocket.create_connection(os.environ.get("TT_WS", "ws://supervisor/core/websocket"), timeout=10)
    try:
        ws.recv()
        ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(ws.recv()).get("type") != "auth_ok":
            raise RuntimeError("Home Assistant refused the add-on token")
        ws.send(json.dumps({"id": 1, "type": msg_type, **extra}))
        while True:
            m = json.loads(ws.recv())
            if m.get("id") == 1:
                return m
    finally:
        try:
            ws.close()
        except Exception:
            pass


def register_card_resource(url):
    """(ok, message). Adds or updates the dashboard resource so the card shows up in the picker."""
    try:
        res = ha_ws_call("lovelace/resources")
        if not res.get("success"):
            return False, "Home Assistant would not list dashboard resources"
        mine = [r for r in (res.get("result") or []) if "travel-times-card" in str(r.get("url", ""))]
        if mine and mine[0].get("url") == url:
            return True, "Resource already registered"
        if mine:
            r = ha_ws_call("lovelace/resources/update", resource_id=mine[0]["id"], res_type="module", url=url)
        else:
            r = ha_ws_call("lovelace/resources/create", res_type="module", url=url)
        if r.get("success"):
            return True, "Resource registered"
        return False, str((r.get("error") or {}).get("message", "Home Assistant refused"))[:150]
    except Exception as e:
        return False, str(e)[:150]


def card_status():
    return {"available": card_version(CARD_SRC), "installed": card_version(CARD_DEST),
            "config_mapped": os.path.isdir(CONFIG_DIR), "url": card_url().split("?")[0],
            "www_exists": os.path.isdir(os.path.join(CONFIG_DIR, "www"))}


def install_card():
    if not os.path.isdir(CONFIG_DIR):
        raise RuntimeError("The /config folder is not available to the add-on. Update to the latest version and restart it.")
    www_new = not os.path.isdir(os.path.join(CONFIG_DIR, "www"))
    os.makedirs(CARD_DIR, exist_ok=True)
    with open(CARD_SRC, "rb") as f:
        data = f.read()
    with open(CARD_DEST, "wb") as f:
        f.write(data)
    ok, msg = register_card_resource(card_url())
    return {"ok": True, "registered": ok, "message": msg, "restart_needed": www_new, "url": card_url(),
            "version": card_version(CARD_DEST)}


def refresh_card_if_installed():
    """Keep an installed card in step with the add-on (called at startup)."""
    try:
        if os.path.isfile(CARD_DEST) and card_version(CARD_DEST) != card_version(CARD_SRC):
            with open(CARD_SRC, "rb") as f:
                data = f.read()
            with open(CARD_DEST, "wb") as f:
                f.write(data)
            ok, msg = register_card_resource(card_url())
            log.info("Dashboard card updated to %s (%s)", card_version(CARD_SRC), msg)
    except Exception as e:
        log.warning("Could not update the dashboard card: %s", e)


# ------------------------------------------------------------------ Flask UI
app = Flask(__name__)
VIEWER_OK = {"/", "/api/config", "/api/status"}


@app.before_request
def admin_gate():
    if request.method == "GET" and request.path in VIEWER_OK:
        return None
    if is_admin():
        return None
    return jsonify(error="Admin access required"), 403


@app.after_request
def nocache(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp


def status_public():
    with lock:
        out = {k: {x: v.get(x) for x in ("minutes", "distance", "unit", "t", "error", "arrived", "via")}
               for k, v in status.items()}
        for gid, v in group_live.items():
            out[f"g|{gid}"] = v
        for tid, v in tracker_live.items():
            out[f"t|{tid}"] = v
        return out


@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")


def viewer_config(c):
    """What a non-admin may see on the Live page: names and structure only. No entity ids, addresses,
    coordinates or settings."""
    names = lambda L: [{"id": x["id"], "name": x["name"]} for x in L]
    return {"people": names(c["people"]), "destinations": names(c["destinations"]),
            "routes": c["routes"], "groups": c["groups"],
            "trackers": [{k: t[k] for k in ("id", "name", "members", "mode")} for t in c["trackers"]],
            "settings": {"units": c["settings"]["units"]}}


@app.get("/api/config")
def api_get():
    if not is_admin():
        with lock:
            c = viewer_config(json.loads(json.dumps(cfg)))
        return jsonify(config=c, status=status_public(), trackers=[], zones=[], ai_tasks=[],
                       info={}, admin=False)
    trackers, zones, ais = list_entities()
    with lock:
        c = json.loads(json.dumps(cfg))
    mq_host = (OPTIONS.get("mqtt_host") or "").strip()
    info = {"mqtt": mq_host or "Mosquitto (automatic)", "log_level": str(OPTIONS.get("log_level", "info"))}
    return jsonify(config=c, status=status_public(), trackers=trackers, zones=zones, ai_tasks=ais, info=info, admin=True)


@app.get("/api/status")
def api_status():
    return jsonify(status_public())


@app.post("/api/config")
def api_set():
    apply_cfg(clean_cfg(request.get_json(force=True, silent=True) or {}))
    return jsonify(ok=True)


@app.post("/api/search")
def api_search():
    b = request.get_json(force=True, silent=True) or {}
    q, ent = str(b.get("query", "")).strip()[:200], str(b.get("entity", "")).strip()
    if not q:
        return jsonify(error="Type something to search for"), 400
    if not (ENTITY_RE.match(ent) and ent.startswith("ai_task.")):
        return jsonify(error="Pick an AI Task entity in Settings first"), 400
    try:
        return jsonify(results=find_places(q, ent, str(b.get("near") or "home")))
    except Exception as e:
        log.warning("Place search failed: %s", e)
        return jsonify(error=str(e)[:300]), 502


@app.get("/api/geosearch")
def api_geosearch():
    """Fast address / place search (no AI). quick=1 = single provider, for search-as-you-type."""
    q = request.args.get("q", "").strip()[:200]
    if len(q) < 2:
        return jsonify(results=[], errors=[])
    near, _ = near_point(request.args.get("near") or "home")
    try:
        res, errs = search_address(q, near, quick=request.args.get("quick") == "1")
    except Exception as e:
        log.warning("Address search failed: %s", e)
        return jsonify(results=[], errors=[str(e)[:120]])
    return jsonify(results=res, errors=errs)


@app.get("/api/browse")
def api_browse():
    path = request.args.get("path", "")
    real = safe_dir(path)
    while real and not os.path.isdir(real):  # default folder may not exist yet: show nearest parent
        real = safe_dir(os.path.dirname(real))
    if not real:
        return jsonify(path=None, parent=None, dirs=[
            {"name": ROOT_LABELS.get(r, r), "path": r} for r in backup_roots()])
    dirs = []
    try:
        for e in sorted(os.scandir(real), key=lambda e: e.name.lower()):
            if e.is_dir(follow_symlinks=False) and not e.name.startswith("."):
                dirs.append({"name": e.name, "path": e.path})
    except OSError as e:
        return jsonify(error=str(e)), 400
    parent = "" if real in backup_roots() else os.path.dirname(real)
    return jsonify(path=real, parent=parent, dirs=dirs)


@app.post("/api/mkdir")
def api_mkdir():
    b = request.get_json(force=True, silent=True) or {}
    real = safe_dir(b.get("path"))
    name = re.sub(r"[^\w\- .]", "", str(b.get("name", ""))).strip()[:60]
    if not real or not name or name.startswith("."):
        return jsonify(error="Pick a folder and type a simple name"), 400
    try:
        os.makedirs(os.path.join(real, name), exist_ok=True)
    except OSError as e:
        return jsonify(error=str(e)), 400
    return jsonify(ok=True, path=os.path.join(real, name))


def backup_dir_now():
    with lock:
        return cfg["settings"]["backup_dir"], cfg["settings"]["backup_keep"]


@app.get("/api/backups")
def api_backups():
    d, keep = backup_dir_now()
    return jsonify(dir=d, files=list_backups(safe_dir(d) or d))


@app.post("/api/backup")
def api_backup():
    d, keep = backup_dir_now()
    try:
        name = make_backup(d, keep)
    except Exception as e:
        return jsonify(error=str(e)[:200]), 400
    return jsonify(ok=True, name=name)


def backup_path(name):
    d, _ = backup_dir_now()
    real = safe_dir(d)
    if not real or not BACKUP_RE.match(str(name or "")):
        return None
    p = os.path.join(real, name)
    return p if os.path.isfile(p) else None


@app.post("/api/backup/restore")
def api_restore():
    p = backup_path((request.get_json(force=True, silent=True) or {}).get("name"))
    if not p:
        return jsonify(error="Backup not found"), 404
    data = load_json(p, {})
    if not isinstance(data.get("config"), dict):
        return jsonify(error="That file is not a Travel Times backup"), 400
    new = clean_cfg(data["config"])
    with lock:
        keep = {k: cfg["settings"][k] for k in ("backup_dir", "backup_auto", "backup_keep")}
    new["settings"].update(keep)
    apply_cfg(new)
    return jsonify(ok=True, people=len(new["people"]), places=len(new["destinations"]), routes=len(new["routes"]))


@app.post("/api/backup/delete")
def api_backup_delete():
    p = backup_path((request.get_json(force=True, silent=True) or {}).get("name"))
    if not p:
        return jsonify(error="Backup not found"), 404
    os.remove(p)
    return jsonify(ok=True)


@app.get("/api/backup/download")
def api_backup_download():
    p = backup_path(request.args.get("name"))
    if not p:
        return jsonify(error="Backup not found"), 404
    with open(p, "rb") as f:
        body = f.read()
    return Response(body, mimetype="application/json",
                    headers={"Content-Disposition": f"attachment; filename={os.path.basename(p)}"})


@app.get("/api/card")
def api_card():
    return jsonify(card_status())


@app.post("/api/card/install")
def api_card_install():
    try:
        return jsonify(install_card())
    except Exception as e:
        return jsonify(error=str(e)[:200]), 400


@app.post("/api/refresh")
def api_refresh():
    with lock:
        for st in status.values():
            st.pop("t", None)
            st.pop("retry_at", None)
    return jsonify(ok=True)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Travel Times</title>
<style>
:root{--tx:#e8e8f0;--mu:#8a8aa0;--ac:#03a9f4;--bad:#ff5252;--ok:#4caf50;--line:#2a2a44;--card:rgba(255,255,255,.05)}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;font:16px system-ui,sans-serif;color:var(--tx);background:linear-gradient(160deg,#0d0d0d,#1a1a2e) fixed;min-height:100vh}
.top{position:sticky;top:0;z-index:5;display:flex;justify-content:space-between;align-items:center;padding:12px 16px;background:#0d0d0df2;border-bottom:1px solid var(--line)}
.top h1{font-size:20px;margin:0}
main{padding:14px 14px calc(150px + env(safe-area-inset-bottom))}
.pg.hid,.hid{display:none!important}
h2{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--mu);margin:22px 0 8px}
h2:first-child{margin-top:4px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px;margin-bottom:10px}
.live{display:flex;justify-content:space-between;align-items:center;gap:10px}
.big{font-size:26px;font-weight:700;color:var(--ac);white-space:nowrap}
.sub{color:var(--mu);font-size:13px;margin-top:2px;word-break:break-word}
.err{color:var(--bad)}.ok{color:var(--ok)}
input,select{width:100%;min-height:46px;background:#12121f;color:var(--tx);border:1px solid var(--line);border-radius:10px;padding:10px;font:inherit;min-width:0}
input[type=checkbox]{width:22px;min-height:22px;height:22px}
button{min-height:44px;background:#23233a;color:var(--tx);border:1px solid var(--line);border-radius:10px;padding:8px 14px;font:inherit;cursor:pointer}
button.p{background:var(--ac);border-color:var(--ac);color:#04121c;font-weight:700}
button.d{color:var(--bad)}button.ico{padding:8px 12px;font-size:18px}button:disabled{opacity:.5}
.stack{display:flex;flex-direction:column;gap:8px}.hrow{display:flex;gap:8px;align-items:center}.hrow>*{flex:1}.hrow>button{flex:0 0 auto}
.field{margin-bottom:12px}.field:last-child{margin-bottom:0}
label.l{display:block;font-size:13px;color:var(--mu);margin-bottom:4px}
.hint{font-size:12px;color:var(--mu);margin-top:4px}
.seg{display:flex;background:#12121f;border:1px solid var(--line);border-radius:12px;padding:3px;margin-bottom:12px;overflow-x:auto;scrollbar-width:none}
.seg button{flex:1 0 auto;border:0;background:transparent;min-height:40px;padding:6px 10px;font-size:14px;white-space:nowrap}.seg button.on{background:var(--ac);color:#04121c;font-weight:700}
.pairs{margin-top:6px;font-size:13px;color:var(--mu);line-height:1.5}.pairs b{color:var(--tx);font-weight:600}
details.acc{background:var(--card);border:1px solid var(--line);border-radius:14px;margin-bottom:8px}
details.acc>summary{list-style:none;display:flex;align-items:center;gap:10px;padding:0 14px;min-height:52px;cursor:pointer}
details.acc>summary::-webkit-details-marker{display:none}
details.acc>summary::after{content:'›';color:var(--mu);font-size:20px;transition:transform .15s}
details.acc[open]>summary::after{transform:rotate(90deg)}
details.acc[open]>summary{border-bottom:1px solid var(--line)}
.at{font-weight:600;white-space:nowrap}.as{color:var(--mu);font-size:13px;margin-left:auto;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
details.acc>.body{padding:14px}
.acc .field{margin:0 0 14px}.acc .field:last-child{margin:0}
.tg{min-height:38px;border-radius:20px;padding:4px 14px;font-size:14px;background:#12121f;color:var(--mu)}
.tg.on{background:#0b3a52;border-color:var(--ac);color:var(--tx)}
.pname{display:flex;flex-direction:column;align-items:flex-start;text-align:left;gap:2px;flex:1;min-height:52px;background:#0b3a52;border-color:var(--ac)}
.pname b{font-size:17px}
.sublab{font-size:12px;color:var(--mu);margin:12px 0 2px}
.viewer main{padding-bottom:24px}
.src{font-size:11px;color:var(--mu);border:1px solid var(--line);border-radius:8px;padding:1px 6px;margin-left:6px;white-space:nowrap}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0}
.chip{display:flex;align-items:center;gap:6px;padding:6px 10px;border:1px solid var(--line);border-radius:20px;font-size:14px}
.nav{position:fixed;left:0;right:0;bottom:0;z-index:6;display:flex;background:#0d0d0dfa;border-top:1px solid var(--line);padding-bottom:env(safe-area-inset-bottom)}
.nav button{flex:1;border:0;border-radius:0;background:transparent;display:flex;flex-direction:column;align-items:center;gap:2px;font-size:11px;color:var(--mu);min-height:58px}
.nav button span{font-size:21px;line-height:1}.nav button.on{color:var(--ac)}
.savebar{position:fixed;left:12px;right:12px;bottom:calc(68px + env(safe-area-inset-bottom));z-index:7;display:flex;align-items:center;justify-content:space-between;gap:10px;background:#1f2a3a;border:1px solid var(--ac);border-radius:14px;padding:8px 8px 8px 14px}
.toast{position:fixed;left:50%;transform:translateX(-50%);bottom:calc(130px + env(safe-area-inset-bottom));z-index:9;background:#2b2b45;border:1px solid var(--line);border-radius:20px;padding:10px 16px;max-width:90%}
.sheet{position:fixed;inset:0;z-index:10;background:#000a;display:none;align-items:flex-end}.sheet.open{display:flex}
.sheet>div{width:100%;max-height:85vh;overflow:auto;background:#161626;border-radius:18px 18px 0 0;padding:14px 14px calc(14px + env(safe-area-inset-bottom))}
.item{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:12px 4px;border-bottom:1px solid var(--line);cursor:pointer;word-break:break-word}
@media(min-width:700px){main{max-width:640px;margin:0 auto}}
</style></head><body>
<div class="top"><h1 id="title">Live</h1><button id="refbtn" class="ico" onclick="refreshNow()" aria-label="Refresh">⟳</button></div>
<main>
<section id="p-live" class="pg"><div id="live"><div class="sub">Loading…</div></div></section>
<section id="p-setup" class="pg hid">
 <div class="seg" id="seg"></div>
 <div id="s-people"></div><div id="s-places" class="hid"></div><div id="s-routes" class="hid"></div><div id="s-groups" class="hid"></div><div id="s-trackers" class="hid"></div>
</section>
<section id="p-find" class="pg hid">
 <div class="seg" id="fseg"></div>
 <div class="card stack">
  <input id="q" autocomplete="off" enterkeyhint="search" oninput="typeahead()" onkeydown="if(event.key==='Enter')doSearch()">
  <select id="near"></select>
  <div><div class="sub">Also add routes from:</div><div id="sopts" class="chips"></div></div>
  <button class="p" onclick="doSearch()">Search</button>
 </div>
 <div id="sres"></div>
</section>
<section id="p-settings" class="pg hid"><div id="settings"></div></section>
<section id="p-backup" class="pg hid"><div id="backup"></div></section>
</main>
<datalist id="dl"></datalist>
<div id="savebar" class="savebar hid"><span>Unsaved changes</span><button class="p" onclick="save()">Save</button></div>
<div id="toast" class="toast hid"></div>
<nav class="nav" id="nav"></nav>
<div id="sheet" class="sheet" onclick="if(event.target===this)closeSheet()"><div id="sheetbody"></div></div>
<script>
let S=null,SR=[],BR=null,BK=null,page='live',sub='people',dirty=false,fmode='quick',qseq=0,qtimer=null;
const OFF=new Set();const $=s=>document.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const rid=()=>{const a=new Uint8Array(4);if(window.crypto&&crypto.getRandomValues)crypto.getRandomValues(a);else a.forEach((_,i)=>a[i]=Math.random()*256);return Array.from(a,b=>b.toString(16).padStart(2,'0')).join('')};
const nameOf=id=>{const p=S.config.people.find(x=>x.id===id);if(p)return p.name||'(unnamed)';const d=S.config.destinations.find(x=>x.id===id);return d?(d.name||'(unnamed)'):'?'};
const J=(url,body)=>fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})}).then(r=>r.json());
function toast(t,ms=2600){const e=$('#toast');e.textContent=t;e.classList.remove('hid');clearTimeout(toast.t);toast.t=setTimeout(()=>e.classList.add('hid'),ms)}
function touch(){dirty=true;$('#savebar').classList.remove('hid')}
const PAGES=[['live','🏠','Live'],['setup','🧭','Setup'],['find','🔎','Find'],['settings','⚙️','Settings'],['backup','💾','Backup']];
$('#nav').innerHTML=PAGES.map(([k,i,l])=>`<button id="n-${k}" onclick="go('${k}')"><span>${i}</span>${l}</button>`).join('');
function go(k){
  if(S&&!S.admin)k='live';
  page=k;try{sessionStorage.setItem('tt_page',k)}catch(e){}
  PAGES.forEach(([x,,l])=>{$('#p-'+x).classList.toggle('hid',x!==k);$('#n-'+x).classList.toggle('on',x===k);if(x===k)$('#title').textContent=l});
  if(k==='backup')loadBackups();window.scrollTo(0,0);
}
async function load(){
  const r=await fetch('api/config');S=await r.json();S.config.routes=S.config.routes||[];S.config.groups=S.config.groups||[];S.config.trackers=S.config.trackers||[];
  $('#dl').innerHTML=[...S.zones,...S.trackers].map(o=>`<option value="${esc(o.id)}">${esc(o.name)}</option>`).join('');
  dirty=false;$('#savebar').classList.add('hid');renderAll();
}
/* viewers (non-admin HA users) only get the Live page; the server enforces it too */
function applyRole(){
  const a=!!S.admin;
  $('#nav').classList.toggle('hid',!a);$('#refbtn').classList.toggle('hid',!a);document.body.classList.toggle('viewer',!a);
  if(!a){$('#savebar').classList.add('hid');go('live');$('#title').textContent='Travel Times'}
}
function renderAll(){
  applyRole();if(!S.admin){renderLive(S.status);return}
  renderSeg();renderPeople();renderDests();renderRoutes();renderGroups();renderTrackers();renderFind();renderSearchOpts();renderSettings();renderBackup();renderLive(S.status)}
function dropRoutes(id){
  S.config.routes=S.config.routes.filter(r=>r.from!==id&&r.to!==id);
  S.config.groups.forEach(g=>{g.members=g.members.filter(m=>m!==id);g.destinations=(g.destinations||[]).filter(x=>x!==id)});
  S.config.groups=S.config.groups.filter(g=>g.destinations.length>=1&&g.members.length>=2);
  S.config.trackers.forEach(t=>{t.members=t.members.filter(m=>m!==id)});
  S.config.trackers=S.config.trackers.filter(t=>t.members.length>=2);
}
function renderSeg(){
  const C=S.config,t=[['people','People',C.people.length],['places','Places',C.destinations.length],['routes','Routes',C.routes.length],['groups','Groups',C.groups.length],['trackers','Trackers',C.trackers.length]];
  $('#seg').innerHTML=t.map(([k,l,n])=>`<button class="${sub===k?'on':''}" onclick="setSub('${k}')">${l} (${n})</button>`).join('');
  t.forEach(([k])=>$('#s-'+k).classList.toggle('hid',k!==sub));
  const on=$('#seg .on');if(on&&on.scrollIntoView)on.scrollIntoView({block:'nearest',inline:'center'});
}
function setSub(k){sub=k;renderSeg()}
function card(){const c=document.createElement('div');c.className='card stack';return c}
const OPEN=new Set();
const gkey=(g,d)=>d===g.legacy?g.id:g.id+'_'+d;
function body(){const b=document.createElement('div');b.className='stack';return b}
function acc(key,title,sub,bd){
  const d=document.createElement('details');d.className='acc';d.open=OPEN.has(key);
  d.addEventListener('toggle',()=>{d.open?OPEN.add(key):OPEN.delete(key)});
  const s=document.createElement('summary'),t=document.createElement('span'),u=document.createElement('span');
  t.className='at';t.textContent=title;u.className='as';u.textContent=sub||'';s.append(t,u);
  bd.classList.add('body');d.append(s,bd);d.setT=(a,b)=>{t.textContent=a;u.textContent=b||''};return d;
}
function tgl(label,on,fn){const b=document.createElement('button');b.type='button';b.className='tg'+(on?' on':'');b.textContent=label;b.onclick=fn;return b}
function lab(t){const d=document.createElement('div');d.className='sublab';d.textContent=t;return d}
function chipset(items,arr,after){
  const w=document.createElement('div');w.className='chips';w.style.margin='6px 0 0';
  items.forEach(([id,label])=>{
    const b=tgl(label,arr.includes(id),()=>{const k=arr.indexOf(id);k<0?arr.push(id):arr.splice(k,1);b.classList.toggle('on',k<0);touch();after&&after()});
    w.append(b);
  });
  if(!items.length)w.innerHTML='<span class="sub">Nothing to pick yet</span>';
  return w;
}
const pItems=()=>S.config.people.map(p=>[p.id,'👤 '+(p.name||'(unnamed)')]);
const dItems=()=>S.config.destinations.map(d=>[d.id,'📍 '+(d.name||'(unnamed)')]);
function addBtn(text,fn){const b=document.createElement('button');b.className='p';b.textContent=text;b.onclick=fn;b.style.width='100%';return b}
function hint(el,t){el.insertAdjacentHTML('beforeend',`<div class="hint" style="margin:6px 4px 0">${t}</div>`)}
function refreshLists(){renderRoutes();renderGroups();renderTrackers();renderSearchOpts();renderLive(S.status);renderSeg()}
function renderPeople(){
  const el=$('#s-people');el.innerHTML='';
  S.config.people.forEach((p,i)=>{
    const b=body(),sub=()=>p.entity||'pick a location source',ttl=()=>p.name||'New person';
    const nm=document.createElement('input');nm.placeholder='Name (Jon)';nm.value=p.name||'';
    const sel=document.createElement('select');
    const opts=[...S.trackers];if(p.entity&&!opts.find(o=>o.id===p.entity))opts.unshift({id:p.entity,name:p.entity});
    sel.innerHTML='<option value="">Pick a location source…</option>'+opts.map(o=>`<option value="${esc(o.id)}">${esc(o.name)} (${esc(o.id)})</option>`).join('');
    sel.value=p.entity||'';
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove person';rm.onclick=()=>{dropRoutes(p.id);S.config.people.splice(i,1);touch();renderAll()};
    b.append(lab('Name'),nm,lab('Location source'),sel,rm);
    const a=acc('p'+p.id,ttl(),sub(),b);
    nm.oninput=()=>{p.name=nm.value;a.setT(ttl(),sub());touch();refreshLists()};
    sel.onchange=()=>{p.entity=sel.value;a.setT(ttl(),sub());touch();renderSearchOpts()};
    el.append(a);
  });
  el.append(addBtn('+ Add person',()=>{const id=rid();S.config.people.push({id,name:'',entity:''});OPEN.add('p'+id);touch();renderAll()}));
}
function renderDests(){
  const el=$('#s-places');el.innerHTML='';
  S.config.destinations.forEach((d,i)=>{
    const b=body(),ttl=()=>d.name||'New place';
    const nm=document.createElement('input');nm.placeholder='Name (Work)';nm.value=d.name||'';
    const tg=document.createElement('input');tg.placeholder='zone.work, 34.5,-82.6, or address';tg.setAttribute('list','dl');tg.value=d.target||'';
    const sb=document.createElement('button');sb.className='ico';sb.textContent='🔎';sb.setAttribute('aria-label','Search address');
    sb.onclick=()=>openAddrSheet(x=>{d.target=`${x.lat}, ${x.lon}`;if(!(d.name||'').trim())d.name=(x.name||'').slice(0,40);OPEN.add('d'+d.id);touch();renderDests();refreshLists()});
    const row=document.createElement('div');row.className='hrow';row.append(tg,sb);
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove place';rm.onclick=()=>{dropRoutes(d.id);S.config.destinations.splice(i,1);touch();renderAll()};
    b.append(lab('Name'),nm,lab('Where is it? Zone, address or "lat, lon"'),row,rm);
    const a=acc('d'+d.id,ttl(),d.target||'no location yet',b);
    nm.oninput=()=>{d.name=nm.value;a.setT(ttl(),d.target||'');touch();refreshLists()};
    tg.oninput=()=>{d.target=tg.value;a.setT(ttl(),d.target||'');touch()};
    el.append(a);
  });
  el.append(addBtn('+ Add place',()=>{const id=rid();S.config.destinations.push({id,name:'',target:''});OPEN.add('d'+id);touch();renderAll()}));
  hint(el,'Tap 🔎 to search an address, or use the Find tab.');
}
function renderRoutes(){
  const el=$('#s-routes');el.innerHTML='';
  const P=S.config.people,D=S.config.destinations,R=()=>S.config.routes;
  if(!P.length){hint(el,'Add people first (Setup › People).');return}
  const top=document.createElement('button');top.textContent='Everyone → all places';top.onclick=fillAll;top.style.cssText='width:100%;margin-bottom:10px';
  if(D.length)el.append(top);
  P.forEach(p=>{
    const has=id=>R().some(r=>r.from===p.id&&r.to===id),mine=()=>R().filter(r=>r.from===p.id).length;
    const c=document.createElement('div');c.className='card';
    const head=document.createElement('div');head.className='hrow';
    const nb=document.createElement('button');nb.className='pname';
    nb.innerHTML=`<b>${esc(p.name||'(unnamed)')}</b><span class="sub">Tap to track all places · ${mine()} route${mine()===1?'':'s'}</span>`;
    nb.onclick=()=>{
      let n=0;D.forEach(d=>{if(!has(d.id)){R().push({from:p.id,to:d.id});n++}});
      touch();renderRoutes();renderSeg();renderLive(S.status);
      toast(!D.length?'Add places first':n?`Added ${n} route${n>1?'s':''} for ${p.name||'this person'}`:'Already tracking every place');
    };
    head.append(nb);
    if(mine()){const clr=document.createElement('button');clr.className='d';clr.textContent='Clear';clr.onclick=()=>{S.config.routes=R().filter(r=>r.from!==p.id);touch();renderRoutes();renderSeg();renderLive(S.status)};head.append(clr)}
    c.append(head);
    const flip=id=>()=>{if(has(id))S.config.routes=R().filter(r=>!(r.from===p.id&&r.to===id));else R().push({from:p.id,to:id});touch();renderRoutes();renderSeg();renderLive(S.status)};
    if(D.length){c.append(lab('Places'));const w=document.createElement('div');w.className='chips';w.style.margin='6px 0 0';D.forEach(d=>w.append(tgl('📍 '+(d.name||'(unnamed)'),has(d.id),flip(d.id))));c.append(w)}
    const others=P.filter(o=>o.id!==p.id);
    if(others.length){c.append(lab('Follow a person live'));const w=document.createElement('div');w.className='chips';w.style.margin='6px 0 0';others.forEach(o=>w.append(tgl('👤 '+(o.name||'(unnamed)'),has(o.id),flip(o.id))));c.append(w)}
    el.append(c);
  });
  hint(el,'Each highlighted chip is one sensor. Tap a name to track every place for that person, tap a chip to turn one on or off.');
}
const MODES=[['max','Longest: when everyone has arrived'],['min','Shortest: first one there'],['sum','Combined total of everyone']];
function renderGroups(){
  const el=$('#s-groups');el.innerHTML='';
  S.config.groups.forEach((g,i)=>{
    g.destinations=g.destinations||[];
    const b=body(),ttl=()=>g.name||'New group';
    const sub=()=>`${g.members.length} people → ${g.destinations.length} place${g.destinations.length===1?'':'s'}`;
    const nm=document.createElement('input');nm.placeholder='Group name (Jon & Sam)';nm.value=g.name||'';
    const md=selCtl(MODES,g.mode||'max',v=>{g.mode=v;touch()});
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove group';rm.onclick=()=>{S.config.groups.splice(i,1);touch();renderGroups();renderSeg();renderLive(S.status)};
    const a=acc('g'+g.id,ttl(),sub(),b);
    const after=()=>{a.setT(ttl(),sub());renderLive(S.status)};
    b.append(lab('Name'),nm,lab('People (2 or more)'),chipset(pItems(),g.members,after),
      lab('Where to: pick as many as you like, one sensor each'),chipset([...dItems(),...pItems().map(([id,l])=>[id,l+' (live)'])],g.destinations,after),
      lab('Sensor shows'),md,rm);
    nm.oninput=()=>{g.name=nm.value;a.setT(ttl(),sub());touch();renderLive(S.status)};
    el.append(a);
  });
  el.append(addBtn('+ Add group',()=>{const id=rid();S.config.groups.push({id,name:'',members:S.config.people.slice(0,2).map(p=>p.id),destinations:[],mode:'max'});OPEN.add('g'+id);touch();renderGroups();renderSeg()}));
  hint(el,'One group can go to several places: every place you pick gets its own sensor (for example "Family to Home" and "Family to Airport"), so there is no need to make the group twice.');
}
const TMODES=[['max','Show farthest pair'],['min','Show closest pair'],['avg','Show average of pairs']];
const nPairs=n=>n*(n-1)/2;
function renderTrackers(){
  const el=$('#s-trackers');el.innerHTML='';
  S.config.trackers.forEach((t,i)=>{
    const b=body(),ttl=()=>t.name||(t.members.map(nameOf).join(' & ')||'New tracker');
    const sub=()=>{const n=t.members.length;return n<2?'pick 2+ people':`${n} people · ${nPairs(n)} pair${nPairs(n)>1?'s':''}`};
    const nm=document.createElement('input');nm.placeholder='Name (blank = use the names)';nm.value=t.name||'';
    const md=selCtl(TMODES,t.mode||'max',v=>{t.mode=v;touch();renderLive(S.status)});
    const pairs=tgl('Also make a sensor for each pair',!!t.pair_sensors,()=>{t.pair_sensors=!t.pair_sensors;pairs.classList.toggle('on',t.pair_sensors);touch()});
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove tracker';rm.onclick=()=>{S.config.trackers.splice(i,1);touch();renderTrackers();renderSeg();renderLive(S.status)};
    const a=acc('t'+t.id,ttl(),sub(),b);
    b.append(lab('Name'),nm,lab('People to track (2 or more, every pair is tracked)'),chipset(pItems(),t.members,()=>{a.setT(ttl(),sub());renderLive(S.status)}),lab('Sensors report'),md,lab('Extras'),pairs,rm);
    nm.oninput=()=>{t.name=nm.value;a.setT(ttl(),sub());touch();renderLive(S.status)};
    el.append(a);
  });
  el.append(addBtn('+ Add tracker',()=>{const id=rid();S.config.trackers.push({id,name:'',members:S.config.people.slice(0,2).map(p=>p.id),mode:'max',pair_sensors:false});OPEN.add('t'+id);touch();renderTrackers();renderSeg()}));
  hint(el,'A tracker shows how far people are from each other. 2 people = one distance; a bigger group tracks every pair. You get a drive-time sensor and a distance sensor.');
}
function fillAll(){
  for(const p of S.config.people)for(const d of S.config.destinations)
    if(!S.config.routes.some(r=>r.from===p.id&&r.to===d.id))S.config.routes.push({from:p.id,to:d.id});
  touch();renderRoutes();renderSeg();renderLive(S.status);
}
function field(label,ctrl,hint){
  const w=document.createElement('div');w.className='field';w.innerHTML=`<label class="l">${label}</label>`;w.append(ctrl);
  if(hint)w.insertAdjacentHTML('beforeend',`<div class="hint">${hint}</div>`);return w;
}
function selCtl(opts,val,on){const s=document.createElement('select');s.innerHTML=opts.map(o=>Array.isArray(o)?`<option value="${esc(o[0])}">${esc(o[1])}</option>`:`<option>${esc(o)}</option>`).join('');s.value=val;s.onchange=()=>on(s.value);return s}
function numCtl(val,on){const i=document.createElement('input');i.type='number';i.inputMode='numeric';i.value=val;i.oninput=()=>on(i.value);return i}
function group(title,fields){const b=body();fields.forEach(f=>b.append(f));return acc('set-'+title,title,'',b)}
function renderSettings(){
  const el=$('#settings');el.innerHTML='';const s=S.config.settings;
  const set=k=>v=>{s[k]=v;touch()};
  const chk=document.createElement('input');chk.type='checkbox';chk.checked=!!s.avoid_tolls;chk.onchange=()=>{s.avoid_tolls=chk.checked;touch()};
  el.append(group('Waze',[
    field('Region',selCtl(['US','EU','AU','IL'],s.region,set('region')),'US covers North America'),
    field('Distance units',selCtl([['imperial','Miles'],['metric','Kilometers']],s.units,set('units')),'Only affects the distance attribute. Time is always minutes'),
    field('Avoid toll roads',chk)]));
  el.append(group('Update speed',[
    field('While moving: update every (sec)',numCtl(s.moving_interval,set('moving_interval')),'Minimum 20. This is your "real-time" speed'),
    field('While parked: update every (sec)',numCtl(s.idle_interval,set('idle_interval')),'Used when nobody is going anywhere'),
    field('Counts as moving after (m)',numCtl(s.move_threshold_m,set('move_threshold_m')),'Distance moved since the last calculation')]));
  el.append(group('Arrival & limits',[
    field('Arrived within (m)',numCtl(s.arrival_radius_m,set('arrival_radius_m')),'Inside this the sensor is 0 and no Waze call is made'),
    field('Max Waze calls per minute',numCtl(s.max_requests_per_min,set('max_requests_per_min')),'Safety cap for all routes combined')]));
  let ai;
  if(S.ai_tasks.length){
    const opts=[...S.ai_tasks];if(s.ai_task_entity&&!opts.find(o=>o.id===s.ai_task_entity))opts.unshift({id:s.ai_task_entity,name:s.ai_task_entity});
    ai=selCtl([['','None (place search off)'],...opts.map(o=>[o.id,`${o.name} (${o.id})`])],s.ai_task_entity||'',set('ai_task_entity'));
  }else{ai=document.createElement('input');ai.placeholder='ai_task.your_entity';ai.value=s.ai_task_entity||'';ai.oninput=()=>{s.ai_task_entity=ai.value.trim();touch()}}
  el.append(group('AI place search',[field('AI Task entity',ai,S.ai_tasks.length?'Used by the Find tab':'No ai_task entities found. Add an AI Task provider (Google, OpenAI, Ollama…) or type the entity id')]));
  const ib=body();ib.innerHTML=`<div class="sub">MQTT broker: ${esc(S.info.mqtt)}<br>Log level: ${esc(S.info.log_level)}</div><div class="hint">Change these on the add-on's Configuration tab, then restart the add-on.</div>`;
  el.append(acc('set-conn','Connection','',ib));
  const cb=body();cb.innerHTML='<div class="sub">Checking…</div>';
  el.append(acc('set-card','Dashboard card','Show travel times on any dashboard',cb));fillCard(cb);
}
const CARD_YAML=`type: custom:travel-times-card
title: Travel Times
layout: list        # list or tiles
group_by: person    # person, place or none
# people: [Jon, MJ]
# warn_above: 30
# font_scale: 1.2`;
async function fillCard(el){
  let i;try{i=await (await fetch('api/card')).json()}catch(e){el.innerHTML='<div class="sub err">Could not check the card</div>';return}
  const st=!i.installed?'Not installed yet':(i.installed===i.available?`Installed (v${esc(i.installed)})`:`Update available: v${esc(i.installed)} to v${esc(i.available)}`);
  el.innerHTML=`<div class="sub">${st}</div><div class="hint">Installs the card file and adds it to your dashboard resources. Then add a <b>Travel Times</b> card from the card picker, or paste the YAML below into a manual card. In sections view you can drag it to any size and it scrolls when the list is longer.</div>`;
  const b=document.createElement('button');b.className='p';b.style.cssText='width:100%;margin:10px 0';b.textContent=i.installed?(i.installed===i.available?'Reinstall card':'Update card'):'Install card';
  b.onclick=async()=>{
    b.disabled=true;b.textContent='Installing…';
    const j=await J('api/card/install');
    if(j.error){toast(j.error,5000);b.disabled=false;b.textContent='Try again';return}
    let m='Card installed.';
    if(!j.registered)m+=' Add it manually under Settings > Dashboards > Resources: '+j.url;
    if(j.restart_needed)m+=' Restart Home Assistant once so it can serve the new www folder.';
    toast(m,8000);fillCard(el);
  };
  el.append(b);
  const pre=document.createElement('pre');pre.textContent=CARD_YAML;pre.style.cssText='background:#12121f;border:1px solid var(--line);border-radius:10px;padding:10px;overflow:auto;font-size:12px;margin:0 0 8px';
  const cp=document.createElement('button');cp.textContent='Copy YAML';cp.onclick=()=>{navigator.clipboard&&navigator.clipboard.writeText(CARD_YAML);toast('Copied')};
  el.append(pre,cp);
}
function renderFind(){
  $('#fseg').innerHTML=[['quick','⚡ Address (fast)'],['ai','✨ AI search']].map(([k,l])=>`<button class="${fmode===k?'on':''}" onclick="setFind('${k}')">${l}</button>`).join('');
  $('#q').placeholder=fmode==='quick'?'Address, business, or "lat, lon"':'e.g. closest Lowe\'s, Mario\'s Pizza downtown';
}
function setFind(k){fmode=k;SR=[];$('#sres').innerHTML='';renderFind()}
function renderSearchOpts(){
  $('#near').innerHTML='<option value="home">Near Home</option>'+S.config.people.filter(p=>p.entity).map(p=>`<option value="${esc(p.id)}">Near ${esc(p.name||p.entity)} right now</option>`).join('');
  const el=$('#sopts');el.innerHTML='';
  S.config.people.forEach(p=>{
    const l=document.createElement('label');l.className='chip';
    const c=document.createElement('input');c.type='checkbox';c.checked=!OFF.has(p.id);c.onchange=()=>{c.checked?OFF.delete(p.id):OFF.add(p.id)};
    l.append(c,p.name||'(unnamed)');el.append(l);
  });
  if(!S.config.people.length)el.innerHTML='<span class="sub">Add people in Setup first</span>';
}
function dist(m){if(m==null)return'';const u=S.config.settings.units==='metric';return ' · '+(m/(u?1000:1609.344)).toFixed(1)+(u?' km':' mi')+' away'}
function nearEntity(){const p=S.config.people.find(x=>x.id===$('#near').value);return p?p.entity:'home'}
/* fast search: parallel geocoders on the server; quick=true is the single-provider type-ahead */
async function geoFetch(q,near,quick){
  const r=await fetch('api/geosearch?q='+encodeURIComponent(q)+'&near='+encodeURIComponent(near||'home')+(quick?'&quick=1':''));
  return r.json();
}
function listResults(out,list,onPick,label){
  out.innerHTML='';
  list.forEach(x=>{
    const c=document.createElement('div');c.className='card live';
    const src=x.source?`<span class="src">${esc(x.source)}</span>`:'';
    c.innerHTML=`<div><b>${esc(x.name)}</b>${src}<div class="sub">${esc(x.address||'')}${dist(x.dist_m)}</div></div>`;
    const b=document.createElement('button');b.className='p';b.textContent=label||'Add';b.onclick=()=>onPick(x,b);
    c.append(b);out.append(c);
  });
}
function noMatch(out,errs){
  out.innerHTML='<div class="sub">No match. Try "street, city, state", add the city, or paste "lat, lon".</div>'+(errs&&errs.length?`<div class="hint err">${esc(errs.join(' | '))}</div>`:'');
}
async function quickSearch(q,quick){
  const out=$('#sres'),seq=++qseq;
  if(!quick)out.innerHTML='<div class="sub">Searching…</div>';
  try{
    const j=await geoFetch(q,nearEntity(),quick);if(seq!==qseq)return;
    if(!j.results.length){if(!quick)noMatch(out,j.errors);return}
    SR=j.results;listResults(out,SR,(x,b)=>addResult(x,b));
  }catch(e){if(seq===qseq&&!quick)out.innerHTML='<div class="sub err">Search failed</div>'}
}
function typeahead(){
  clearTimeout(qtimer);if(fmode!=='quick')return;
  const q=$('#q').value.trim();if(q.length<4)return;
  qtimer=setTimeout(()=>quickSearch(q,true),450);
}
async function doSearch(){
  const q=$('#q').value.trim();if(!q)return;clearTimeout(qtimer);
  if(fmode==='quick')return quickSearch(q,false);
  const out=$('#sres'),ent=S.config.settings.ai_task_entity;
  if(!ent){out.innerHTML='<div class="sub err">Pick an AI Task entity in Settings first, or use the fast Address search.</div>';return}
  out.innerHTML='<div class="sub">Searching… this can take up to a minute</div>';
  try{
    const j=await J('api/search',{query:q,entity:ent,near:nearEntity()});
    if(j.error){out.innerHTML=`<div class="sub err">${esc(j.error)}</div>`;return}
    SR=j.results;listResults(out,SR,(x,b)=>addResult(x,b));
  }catch(e){out.innerHTML='<div class="sub err">Search failed</div>'}
}
function addResult(x,btn){
  const id=rid();
  S.config.destinations.push({id,name:(x.name||'Place').slice(0,40),target:`${x.lat}, ${x.lon}`});
  S.config.people.forEach(p=>{if(p.id&&!OFF.has(p.id))S.config.routes.push({from:p.id,to:id})});
  btn.textContent='Added ✓';btn.disabled=true;touch();renderDests();renderRoutes();renderSeg();renderLive(S.status);toast('Added. Press Save to start tracking.');
}
/* address picker sheet (used by the 🔎 button on a place) */
function openAddrSheet(onPick){
  const b=$('#sheetbody');b.innerHTML='<h2 style="margin:0 0 8px">Search address</h2>';
  const q=document.createElement('input');q.placeholder='Address, business, or "lat, lon"';q.autocomplete='off';q.enterkeyhint='search';
  const out=document.createElement('div');out.style.marginTop='10px';
  let seq=0,tm=null;
  const run=async quick=>{
    const v=q.value.trim();if(v.length<2)return;const my=++seq;
    if(!quick)out.innerHTML='<div class="sub">Searching…</div>';
    try{const j=await geoFetch(v,'home',quick);if(my!==seq)return;
      if(!j.results.length){if(!quick)noMatch(out,j.errors);return}
      listResults(out,j.results,x=>{closeSheet();onPick(x)},'Use');
    }catch(e){if(my===seq&&!quick)out.innerHTML='<div class="sub err">Search failed</div>'}
  };
  q.oninput=()=>{clearTimeout(tm);if(q.value.trim().length>=4)tm=setTimeout(()=>run(true),450)};
  q.onkeydown=e=>{if(e.key==='Enter'){clearTimeout(tm);run(false)}};
  const go=document.createElement('button');go.className='p';go.textContent='Search';go.style.cssText='width:100%;margin-top:8px';go.onclick=()=>{clearTimeout(tm);run(false)};
  const cl=document.createElement('button');cl.textContent='Cancel';cl.style.cssText='width:100%;margin-top:8px';cl.onclick=closeSheet;
  b.append(q,go,out,cl);$('#sheet').classList.add('open');setTimeout(()=>q.focus(),50);
}
function ago(t){if(!t)return'never';const s=Math.round(Date.now()/1000-t);return s<90?s+'s ago':Math.round(s/60)+'m ago'}
function renderLive(st){
  const by={};
  for(const r of S.config.routes){if(r.from&&r.to&&r.from!==r.to)(by[r.from]=by[r.from]||[]).push(r)}
  let out='';
  const tl=S.config.trackers.filter(t=>t.members.length>1);
  if(tl.length){
    out+='<h2>Between people</h2>';
    const ML={max:'farthest pair',min:'closest pair',avg:'average'};
    for(const t of tl){
      const x=st['t|'+t.id]||{};const u=x.unit||'';let big='…',sb='waiting for first calculation';
      const names=t.name||t.members.map(nameOf).join(' & ');
      if(x.minutes!=null){
        big=x.together?'Together':Math.round(x.minutes)+' min';
        const bits=[];if(x.distance!=null)bits.push(`${x.distance} ${u} by road`);if(x.straight_line!=null)bits.push(`${x.straight_line} ${u} straight`);
        if(t.members.length>2)bits.push(ML[x.mode]||'');
        sb=bits.join(' · ');
      }else if(x.straight_line!=null)sb=`${x.straight_line} ${u} straight-line · waiting for drive time`;
      let pr='';
      if(t.members.length>2&&x.pairs){
        pr='<div class="pairs">'+x.pairs.map(q=>`<div><b>${esc(q.a)} ↔ ${esc(q.b)}</b> ${q.minutes!=null?(q.arrived?'together':Math.round(q.minutes)+' min · '+q.distance+' '+esc(u)):(q.error?'<span class="err">'+esc(q.error)+'</span>':'…')}</div>`).join('')+'</div>';
      }
      out+=`<div class="card"><div class="live"><div><b>📏 ${esc(names)}</b><div class="sub">${sb}</div></div><div class="big">${big}</div></div>${pr}</div>`;
    }
  }
  const gl=S.config.groups.filter(g=>g.members.length>1&&(g.destinations||[]).length);
  if(gl.length){
    out+='<h2>Groups</h2>';
    for(const g of gl)for(const did of g.destinations){
      const x=st['g|'+gkey(g,did)]||{};let big='…',sb='waiting for everyone\'s first calculation';
      if(x.minutes!=null){
        big=x.arrived?'Here':Math.round(x.minutes)+' min';
        sb=x.members.map(m=>`${esc(m.name)} ${m.arrived?'here':Math.round(m.minutes)+' min'}`).join(' · ')+` · ${x.total_distance} ${x.unit||''} combined`;
      }
      out+=`<div class="card live"><div><b>👥 ${esc(g.name||'(unnamed)')} → ${esc(nameOf(did))}</b><div class="sub">${sb}</div></div><div class="big">${big}</div></div>`;
    }
  }
  for(const p of S.config.people){
    if(!by[p.id])continue;out+=`<h2>${esc(p.name||'(unnamed)')}</h2>`;
    for(const r of by[p.id]){
      const x=st[r.from+'|'+r.to]||{};let big='…',sb='waiting for first calculation';
      if(x.minutes!=null){big=x.arrived?'Here':Math.round(x.minutes)+' min';
        sb=(x.arrived?'arrived':`${x.distance} ${x.unit}${x.via?' via '+esc(x.via):''}`)+' · '+ago(x.t)}
      if(x.error)sb=`<span class="err">${esc(x.error)}</span>`+(x.minutes!=null?' · last good '+ago(x.t):'');
      out+=`<div class="card live"><div><b>→ ${esc(nameOf(r.to))}</b><div class="sub">${sb}</div></div><div class="big">${big}</div></div>`;
    }
  }
  $('#live').innerHTML=out||'<div class="card"><div class="sub">'+(S.admin?'Nothing to show yet. Open <b>Setup</b> to add people, places, routes and trackers, then Save.':'Nothing to show yet.')+'</div></div>';
}
async function save(){
  const r=await J('api/config',S.config);
  const keep=page;await load();go(keep);toast('Saved ✓');
}
async function refreshNow(){await J('api/refresh');toast('Recalculating…')}
/* ---------- backups ---------- */
const RL={'/data':'Add-on storage','/share':'Share','/media':'Media','/config':'HA config'};
async function loadBackups(){try{BK=await (await fetch('api/backups')).json()}catch(e){BK=null}renderBackup()}
function fmtDate(t){return new Date(t*1000).toLocaleString([], {dateStyle:'medium',timeStyle:'short'})}
function renderBackup(){
  const el=$('#backup');if(!S)return;el.innerHTML='';const s=S.config.settings;
  const c1=document.createElement('div');c1.className='card stack';
  c1.innerHTML=`<h2 style="margin:0">Backup folder</h2><div style="word-break:break-all">${esc(s.backup_dir)}</div><div class="hint">Saves your people, places, routes and settings. Folders under Share, Media or HA config are easy to reach from other tools. The default is inside the add-on and is part of normal Home Assistant backups.</div>`;
  const ch=document.createElement('button');ch.textContent='Choose folder…';ch.onclick=openPicker;c1.append(ch);el.append(c1);
  const c2=document.createElement('div');c2.className='card';c2.insertAdjacentHTML('beforeend','<h2 style="margin-top:0">Automatic backups</h2>');
  c2.append(field('Schedule',selCtl([['off','Off'],['daily','Every day'],['weekly','Every week']],s.backup_auto,v=>{s.backup_auto=v;touch()})),
    field('Keep the newest (files)',numCtl(s.backup_keep,v=>{s.backup_keep=v;touch()}),'Older backups in this folder are deleted'));
  el.append(c2);
  const now=document.createElement('button');now.className='p';now.textContent='Back up now';now.style.width='100%';now.style.marginBottom='12px';
  now.onclick=async()=>{if(dirty){await save();go('backup')}const j=await J('api/backup');j.error?toast(j.error,4000):toast('Backup saved: '+j.name,3500);loadBackups()};
  el.append(now);
  const c3=document.createElement('div');c3.insertAdjacentHTML('beforeend','<h2>Backups in this folder</h2>');
  const files=(BK&&BK.files)||[];
  if(!files.length)c3.insertAdjacentHTML('beforeend','<div class="card"><div class="sub">No backups yet.</div></div>');
  files.forEach(f=>{
    const c=document.createElement('div');c.className='card stack';
    c.innerHTML=`<div><b>${esc(fmtDate(f.mtime))}</b><div class="sub">${esc(f.name)} · ${(f.size/1024).toFixed(1)} KB</div></div>`;
    const h=document.createElement('div');h.className='hrow';
    const rs=document.createElement('button');rs.textContent='Restore';rs.onclick=async()=>{if(!confirm('Replace your current people, places, routes and settings with this backup?'))return;const j=await J('api/backup/restore',{name:f.name});if(j.error){toast(j.error,4000);return}await load();go('backup');toast(`Restored ${j.routes} routes`)};
    const dl=document.createElement('a');dl.href='api/backup/download?name='+encodeURIComponent(f.name);dl.textContent='Download';dl.style.cssText='display:flex;align-items:center;justify-content:center;min-height:44px;border:1px solid var(--line);border-radius:10px;color:var(--tx);text-decoration:none;background:#23233a;padding:0 14px';
    const del=document.createElement('button');del.className='d';del.textContent='Delete';del.onclick=async()=>{if(!confirm('Delete this backup file?'))return;await J('api/backup/delete',{name:f.name});loadBackups()};
    h.append(rs,dl,del);c.append(h);c3.append(c);
  });
  el.append(c3);
}
/* ---------- folder picker ---------- */
function closeSheet(){$('#sheet').classList.remove('open')}
async function openPicker(){$('#sheet').classList.add('open');await browse(S.config.settings.backup_dir||'')}
async function browse(p){BR=await (await fetch('api/browse?path='+encodeURIComponent(p))).json();renderSheet()}
function renderSheet(){
  const b=$('#sheetbody');b.innerHTML='';
  const hd=document.createElement('div');hd.innerHTML=`<h2 style="margin:0 0 4px">Choose a folder</h2><div class="sub" style="margin-bottom:8px;word-break:break-all">${BR.path?esc(BR.path):'Pick a location'}</div>`;b.append(hd);
  const add=(label,path)=>{const it=document.createElement('div');it.className='item';it.innerHTML=`<span>${label}</span><span class="sub">›</span>`;it.onclick=()=>browse(path);b.append(it)};
  if(BR.path!==null)add('⬆️ Up',BR.parent);
  (BR.dirs||[]).forEach(d=>add(`📁 ${esc(d.name)}`,d.path));
  if(BR.path!==null&&!(BR.dirs||[]).length){b.insertAdjacentHTML('beforeend','<div class="sub" style="padding:10px 4px">No subfolders here.</div>')}
  if(BR.path!==null){
    const nf=document.createElement('div');nf.className='hrow';nf.style.margin='12px 0 8px';
    const ni=document.createElement('input');ni.placeholder='New folder name';
    const nb=document.createElement('button');nb.textContent='Create';nb.onclick=async()=>{const j=await J('api/mkdir',{path:BR.path,name:ni.value});if(j.error){toast(j.error,3500);return}browse(j.path)};
    nf.append(ni,nb);b.append(nf);
    const use=document.createElement('button');use.className='p';use.style.width='100%';use.textContent='Use this folder';
    use.onclick=async()=>{S.config.settings.backup_dir=BR.path;closeSheet();await save();go('backup')};b.append(use);
  }
  const cl=document.createElement('button');cl.textContent='Cancel';cl.style.cssText='width:100%;margin-top:8px';cl.onclick=closeSheet;b.append(cl);
}
setInterval(async()=>{if(!S)return;try{const r=await fetch('api/status');S.status=await r.json();renderLive(S.status)}catch(e){}},5000);
load().then(()=>{let k='live';try{k=sessionStorage.getItem('tt_page')||'live'}catch(e){}go(PAGES.some(x=>x[0]===k)?k:'live')});
</script></body></html>"""


def main():
    global cfg, geocache
    os.makedirs(DATA, exist_ok=True)
    cfg = load_cfg()
    geocache = load_json(GEO_FILE, {})
    refresh_card_if_installed()
    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=worker, daemon=True).start()
    log.info("Travel Times started")
    app.run(host="0.0.0.0", port=8099, threaded=True)


if __name__ == "__main__":
    main()
