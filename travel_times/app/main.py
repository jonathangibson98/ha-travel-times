#!/usr/bin/env python3
"""Travel Times - near-real-time Waze travel-time sensors for Home Assistant.

* Reads each person's GPS from Home Assistant (person / device_tracker entities)
* Asks Waze for the drive time to every destination you pick
* Polls FAST while someone is moving, SLOW while they sit still
* Publishes one sensor per person+destination through MQTT discovery
"""
import json
import logging
import math
import os
import re
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests
try:
    from curl_cffi import requests as cffi_requests
except Exception:  # falls back to plain requests (Waze will likely answer 403)
    cffi_requests = None
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
        d = ends.get(g.get("to"))
        for m in g.get("members", []):
            if d and m in people and m != d["id"]:
                out.setdefault((m, d["id"]), (people[m], d))
    return out


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
def geocode(addr):
    key = addr.strip().lower()
    with lock:
        if key in geocache:
            return tuple(geocache[key])
        if time.time() < geo_fail.get(key, 0):
            return None
    try:
        time.sleep(1.1)  # Nominatim asks for max 1 request/second
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": addr, "format": "jsonv2", "limit": 1},
            headers={"User-Agent": "ha-travel-times-addon/1.0"},
            timeout=12,
        )
        js = r.json()
        if js:
            pt = (float(js[0]["lat"]), float(js[0]["lon"]))
            with lock:
                geocache[key] = list(pt)
                save_json(GEO_FILE, geocache)
            return pt
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
def group_disc_topic(gid):
    return f"homeassistant/sensor/{BASE}_group_{gid}/config"


def publish_group_discovery(g, ends):
    d = ends.get(g["to"])
    if not d:
        return
    payload = {
        "name": f"{g['name']} to {d['name']}",
        "unique_id": f"{BASE}_group_{g['id']}",
        "default_entity_id": f"sensor.{slug(g['name'])}_to_{slug(d['name'])}",
        "state_topic": f"{BASE}/group/{g['id']}/state",
        "json_attributes_topic": f"{BASE}/group/{g['id']}/attrs",
        "availability_topic": STATUS_TOPIC,
        "unit_of_measurement": "min",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": "mdi:account-group",
        "device": DEVICE,
    }
    pub(group_disc_topic(g["id"]), json.dumps(payload))


def group_state(c, g, snap):
    """(minutes, attrs) for a group, or None until every member has a result."""
    people = {p["id"]: p for p in c["people"]}
    ends = endpoints_of(c)
    d = ends.get(g["to"])
    if not d:
        return None
    members, unit = [], None
    for m in g["members"]:
        st = snap.get(f"{m}|{g['to']}", {})
        if m not in people or st.get("minutes") is None:
            return None
        unit = st.get("unit") or unit
        members.append({"name": people[m]["name"], "minutes": st["minutes"],
                        "distance": st.get("distance") or 0, "arrived": bool(st.get("arrived"))})
    mins = [x["minutes"] for x in members]
    mode = g.get("mode", "max")
    minutes = round({"max": max(mins), "min": min(mins), "sum": sum(mins)}[mode], 1)
    now = datetime.now().astimezone()
    attrs = {
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
        res = group_state(c, g, snap)
        if not res:
            continue
        minutes, attrs = res
        key = json.dumps([minutes, attrs["members"], attrs["mode"]])
        with lock:
            group_live[g["id"]] = {"minutes": minutes, "unit": attrs["distance_unit"], "t": time.time(),
                                   "members": attrs["members"], "total_distance": attrs["total_distance"],
                                   "arrived": attrs["arrived"], "mode": attrs["mode"], "error": None}
            if group_cache.get(g["id"]) == key:
                continue
            group_cache[g["id"]] = key
        pub(f"{BASE}/group/{g['id']}/state", str(minutes))
        pub(f"{BASE}/group/{g['id']}/attrs", json.dumps(attrs))


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
        to = idmap.get(str(g.get("to")))
        members = []
        for m in g.get("members") or []:
            m = idmap.get(str(m))
            if m in person_ids and m != to and m not in members:
                members.append(m)
        if name and to in valid_to and len(members) >= 2:
            groups.append({"id": new_id(g.get("id")), "name": name, "members": members[:8], "to": to,
                           "mode": g.get("mode") if g.get("mode") in ("max", "min", "sum") else "max"})

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
    return {"people": people, "destinations": dests, "routes": routes, "groups": groups, "settings": s}


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
        gone = {g["id"] for g in old.get("groups", [])} - {g["id"] for g in new.get("groups", [])}
        for gid in gone:
            pub(group_disc_topic(gid), "")
            group_cache.pop(gid, None)
            group_live.pop(gid, None)
        cfg = new
        save_json(CFG_FILE, new)
    republish_all()


# -------------------------------------------------------------- AI place search
def near_point(near):
    if near and near != "home" and ENTITY_RE.match(near):
        pt = ha_coords(near)
        if pt:
            return pt, "the person's current location"
    try:
        j = requests.get(f"{HA}/config", headers=HDR, timeout=8).json()
        return (float(j["latitude"]), float(j["longitude"])), "home"
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


# ------------------------------------------------------------------ Flask UI
app = Flask(__name__)


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
        return out


@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.get("/api/config")
def api_get():
    trackers, zones, ais = list_entities()
    with lock:
        c = json.loads(json.dumps(cfg))
    mq_host = (OPTIONS.get("mqtt_host") or "").strip()
    info = {"mqtt": mq_host or "Mosquitto (automatic)", "log_level": str(OPTIONS.get("log_level", "info"))}
    return jsonify(config=c, status=status_public(), trackers=trackers, zones=zones, ai_tasks=ais, info=info)


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
.seg{display:flex;background:#12121f;border:1px solid var(--line);border-radius:12px;padding:3px;margin-bottom:12px}
.seg button{flex:1;border:0;background:transparent;min-height:40px;padding:6px 4px;font-size:14px}.seg button.on{background:var(--ac);color:#04121c;font-weight:700}
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
<div class="top"><h1 id="title">Live</h1><button class="ico" onclick="refreshNow()" aria-label="Refresh">⟳</button></div>
<main>
<section id="p-live" class="pg"><div id="live"><div class="sub">Loading…</div></div></section>
<section id="p-setup" class="pg hid">
 <div class="seg" id="seg"></div>
 <div id="s-people"></div><div id="s-places" class="hid"></div><div id="s-routes" class="hid"></div><div id="s-groups" class="hid"></div>
</section>
<section id="p-find" class="pg hid">
 <div class="card stack">
  <input id="q" placeholder="e.g. closest Lowe's, Mario's Pizza downtown" enterkeyhint="search" onkeydown="if(event.key==='Enter')doSearch()">
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
let S=null,SR=[],BR=null,BK=null,page='live',sub='people',dirty=false;
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
  page=k;try{sessionStorage.setItem('tt_page',k)}catch(e){}
  PAGES.forEach(([x,,l])=>{$('#p-'+x).classList.toggle('hid',x!==k);$('#n-'+x).classList.toggle('on',x===k);if(x===k)$('#title').textContent=l});
  if(k==='backup')loadBackups();window.scrollTo(0,0);
}
async function load(){
  const r=await fetch('api/config');S=await r.json();S.config.routes=S.config.routes||[];S.config.groups=S.config.groups||[];
  $('#dl').innerHTML=[...S.zones,...S.trackers].map(o=>`<option value="${esc(o.id)}">${esc(o.name)}</option>`).join('');
  dirty=false;$('#savebar').classList.add('hid');renderAll();
}
function renderAll(){renderSeg();renderPeople();renderDests();renderRoutes();renderGroups();renderSearchOpts();renderSettings();renderBackup();renderLive(S.status)}
function dropRoutes(id){
  S.config.routes=S.config.routes.filter(r=>r.from!==id&&r.to!==id);
  S.config.groups.forEach(g=>{g.members=g.members.filter(m=>m!==id)});
  S.config.groups=S.config.groups.filter(g=>g.to!==id&&g.members.length>=2);
}
function renderSeg(){
  const C=S.config,t=[['people','People',C.people.length],['places','Places',C.destinations.length],['routes','Routes',C.routes.length],['groups','Groups',C.groups.length]];
  $('#seg').innerHTML=t.map(([k,l,n])=>`<button class="${sub===k?'on':''}" onclick="setSub('${k}')">${l} (${n})</button>`).join('');
  t.forEach(([k])=>$('#s-'+k).classList.toggle('hid',k!==sub));
}
function setSub(k){sub=k;renderSeg()}
function card(){const c=document.createElement('div');c.className='card stack';return c}
function renderPeople(){
  const el=$('#s-people');el.innerHTML='';
  S.config.people.forEach((p,i)=>{
    const c=card();
    const nm=document.createElement('input');nm.placeholder='Name (Jon)';nm.value=p.name||'';nm.oninput=()=>{p.name=nm.value;touch();renderRoutes();renderLive(S.status);renderSearchOpts()};
    const sel=document.createElement('select');
    const opts=[...S.trackers];if(p.entity&&!opts.find(o=>o.id===p.entity))opts.unshift({id:p.entity,name:p.entity});
    sel.innerHTML='<option value="">Pick a location source…</option>'+opts.map(o=>`<option value="${esc(o.id)}">${esc(o.name)} (${esc(o.id)})</option>`).join('');
    sel.value=p.entity||'';sel.onchange=()=>{p.entity=sel.value;touch();renderSearchOpts()};
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove person';rm.onclick=()=>{dropRoutes(p.id);S.config.people.splice(i,1);touch();renderAll()};
    c.append(nm,sel,rm);el.append(c);
  });
  const add=document.createElement('button');add.className='p';add.textContent='+ Add person';add.onclick=()=>{S.config.people.push({id:rid(),name:'',entity:''});touch();renderAll()};
  el.append(add);
}
function renderDests(){
  const el=$('#s-places');el.innerHTML='';
  S.config.destinations.forEach((d,i)=>{
    const c=card();
    const nm=document.createElement('input');nm.placeholder='Name (Work)';nm.value=d.name||'';nm.oninput=()=>{d.name=nm.value;touch();renderRoutes();renderLive(S.status)};
    const tg=document.createElement('input');tg.placeholder='zone.work, 34.5,-82.6, or address';tg.setAttribute('list','dl');tg.value=d.target||'';tg.oninput=()=>{d.target=tg.value;touch()};
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove place';rm.onclick=()=>{dropRoutes(d.id);S.config.destinations.splice(i,1);touch();renderAll()};
    c.append(nm,tg,rm);el.append(c);
  });
  const add=document.createElement('button');add.className='p';add.textContent='+ Add place';add.onclick=()=>{S.config.destinations.push({id:rid(),name:'',target:''});touch();renderAll()};
  el.append(add);el.insertAdjacentHTML('beforeend','<div class="hint">A place can be a zone, "lat, lon", or a street address. Use the Find tab to search with AI.</div>');
}
function renderRoutes(){
  const el=$('#s-routes');el.innerHTML='';
  const P=S.config.people,D=S.config.destinations;
  const po=P.map(p=>`<option value="${esc(p.id)}">${esc(p.name||'(unnamed)')}</option>`).join('');
  const dopt=D.map(d=>`<option value="${esc(d.id)}">${esc(d.name||'(unnamed)')}</option>`).join('');
  S.config.routes.forEach((r,i)=>{
    const c=card();
    const f=document.createElement('select');f.innerHTML='<option value="">From…</option>'+po;f.value=r.from||'';f.onchange=()=>{r.from=f.value;touch();renderLive(S.status)};
    const t=document.createElement('select');
    t.innerHTML='<option value="">To…</option>'+(D.length?`<optgroup label="Places">${dopt}</optgroup>`:'')+(P.length?`<optgroup label="People (live)">${po}</optgroup>`:'');
    t.value=r.to||'';t.onchange=()=>{r.to=t.value;touch();renderLive(S.status)};
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove route';rm.onclick=()=>{S.config.routes.splice(i,1);touch();renderRoutes();renderSeg();renderLive(S.status)};
    c.append(f,t,rm);el.append(c);
  });
  const add=document.createElement('button');add.className='p';add.textContent='+ Add route';add.onclick=()=>{S.config.routes.push({from:'',to:''});touch();renderRoutes();renderSeg()};
  const fill=document.createElement('button');fill.textContent='Everyone → all places';fill.onclick=fillAll;
  const w=document.createElement('div');w.className='stack';w.append(add,fill);el.append(w);
  el.insertAdjacentHTML('beforeend','<div class="hint">Each route is one sensor. Pick a person as the "To" to track person to person.</div>');
}
const MODES=[['max','Longest: when everyone has arrived'],['min','Shortest: first one there'],['sum','Combined total of everyone']];
function renderGroups(){
  const el=$('#s-groups');el.innerHTML='';
  const P=S.config.people,D=S.config.destinations;
  const dopt=(D.length?`<optgroup label="Places">${D.map(d=>`<option value="${esc(d.id)}">${esc(d.name||'(unnamed)')}</option>`).join('')}</optgroup>`:'')+(P.length?`<optgroup label="People (live)">${P.map(p=>`<option value="${esc(p.id)}">${esc(p.name||'(unnamed)')}</option>`).join('')}</optgroup>`:'');
  S.config.groups.forEach((g,i)=>{
    const c=card();
    const nm=document.createElement('input');nm.placeholder='Group name (Jon & Sam)';nm.value=g.name||'';nm.oninput=()=>{g.name=nm.value;touch();renderLive(S.status)};
    const mem=document.createElement('div');mem.innerHTML='<div class="sub">People in this group (2 or more)</div>';
    const chips=document.createElement('div');chips.className='chips';
    P.forEach(p=>{
      const l=document.createElement('label');l.className='chip';
      const k=document.createElement('input');k.type='checkbox';k.checked=g.members.includes(p.id);
      k.onchange=()=>{g.members=k.checked?[...g.members,p.id]:g.members.filter(x=>x!==p.id);touch()};
      l.append(k,p.name||'(unnamed)');chips.append(l);
    });
    mem.append(chips);
    const to=document.createElement('select');to.innerHTML='<option value="">To…</option>'+dopt;to.value=g.to||'';to.onchange=()=>{g.to=to.value;touch();renderLive(S.status)};
    const md=selCtl(MODES,g.mode||'max',v=>{g.mode=v;touch()});
    const rm=document.createElement('button');rm.className='d';rm.textContent='Remove group';rm.onclick=()=>{S.config.groups.splice(i,1);touch();renderGroups();renderSeg();renderLive(S.status)};
    c.append(nm,mem,to,md,rm);el.append(c);
  });
  const add=document.createElement('button');add.className='p';add.textContent='+ Add group';
  add.onclick=()=>{S.config.groups.push({id:rid(),name:'',members:S.config.people.slice(0,2).map(p=>p.id),to:'',mode:'max'});touch();renderGroups();renderSeg()};
  el.append(add);
  el.insertAdjacentHTML('beforeend','<div class="hint">A group is one sensor for several people going to the same place, e.g. you and your partner heading home. The sensor shows the time by the rule you pick, and its attributes list each person plus the combined total time and distance.</div>');
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
function group(title,fields){const c=document.createElement('div');c.className='card';c.insertAdjacentHTML('beforeend',`<h2 style="margin-top:0">${title}</h2>`);fields.forEach(f=>c.append(f));return c}
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
  const inf=document.createElement('div');inf.className='card';
  inf.innerHTML=`<h2 style="margin-top:0">Connection</h2><div class="sub">MQTT broker: ${esc(S.info.mqtt)}<br>Log level: ${esc(S.info.log_level)}</div><div class="hint">Change these on the add-on's Configuration tab, then restart the add-on.</div>`;
  el.append(inf);
}
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
async function doSearch(){
  const q=$('#q').value.trim();if(!q)return;const out=$('#sres');
  const ent=S.config.settings.ai_task_entity;
  if(!ent){out.innerHTML='<div class="sub err">Pick an AI Task entity in Settings first.</div>';return}
  out.innerHTML='<div class="sub">Searching… this can take up to a minute</div>';
  const p=S.config.people.find(x=>x.id===$('#near').value);
  try{
    const j=await J('api/search',{query:q,entity:ent,near:p?p.entity:'home'});
    if(j.error){out.innerHTML=`<div class="sub err">${esc(j.error)}</div>`;return}
    SR=j.results;out.innerHTML='';
    SR.forEach((x,i)=>{
      const c=document.createElement('div');c.className='card live';
      c.innerHTML=`<div><b>${esc(x.name)}</b><div class="sub">${esc(x.address)}${dist(x.dist_m)}</div></div>`;
      const b=document.createElement('button');b.className='p';b.textContent='Add';b.onclick=()=>addResult(i,b);
      c.append(b);out.append(c);
    });
  }catch(e){out.innerHTML='<div class="sub err">Search failed</div>'}
}
function addResult(i,btn){
  const x=SR[i],id=rid();
  S.config.destinations.push({id,name:x.name.slice(0,40),target:`${x.lat}, ${x.lon}`});
  S.config.people.forEach(p=>{if(p.id&&!OFF.has(p.id))S.config.routes.push({from:p.id,to:id})});
  btn.textContent='Added ✓';btn.disabled=true;touch();renderDests();renderRoutes();renderSeg();renderLive(S.status);toast('Added. Press Save to start tracking.');
}
function ago(t){if(!t)return'never';const s=Math.round(Date.now()/1000-t);return s<90?s+'s ago':Math.round(s/60)+'m ago'}
function renderLive(st){
  const by={};
  for(const r of S.config.routes){if(r.from&&r.to&&r.from!==r.to)(by[r.from]=by[r.from]||[]).push(r)}
  let out='';
  const gl=S.config.groups.filter(g=>g.to&&g.members.length>1);
  if(gl.length){
    out+='<h2>Groups</h2>';
    for(const g of gl){
      const x=st['g|'+g.id]||{};let big='…',sb='waiting for everyone\'s first calculation';
      if(x.minutes!=null){
        big=x.arrived?'Here':Math.round(x.minutes)+' min';
        sb=x.members.map(m=>`${esc(m.name)} ${m.arrived?'here':Math.round(m.minutes)+' min'}`).join(' · ')+` · ${x.total_distance} ${x.unit||''} combined`;
      }
      out+=`<div class="card live"><div><b>👥 ${esc(g.name||'(unnamed)')} → ${esc(nameOf(g.to))}</b><div class="sub">${sb}</div></div><div class="big">${big}</div></div>`;
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
  $('#live').innerHTML=out||'<div class="card"><div class="sub">Nothing to show yet. Open <b>Setup</b> to add people, places and routes, then Save.</div></div>';
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
    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=worker, daemon=True).start()
    log.info("Travel Times started")
    app.run(host="0.0.0.0", port=8099, threaded=True)


if __name__ == "__main__":
    main()
