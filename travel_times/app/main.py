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
    s = dict(DEFAULT_SETTINGS)
    s.update(c.get("settings", {}))
    c["settings"] = s
    return c


def pairs_of(c):
    return {(p["id"], d["id"]): (p, d) for p in c["people"] for d in c["destinations"]}


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
    trackers, zones = [], []
    try:
        r = requests.get(f"{HA}/states", headers=HDR, timeout=20)
        r.raise_for_status()
        for st in r.json():
            eid, a = st["entity_id"], st.get("attributes", {})
            item = {"id": eid, "name": a.get("friendly_name", eid)}
            if eid.startswith("zone."):
                zones.append(item)
            elif eid.startswith("person.") or ("latitude" in a and "longitude" in a):
                trackers.append(item)
    except Exception as e:
        log.warning("Could not list HA entities: %s", e)
    key = lambda x: x["name"].lower()
    return sorted(trackers, key=key), sorted(zones, key=key)


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


# --------------------------------------------------------------------- MQTT
def pub(topic, payload, retain=True):
    if mq is not None:
        mq.publish(topic, payload, qos=0, retain=retain)


def disc_topic(pid, did):
    return f"homeassistant/sensor/{BASE}_{pid}_{did}/config"


def publish_discovery(p, d):
    payload = {
        "name": f"To {d['name']}",
        "unique_id": f"{BASE}_{p['id']}_{d['id']}",
        "default_entity_id": f"sensor.{slug(p['name'])}_to_{slug(d['name'])}",
        "state_topic": f"{BASE}/{p['id']}/{d['id']}/state",
        "json_attributes_topic": f"{BASE}/{p['id']}/{d['id']}/attrs",
        "availability_topic": STATUS_TOPIC,
        "unit_of_measurement": "min",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": "mdi:car-clock",
        "device": {
            "identifiers": [f"{BASE}_{p['id']}"],
            "name": f"{p['name']} Travel Times",
            "manufacturer": "Travel Times add-on",
            "model": "Waze travel time",
        },
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
    publish_state(p["id"], d["id"], st)


def tick():
    global paused_until
    with lock:
        c = json.loads(json.dumps(cfg))
    s = c["settings"]
    now = time.time()
    if not c["people"] or not c["destinations"]:
        return

    locs = {p["id"]: ha_coords(p["entity"]) for p in c["people"]}
    dcoords = {d["id"]: resolve(d["target"]) for d in c["destinations"]}

    due = []
    for p in c["people"]:
        loc = locs[p["id"]]
        for d in c["destinations"]:
            key = f"{p['id']}|{d['id']}"
            dc = dcoords[d["id"]]
            with lock:
                st = status.setdefault(key, {})
                if not loc or not dc:
                    st["error"] = "no location yet" if not loc else "can't find destination"
                    continue
                if haversine(loc, dc) <= s["arrival_radius_m"]:
                    if not st.get("arrived"):
                        record_result(p, d, st, 0, 0, "", s, True)
                        st.update(t=now, loc=loc, error=None, fail=0)
                    continue
                if now < st.get("retry_at", 0):
                    continue
                age = now - st.get("t", 0)
                last = st.get("loc")
                moved = haversine(loc, last) if last else 1e9
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
                st.update(t=time.time(), loc=loc, error=None, fail=0)
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
        time.sleep(LOOP_SECONDS)


# ----------------------------------------------------------------- config API
def clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, int(float(v))))
    except (TypeError, ValueError):
        return default


def clean_cfg(body):
    people, dests, seen = [], [], set()

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
            people.append({"id": new_id(p.get("id")), "name": name, "entity": ent})
    for d in (body.get("destinations") or [])[:20]:
        name, tgt = str(d.get("name", "")).strip()[:40], str(d.get("target", "")).strip()[:200]
        if name and tgt:
            dests.append({"id": new_id(d.get("id")), "name": name, "target": tgt})

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
    return {"people": people, "destinations": dests, "settings": s}


def apply_cfg(new):
    global cfg
    with lock:
        old = cfg
        old_pairs, new_pairs = pairs_of(old), pairs_of(new)
        for (pid, did) in old_pairs.keys() - new_pairs.keys():
            pub(disc_topic(pid, did), "")  # empty payload = delete the entity
            status.pop(f"{pid}|{did}", None)
        # anything whose source/target changed gets recalculated right away
        for key, (p, d) in new_pairs.items():
            if key in old_pairs:
                op, od = old_pairs[key]
                if op["entity"] != p["entity"] or od["target"] != d["target"]:
                    st = status.get(f"{key[0]}|{key[1]}", {})
                    st.pop("t", None)
                    st.pop("arrived", None)
        cfg = new
        save_json(CFG_FILE, new)
    republish_all()


# ------------------------------------------------------------------ Flask UI
app = Flask(__name__)


@app.after_request
def nocache(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp


def status_public():
    with lock:
        return {k: {x: v.get(x) for x in ("minutes", "distance", "unit", "t", "error", "arrived", "via")}
                for k, v in status.items()}


@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.get("/api/config")
def api_get():
    trackers, zones = list_entities()
    with lock:
        c = json.loads(json.dumps(cfg))
    return jsonify(config=c, status=status_public(), trackers=trackers, zones=zones)


@app.get("/api/status")
def api_status():
    return jsonify(status_public())


@app.post("/api/config")
def api_set():
    apply_cfg(clean_cfg(request.get_json(force=True, silent=True) or {}))
    return jsonify(ok=True)


@app.post("/api/refresh")
def api_refresh():
    with lock:
        for st in status.values():
            st.pop("t", None)
            st.pop("retry_at", None)
    return jsonify(ok=True)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Travel Times</title>
<style>
:root{--tx:#e8e8f0;--mu:#8a8aa0;--ac:#03a9f4;--bad:#ff5252;--ok:#4caf50;--line:#2a2a44}
*{box-sizing:border-box}
body{margin:0;padding:16px 16px 90px;font:15px system-ui,sans-serif;color:var(--tx);
background:linear-gradient(160deg,#0d0d0d,#1a1a2e) fixed;min-height:100vh}
h1{font-size:22px;margin:0}h2{font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:var(--mu);margin:26px 0 8px}
header{display:flex;justify-content:space-between;align-items:center;gap:10px}
.card{background:rgba(255,255,255,.04);border:1px solid var(--line);border-radius:14px;padding:12px;margin-bottom:8px}
.live{display:flex;justify-content:space-between;align-items:center;gap:10px}
.big{font-size:26px;font-weight:700;color:var(--ac);white-space:nowrap}.sub{color:var(--mu);font-size:12px;margin-top:2px}
.err{color:var(--bad)}.ok{color:var(--ok)}
.row{display:grid;grid-template-columns:1fr 1.6fr auto;gap:8px;margin-bottom:8px}
input,select{width:100%;background:#12121f;color:var(--tx);border:1px solid var(--line);border-radius:10px;padding:10px;font:inherit;min-width:0}
button{background:#23233a;color:var(--tx);border:1px solid var(--line);border-radius:10px;padding:10px 14px;font:inherit;cursor:pointer}
button.p{background:var(--ac);border-color:var(--ac);color:#04121c;font-weight:700}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}label{font-size:12px;color:var(--mu);display:block;margin-bottom:4px}
.hint{font-size:11px;color:var(--mu);margin-top:3px}
.bar{position:fixed;left:0;right:0;bottom:0;padding:12px 16px;background:#0d0d0dee;border-top:1px solid var(--line);display:flex;gap:12px;align-items:center}
@media(max-width:560px){.row{grid-template-columns:1fr}.grid{grid-template-columns:1fr}}
</style></head><body>
<header><h1>🚗 Travel Times</h1><button onclick="refreshNow()">Refresh now</button></header>
<h2>Live</h2><div id="live"><div class="sub">Loading…</div></div>
<h2>People</h2><div id="people"></div><button onclick="addPerson()">+ Person</button>
<h2>Destinations</h2><div id="dests"></div><button onclick="addDest()">+ Destination</button>
<div class="hint">Destination can be a zone, another person (follows them live), "lat, lon", or a street address.</div>
<h2>Settings</h2><div id="settings" class="grid"></div>
<datalist id="dl"></datalist>
<div class="bar"><button class="p" onclick="save()">Save</button><span id="msg" class="sub"></span></div>
<script>
let S=null;const $=s=>document.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const FIELDS=[
 ['region','Waze region','select',['US','EU','AU','IL'],'US covers North America'],
 ['units','Units','select',['imperial','metric'],'Distance attribute only. Time is always minutes'],
 ['moving_interval','Moving: update every (sec)','number',0,'Min 20. This is your "real-time" speed'],
 ['idle_interval','Parked: update every (sec)','number',0,'Used when they are not going anywhere'],
 ['move_threshold_m','Counts as moving after (m)','number',0,'Distance moved since last calc'],
 ['arrival_radius_m','Arrived within (m)','number',0,'Inside this = 0 min, no Waze call'],
 ['max_requests_per_min','Max Waze calls / min','number',0,'Safety cap for all pairs combined'],
 ['avoid_tolls','Avoid toll roads','check',0,'']];
async function load(){
  const r=await fetch('api/config');S=await r.json();
  $('#dl').innerHTML=[...S.zones,...S.trackers].map(o=>`<option value="${esc(o.id)}">${esc(o.name)}</option>`).join('');
  renderPeople();renderDests();renderSettings();renderLive(S.status);
}
function renderPeople(){
  const el=$('#people');el.innerHTML='';
  S.config.people.forEach((p,i)=>{
    const row=document.createElement('div');row.className='row';
    const nm=document.createElement('input');nm.placeholder='Name (Jon)';nm.value=p.name||'';nm.oninput=()=>p.name=nm.value;
    const sel=document.createElement('select');
    const opts=[...S.trackers];if(p.entity&&!opts.find(o=>o.id===p.entity))opts.unshift({id:p.entity,name:p.entity});
    sel.innerHTML='<option value="">Pick a location source…</option>'+opts.map(o=>`<option value="${esc(o.id)}">${esc(o.name)} (${esc(o.id)})</option>`).join('');
    sel.value=p.entity||'';sel.onchange=()=>p.entity=sel.value;
    const rm=document.createElement('button');rm.textContent='✕';rm.onclick=()=>{S.config.people.splice(i,1);renderPeople();renderLive(S.status)};
    row.append(nm,sel,rm);el.append(row);
  });
}
function renderDests(){
  const el=$('#dests');el.innerHTML='';
  S.config.destinations.forEach((d,i)=>{
    const row=document.createElement('div');row.className='row';
    const nm=document.createElement('input');nm.placeholder='Name (Work)';nm.value=d.name||'';nm.oninput=()=>d.name=nm.value;
    const tg=document.createElement('input');tg.placeholder='zone.work, 34.5,-82.6, or address';tg.setAttribute('list','dl');tg.value=d.target||'';tg.oninput=()=>d.target=tg.value;
    const rm=document.createElement('button');rm.textContent='✕';rm.onclick=()=>{S.config.destinations.splice(i,1);renderDests();renderLive(S.status)};
    row.append(nm,tg,rm);el.append(row);
  });
}
function renderSettings(){
  const el=$('#settings');el.innerHTML='';const s=S.config.settings;
  FIELDS.forEach(([k,l,t,o,h])=>{
    const w=document.createElement('div');let inp;
    if(t==='select'){inp=document.createElement('select');inp.innerHTML=o.map(x=>`<option>${x}</option>`).join('');inp.value=s[k];inp.onchange=()=>s[k]=inp.value}
    else if(t==='check'){inp=document.createElement('input');inp.type='checkbox';inp.style.width='auto';inp.checked=!!s[k];inp.onchange=()=>s[k]=inp.checked}
    else{inp=document.createElement('input');inp.type='number';inp.value=s[k];inp.oninput=()=>s[k]=inp.value}
    w.innerHTML=`<label>${l}</label>`;w.append(inp);if(h)w.insertAdjacentHTML('beforeend',`<div class="hint">${h}</div>`);el.append(w);
  });
}
function ago(t){if(!t)return'never';const s=Math.round(Date.now()/1000-t);return s<90?s+'s ago':Math.round(s/60)+'m ago'}
function renderLive(st){
  const out=[];
  for(const p of S.config.people)for(const d of S.config.destinations){
    const x=st[p.id+'|'+d.id]||{};let big='…',sub='waiting for first calculation';
    if(x.minutes!=null){big=x.arrived?'Here':Math.round(x.minutes)+' min';
      sub=(x.arrived?'arrived':`${x.distance} ${x.unit}${x.via?' via '+esc(x.via):''}`)+' · '+ago(x.t)}
    if(x.error)sub=`<span class="err">${esc(x.error)}</span>`+(x.minutes!=null?' · last good '+ago(x.t):'');
    out.push(`<div class="card live"><div><b>${esc(p.name)} → ${esc(d.name)}</b><div class="sub">${sub}</div></div><div class="big">${big}</div></div>`);
  }
  $('#live').innerHTML=out.join('')||'<div class="sub">Add a person and a destination, hit Save.</div>';
}
function addPerson(){S.config.people.push({id:'',name:'',entity:''});renderPeople()}
function addDest(){S.config.destinations.push({id:'',name:'',target:''});renderDests()}
async function save(){
  $('#msg').textContent='Saving…';
  await fetch('api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(S.config)});
  await load();$('#msg').innerHTML='<span class="ok">Saved ✓</span>';setTimeout(()=>$('#msg').textContent='',2500);
}
async function refreshNow(){await fetch('api/refresh',{method:'POST'});$('#msg').textContent='Recalculating…'}
setInterval(async()=>{if(!S)return;try{const r=await fetch('api/status');S.status=await r.json();renderLive(S.status)}catch(e){}},5000);
load();
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
