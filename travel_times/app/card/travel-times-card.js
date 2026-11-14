/* Travel Times card for Home Assistant
 * Shows every Travel Times sensor (routes, groups, trackers) in one compact, scrollable, resizable card.
 * Every option below can be set in the card's visual editor (no YAML needed) or in YAML.
 *
 *   type: custom:travel-times-card
 *
 *  CONTENT
 *   title: Travel Times        # show_title: false hides it
 *   layout: list               # list | tiles
 *   group_by: person           # person | place | none   (list layout)
 *   sort: name                 # name | minutes
 *   people: [Jon, MJ]          # optional filter (names as shown in Travel Times)
 *   places: [Home, Work]       # optional filter
 *   entities: [sensor.jon_to_work]   # optional: only these sensors
 *   show_groups / show_trackers / show_distance / show_via / show_members: true | false
 *   warn_above: 30             # minutes: value turns orange
 *   alert_above: 60            # minutes: value turns red
 *
 *  SIZE (numbers are px; text like "50%", "30vw", "20em" also works in YAML)
 *   width / min_width / max_width
 *   height / min_height / max_height
 *   aspect_ratio: "16:9"       # height follows the width (ignored when height is set)
 *   align: left                # left | center | right   (when the card is narrower than its space)
 *   fit: scroll                # scroll | shrink (text shrinks to fit the box) | clip
 *   min_font_scale: 0.5        # smallest text size "shrink" may use
 *
 *  TEXT & SPACING
 *   font_size: 14              # px, base text size
 *   font_scale: 1              # multiplier on top of font_size (TV friendly: 1.3)
 *   title_scale: 1             # multiplier for the title
 *   value_scale: 1             # multiplier for the big minutes number
 *   padding: 8                 # px, space inside the card
 *   gap: 8                     # px, space between rows / tiles
 *   row_height: 40             # px, minimum height of each row / tile
 *   border_radius: 12          # px
 *
 *  TILES
 *   columns: 3                 # fixed number of columns
 *   tile_min_width: 136        # px, used when columns is not set
 *
 *  LOOK
 *   transparent: false         # no card background
 *   hide_scrollbar: false
 */
const TT_CARD_VERSION = "1.1.0";

const ttEsc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const ttNum = (v) => { const n = parseFloat(v); return Number.isFinite(n) ? n : NaN; };
const ttLow = (v) => String(v ?? "").trim().toLowerCase();
const ttEmpty = (v) => v === undefined || v === null || v === "" || (typeof v === "number" && !Number.isFinite(v)) || (Array.isArray(v) && !v.length);

// "400" or 400 -> "400px"; "50%", "30vw", "20em" pass through; anything else -> null
const ttSize = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const s = String(v).trim();
  if (/^\d+(\.\d+)?$/.test(s)) return `${s}px`;
  if (/^\d+(\.\d+)?(px|%|vw|vh|em|rem)$/.test(s)) return s;
  return null;
};
// px number only (padding, gap ...)
const ttPx = (v) => { const n = ttNum(v); return Number.isFinite(n) && n >= 0 ? n : null; };
// "16:9", "16/9", 1.78 -> "16 / 9" or "1.78"
const ttRatio = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const m = String(v).trim().match(/^(\d+(?:\.\d+)?)\s*[:/x]\s*(\d+(?:\.\d+)?)$/i);
  if (m && Number(m[1]) > 0 && Number(m[2]) > 0) return `${m[1]} / ${m[2]}`;
  const n = ttNum(v);
  return Number.isFinite(n) && n > 0 ? String(n) : null;
};
const ttPos = (v, d) => { const n = ttNum(v); return Number.isFinite(n) && n > 0 ? n : d; };

// Defaults. The editor drops any value equal to its default, so your YAML stays short.
const TT_DEFAULTS = {
  title: "Travel Times", show_title: true, layout: "list", group_by: "person", sort: "name",
  show_distance: true, show_via: false, show_groups: true, show_trackers: true, show_members: true,
  warn_above: null, alert_above: null,
  fit: "scroll", align: "left", min_font_scale: 0.5,
  font_size: 14, font_scale: 1, title_scale: 1, value_scale: 1,
  transparent: false, hide_scrollbar: false,
  width: null, min_width: null, max_width: null, height: null, min_height: null, max_height: null,
  aspect_ratio: null, padding: null, gap: null, row_height: null, border_radius: null,
  columns: null, tile_min_width: null,
  people: null, places: null, entities: null,
};
const TT_KEYS = Object.keys(TT_DEFAULTS);

class TravelTimesCard extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._sig = "";
    this._count = 0;
    this._ro = null;
  }

  connectedCallback() { this._observe(); }
  disconnectedCallback() { if (this._ro) { this._ro.disconnect(); this._ro = null; } }

  setConfig(config) {
    if (config.entities && !Array.isArray(config.entities)) throw new Error("entities must be a list");
    this._config = { ...TT_DEFAULTS, ...config };
    this._build();
    this._sig = "";
    if (this._hass) this._update();
  }

  set hass(hass) {
    this._hass = hass;
    this._update();
  }

  _fixedHeight() {
    const c = this._config || {};
    return !!(ttSize(c.height) || ttRatio(c.aspect_ratio));
  }

  getCardSize() {
    const c = this._config || {};
    const h = ttSize(c.height);
    if (h && h.endsWith("px")) return Math.max(1, Math.ceil(parseFloat(h) / 50));
    let rows = Math.max(2, Math.ceil(this._count / 2) + 1);
    const mh = ttSize(c.max_height);
    if (mh && mh.endsWith("px")) rows = Math.min(rows, Math.ceil(parseFloat(mh) / 50));
    return rows;
  }

  // Sections view. Without a px height / ratio you drag the card to any size (the content scrolls or shrinks).
  // With a px height or aspect ratio the card sizes itself, so the grid row count becomes "auto".
  getGridOptions() {
    if (this._fixedHeight()) return { columns: 12, rows: "auto", min_columns: 3 };
    return { columns: 12, rows: 4, min_columns: 3, min_rows: 2 };
  }

  static getStubConfig() { return {}; }
  static getConfigElement() { return document.createElement("travel-times-card-editor"); }

  _build() {
    const c = this._config;
    const w = ttSize(c.width), minw = ttSize(c.min_width), maxw = ttSize(c.max_width);
    const h = ttSize(c.height), minh = ttSize(c.min_height), maxh = ttSize(c.max_height);
    const ar = ttRatio(c.aspect_ratio);
    const fixedH = !!(h || ar);
    const align = c.align === "center" ? "0 auto" : c.align === "right" ? "0 0 0 auto" : "0 auto 0 0";
    const pad = ttPx(c.padding);
    const gap = ttPx(c.gap);
    const rh = ttPx(c.row_height);
    const br = ttPx(c.border_radius);
    const tw = ttPos(c.tile_min_width, null);
    const ts = ttPos(c.title_scale, 1), vs = ttPos(c.value_scale, 1);
    const cardCss = [
      w ? `width:${w}` : "",
      `max-width:${maxw || "100%"}`,
      minw ? `min-width:${minw}` : "",
      h ? `height:${h}` : (ar ? `aspect-ratio:${ar};height:auto` : "height:100%"),
      minh ? `min-height:${minh}` : "",
      maxh ? `max-height:${maxh}` : "",
      (w || maxw) ? `margin:${align}` : "",
      br !== null ? `border-radius:${br}px` : "",
      c.transparent ? "background:none;box-shadow:none;border:none" : "",
    ].filter(Boolean).join(";");
    const bodyPad = pad !== null ? `${pad}px` : "6px 8px 10px";
    const headPad = pad !== null ? `${pad}px ${pad}px 0` : "10px 14px 0";
    this.shadowRoot.innerHTML = `
      <style>
        :host{display:block;${fixedH ? "" : "height:100%"}}
        ha-card{box-sizing:border-box;display:flex;flex-direction:column;overflow:hidden;
          --tt-fit:1;
          font-size:calc(${ttPos(c.font_size, 14)}px * ${ttPos(c.font_scale, 1)} * var(--tt-fit));
          ${cardCss}}
        .hd{padding:${headPad};font-size:calc(1.15em * ${ts});font-weight:500;color:var(--primary-text-color);flex:none}
        .bd{flex:1 1 auto;min-height:0;overflow-y:${c.fit === "clip" ? "hidden" : "auto"};padding:${bodyPad};
          scrollbar-width:${c.hide_scrollbar ? "none" : "thin"};scrollbar-color:var(--divider-color) transparent}
        ${c.hide_scrollbar ? ".bd::-webkit-scrollbar{display:none}" : ""}
        .sec{font-size:.72em;letter-spacing:.08em;text-transform:uppercase;color:var(--secondary-text-color);
          margin:8px 6px 2px;display:flex;align-items:center;gap:6px}
        .sec:first-child{margin-top:2px}
        .row{display:flex;justify-content:space-between;align-items:center;gap:.7em;padding:.36em .57em;
          border-radius:10px;cursor:pointer;${rh !== null ? `min-height:${rh}px;box-sizing:border-box;` : ""}${gap !== null ? `margin-bottom:${gap}px;` : ""}}
        .row:hover,.tile:hover{background:rgba(127,127,127,.14)}
        .l{min-width:0}
        .t{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:500;color:var(--primary-text-color)}
        .s{color:var(--secondary-text-color);font-size:.78em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .v{font-size:calc(1.35em * ${vs});font-weight:700;color:var(--primary-color);white-space:nowrap;text-align:right}
        .v i{font-size:.55em;font-style:normal;font-weight:500;color:var(--secondary-text-color);margin-left:2px}
        .arrived .v{color:var(--success-color,#4caf50)}
        .warn .v{color:var(--warning-color,#ffa600)}
        .alert .v{color:var(--error-color,#db4437)}
        .tiles{display:grid;gap:${gap !== null ? gap : 8}px;padding:4px 2px;
          grid-template-columns:${c.columns ? `repeat(${Math.max(1, Math.floor(Number(c.columns)))},minmax(0,1fr))` : `repeat(auto-fill,minmax(${tw ? tw + "px" : "8.5em"},1fr))`}}
        .tile{background:rgba(127,127,127,.1);border-radius:12px;padding:.57em .71em;cursor:pointer;min-width:0;
          ${rh !== null ? `min-height:${rh}px;box-sizing:border-box;display:flex;flex-direction:column;justify-content:center;` : ""}}
        .tile .v{text-align:left;margin-top:2px}
        .empty{padding:18px 12px;color:var(--secondary-text-color);text-align:center}
      </style>
      <ha-card>
        ${c.show_title && c.title ? `<div class="hd">${ttEsc(c.title)}</div>` : ""}
        <div class="bd"></div>
      </ha-card>`;
    this._observe();
  }

  // "shrink" mode: re-fit the text whenever the card's size changes (drag in sections view, window resize, TV).
  _observe() {
    if (this._ro) { this._ro.disconnect(); this._ro = null; }
    if (!this._config || this._config.fit !== "shrink" || !this.isConnected) return;
    const card = this.shadowRoot.querySelector("ha-card");
    if (!card || typeof ResizeObserver === "undefined") return;
    let last = "";
    this._ro = new ResizeObserver(() => {
      const key = `${card.clientWidth}x${card.clientHeight}`;
      if (key === last) return;
      last = key;
      requestAnimationFrame(() => this._fit());
    });
    this._ro.observe(card);
  }

  _fit() {
    const c = this._config;
    const card = this.shadowRoot.querySelector("ha-card");
    const bd = this.shadowRoot.querySelector(".bd");
    if (!card || !bd) return;
    card.style.setProperty("--tt-fit", "1");
    if (c.fit !== "shrink") return;
    const fits = () => bd.scrollHeight <= bd.clientHeight + 1;
    if (fits()) return;
    let lo = Math.min(1, ttPos(c.min_font_scale, 0.5)), hi = 1;
    for (let i = 0; i < 8; i++) {
      const mid = (lo + hi) / 2;
      card.style.setProperty("--tt-fit", String(mid));
      if (fits()) lo = mid; else hi = mid;
    }
    card.style.setProperty("--tt-fit", String(lo));
  }

  _collect() {
    const c = this._config;
    const wanted = c.entities ? new Set(c.entities.map((e) => (typeof e === "string" ? e : e.entity))) : null;
    const pf = c.people ? c.people.map(ttLow) : null;
    const lf = c.places ? c.places.map(ttLow) : null;
    const items = [];
    for (const [id, st] of Object.entries(this._hass.states)) {
      const a = st.attributes || {};
      const kind = a.tt_kind;
      if (!kind) continue;
      if (wanted && !wanted.has(id)) continue;
      if (kind === "tracker" && a.unit_of_measurement !== "min") continue; // skip the distance twin
      if (kind === "group" && !c.show_groups) continue;
      if (kind === "tracker" && !c.show_trackers) continue;
      const it = {
        id, kind, from: a.tt_from || "", to: a.tt_to || "", min: ttNum(st.state),
        arrived: a.arrived === true, dist: a.distance, unit: a.distance_unit || "",
        via: a.route || "", members: Array.isArray(a.members) ? a.members : [],
        pairs: Array.isArray(a.pairs) ? a.pairs : [],
      };
      if (pf) {
        const names = [it.from, ...it.members.map((m) => m.name), ...it.pairs.flatMap((p) => [p.a, p.b])].map(ttLow);
        if (!names.some((n) => pf.includes(n))) continue;
      }
      if (lf && (kind === "tracker" || !lf.includes(ttLow(it.to)))) continue;
      items.push(it);
    }
    return items;
  }

  _fmt(it) {
    if (it.arrived) return "Here";
    const m = it.min;
    if (!Number.isFinite(m)) return "—";
    if (m < 1) return "<1<i>min</i>";
    if (m < 60) return `${Math.round(m)}<i>min</i>`;
    const h = Math.floor(m / 60), r = Math.round(m % 60);
    return r ? `${h}<i>h</i> ${r}<i>m</i>` : `${h}<i>h</i>`;
  }

  _cls(it) {
    const c = this._config;
    if (it.arrived) return "arrived";
    if (c.alert_above != null && it.min >= c.alert_above) return "alert";
    if (c.warn_above != null && it.min >= c.warn_above) return "warn";
    return "";
  }

  _sub(it) {
    const c = this._config, p = [];
    if (it.kind === "route") {
      if (c.show_distance && it.dist != null && !it.arrived) p.push(`${it.dist} ${it.unit}`);
      if (c.show_via && it.via && !it.arrived) p.push(`via ${it.via}`);
    } else if (it.kind === "group") {
      if (c.show_members) p.push(...it.members.map((m) => `${m.name} ${m.arrived ? "here" : Math.round(m.minutes) + "m"}`));
    } else if (it.kind === "tracker") {
      if (it.dist != null) p.push(`${it.dist} ${it.unit} apart`);
    }
    return p.join(" · ");
  }

  _rowTitle(it, byPlace) {
    if (it.kind === "tracker") return it.from;
    return byPlace ? it.from : it.to;
  }

  _update() {
    if (!this._hass || !this._config) return;
    const items = this._collect();
    const sig = JSON.stringify(items);
    if (sig === this._sig) return;
    this._sig = sig;
    this._count = items.length;
    this._render(items);
  }

  _render(items) {
    const c = this._config;
    const bd = this.shadowRoot.querySelector(".bd");
    if (!items.length) {
      bd.innerHTML = '<div class="empty">No Travel Times sensors found yet.<br>Add routes in the Travel Times add-on and save.</div>';
      requestAnimationFrame(() => this._fit());
      return;
    }
    const cmp = (a, b) => c.sort === "minutes"
      ? (a.min || 0) - (b.min || 0)
      : (a.from + a.to).localeCompare(b.from + b.to, undefined, { sensitivity: "base" });
    items.sort(cmp);
    const row = (it, title) => `<div class="row ${this._cls(it)}" data-e="${ttEsc(it.id)}"><div class="l"><div class="t">${ttEsc(title)}</div>${
      this._sub(it) ? `<div class="s">${ttEsc(this._sub(it))}</div>` : ""}</div><div class="v">${this._fmt(it)}</div></div>`;
    let html = "";
    if (c.layout === "tiles") {
      html = '<div class="tiles">' + items.map((it) => {
        const t = it.kind === "tracker" ? `↔ ${it.from}` : `${it.kind === "group" ? "👥 " : ""}${it.from} → ${it.to}`;
        return `<div class="tile ${this._cls(it)}" data-e="${ttEsc(it.id)}"><div class="t">${ttEsc(t)}</div><div class="v">${this._fmt(it)}</div>${
          this._sub(it) ? `<div class="s">${ttEsc(this._sub(it))}</div>` : ""}</div>`;
      }).join("") + "</div>";
    } else if (c.group_by === "none") {
      html = items.map((it) => row(it, it.kind === "tracker" ? `↔ ${it.from}` : `${it.from} → ${it.to}`)).join("");
    } else {
      const byPlace = c.group_by === "place";
      const secs = new Map();
      for (const it of items) {
        const key = it.kind === "tracker" ? "Between people" : (byPlace ? it.to : it.from);
        const label = it.kind === "tracker" ? key : (it.kind === "group" && !byPlace ? `👥 ${key}` : key);
        if (!secs.has(key)) secs.set(key, { label, rows: [] });
        secs.get(key).rows.push(it);
      }
      const order = [...secs.keys()].sort((a, b) => (a === "Between people") - (b === "Between people") || a.localeCompare(b, undefined, { sensitivity: "base" }));
      html = order.map((k) => `<div class="sec">${ttEsc(secs.get(k).label)}</div>` +
        secs.get(k).rows.map((it) => row(it, this._rowTitle(it, byPlace))).join("")).join("");
    }
    bd.innerHTML = html;
    bd.querySelectorAll("[data-e]").forEach((n) => {
      n.onclick = () => this.dispatchEvent(new CustomEvent("hass-more-info", { detail: { entityId: n.dataset.e }, bubbles: true, composed: true }));
    });
    requestAnimationFrame(() => this._fit());
  }
}

/* ------------------------------------------------------------------ visual editor */
const TT_LABELS = {
  title: "Title", show_title: "Show title", layout: "Layout", group_by: "Group by (list)", sort: "Sort by",
  people: "Only these people", places: "Only these places", entities: "Only these sensors",
  show_groups: "Show groups", show_trackers: "Show trackers (between people)", show_distance: "Show distance",
  show_via: "Show road name", show_members: "Show each person inside a group",
  warn_above: "Orange above (minutes)", alert_above: "Red above (minutes)",
  width: "Width", min_width: "Min width", max_width: "Max width",
  height: "Height", min_height: "Min height", max_height: "Max height",
  aspect_ratio: "Aspect ratio", align: "Align (when narrower than its space)",
  fit: "When content is too big", min_font_scale: "Smallest text when shrinking",
  font_size: "Base text size", font_scale: "Text size multiplier", title_scale: "Title size multiplier",
  value_scale: "Minutes number size multiplier", padding: "Padding inside card", gap: "Space between rows / tiles",
  row_height: "Row / tile min height", border_radius: "Corner radius",
  columns: "Tile columns", tile_min_width: "Tile min width",
  transparent: "Transparent background", hide_scrollbar: "Hide scrollbar",
};
const TT_HELP = {
  width: "px. Leave empty to fill the available width. YAML also accepts 50%, 30vw …",
  height: "px. Leave empty to use the dashboard grid size (sections view) or the content height.",
  max_height: "px. Card stops growing here and scrolls (or shrinks).",
  aspect_ratio: "Height follows the width. Ignored when a height is set.",
  fit: "Scroll adds a scrollbar, Shrink makes the text smaller until everything fits, Clip hides the rest.",
  min_font_scale: "0.5 = half size. Only used when shrinking.",
  font_size: "px. Default 14.",
  font_scale: "1 = normal, 1.3 = bigger for a TV.",
  columns: "Leave empty for automatic columns.",
  tile_min_width: "px. Used when Tile columns is empty.",
  people: "Names as shown in the Travel Times add-on. Empty = everyone.",
  places: "Empty = all places.",
  entities: "Pick exact sensors. Empty = all Travel Times sensors.",
};

class TravelTimesCardEditor extends HTMLElement {
  setConfig(config) {
    this._config = { ...config };
    this._render(true);
  }

  set hass(hass) {
    this._hass = hass;
    const sig = this._optionSig();
    if (this._form && sig === this._optSig) { this._form.hass = hass; return; }
    this._render(true);
  }

  _options() {
    const people = new Set(), places = new Set(), ents = [];
    for (const [id, st] of Object.entries((this._hass && this._hass.states) || {})) {
      const a = st.attributes || {};
      if (!a.tt_kind) continue;
      if (a.tt_kind === "tracker" && a.unit_of_measurement !== "min") continue;
      ents.push(id);
      if (a.tt_from) people.add(a.tt_from);
      if (a.tt_kind !== "tracker" && a.tt_to) places.add(a.tt_to);
      (Array.isArray(a.members) ? a.members : []).forEach((m) => m && m.name && people.add(m.name));
    }
    const sort = (s) => [...s].sort((x, y) => x.localeCompare(y, undefined, { sensitivity: "base" }));
    return { people: sort(people), places: sort(places), ents: ents.sort() };
  }

  _optionSig() { return JSON.stringify(this._options()); }

  _schema() {
    const o = this._options();
    const px = (name, max = 4000, step = 1) => ({ name, selector: { number: { mode: "box", min: 0, max, step, unit_of_measurement: "px" } } });
    const mult = (name, min, max) => ({ name, selector: { number: { mode: "slider", min, max, step: 0.05 } } });
    const sel = (name, options) => ({ name, selector: { select: { mode: "dropdown", options } } });
    const multi = (name, list) => ({ name, selector: { select: { multiple: true, custom_value: true, options: list } } });
    const bool = (name) => ({ name, selector: { boolean: {} } });
    const grid = (schema) => ({ type: "grid", name: "", flatten: true, schema });
    const sect = (title, icon, schema, expanded = false) => ({ type: "expandable", name: "", title, icon, flatten: true, expanded, schema });
    return [
      sect("Content", "mdi:format-list-bulleted", [
        { name: "title", selector: { text: {} } },
        bool("show_title"),
        grid([
          sel("layout", [{ value: "list", label: "List" }, { value: "tiles", label: "Tiles" }]),
          sel("group_by", [{ value: "person", label: "Person" }, { value: "place", label: "Place" }, { value: "none", label: "No grouping" }]),
          sel("sort", [{ value: "name", label: "Name" }, { value: "minutes", label: "Minutes (shortest first)" }]),
        ]),
        grid([bool("show_groups"), bool("show_trackers"), bool("show_distance"), bool("show_via"), bool("show_members")]),
        grid([
          { name: "warn_above", selector: { number: { mode: "box", min: 0, max: 1440, step: 1, unit_of_measurement: "min" } } },
          { name: "alert_above", selector: { number: { mode: "box", min: 0, max: 1440, step: 1, unit_of_measurement: "min" } } },
        ]),
      ], true),
      sect("Filter", "mdi:filter-variant", [
        multi("people", o.people),
        multi("places", o.places),
        { name: "entities", selector: { entity: { multiple: true, include_entities: o.ents } } },
      ]),
      sect("Size & fit", "mdi:resize", [
        grid([px("width"), px("height")]),
        grid([px("min_width"), px("min_height")]),
        grid([px("max_width"), px("max_height")]),
        { name: "aspect_ratio", selector: { select: { mode: "dropdown", custom_value: true, options: [
          { value: "16:9", label: "16:9 (TV / widescreen)" }, { value: "16:10", label: "16:10" }, { value: "4:3", label: "4:3" },
          { value: "3:2", label: "3:2" }, { value: "1:1", label: "1:1 (square)" }, { value: "21:9", label: "21:9 (ultrawide)" },
          { value: "9:16", label: "9:16 (portrait)" }] } } },
        sel("align", [{ value: "left", label: "Left" }, { value: "center", label: "Center" }, { value: "right", label: "Right" }]),
        sel("fit", [{ value: "scroll", label: "Scroll" }, { value: "shrink", label: "Shrink text to fit" }, { value: "clip", label: "Clip (no scrolling)" }]),
        mult("min_font_scale", 0.2, 1),
      ], true),
      sect("Text & spacing", "mdi:format-size", [
        grid([px("font_size", 120), mult("font_scale", 0.4, 3)]),
        grid([mult("title_scale", 0.4, 3), mult("value_scale", 0.4, 3)]),
        grid([px("padding", 80), px("gap", 80)]),
        grid([px("row_height", 300), px("border_radius", 80)]),
      ]),
      sect("Tiles", "mdi:view-grid", [
        grid([
          { name: "columns", selector: { number: { mode: "box", min: 1, max: 12, step: 1 } } },
          px("tile_min_width", 800),
        ]),
      ]),
      sect("Look", "mdi:palette-outline", [bool("transparent"), bool("hide_scrollbar")]),
    ];
  }

  _data() {
    const d = { ...TT_DEFAULTS, ...this._config };
    for (const k of ["width", "min_width", "max_width", "height", "min_height", "max_height"]) {
      if (typeof d[k] === "string" && /^\d+(\.\d+)?(px)?$/.test(d[k].trim())) d[k] = parseFloat(d[k]);
    }
    if (Array.isArray(d.entities)) d.entities = d.entities.map((e) => (typeof e === "string" ? e : e && e.entity)).filter(Boolean);
    for (const k of TT_KEYS) if (d[k] === null) delete d[k];
    return d;
  }

  _render(force) {
    if (!this._config) return;
    if (!this._form) {
      this._form = document.createElement("ha-form");
      this._form.computeLabel = (s) => TT_LABELS[s.name] || s.title || s.name;
      this._form.computeHelper = (s) => TT_HELP[s.name] || "";
      this._form.addEventListener("value-changed", (ev) => this._changed(ev));
      this.appendChild(this._form);
    }
    this._optSig = this._optionSig();
    this._form.hass = this._hass;
    if (force) this._form.schema = this._schema();
    this._form.data = this._data();
  }

  _changed(ev) {
    ev.stopPropagation();
    if (!this._config) return;
    const v = ev.detail.value || {};
    const out = {};
    for (const k of Object.keys(this._config)) if (!TT_KEYS.includes(k)) out[k] = this._config[k]; // type, grid_options, visibility ...
    for (const k of TT_KEYS) {
      const val = v[k];
      if (ttEmpty(val) || val === TT_DEFAULTS[k]) continue;
      out[k] = val;
    }
    this._config = out;
    this.dispatchEvent(new CustomEvent("config-changed", { detail: { config: out }, bubbles: true, composed: true }));
  }
}

if (!customElements.get("travel-times-card")) customElements.define("travel-times-card", TravelTimesCard);
if (!customElements.get("travel-times-card-editor")) customElements.define("travel-times-card-editor", TravelTimesCardEditor);
window.customCards = window.customCards || [];
if (!window.customCards.some((x) => x.type === "travel-times-card")) {
  window.customCards.push({
    type: "travel-times-card",
    name: "Travel Times",
    description: "Compact, scrollable drive times for people, groups and trackers from the Travel Times add-on. Size it in px or by aspect ratio.",
    preview: false,
  });
}
console.info(`%c TRAVEL-TIMES-CARD %c ${TT_CARD_VERSION} `, "color:#fff;background:#03a9f4;font-weight:700", "color:#03a9f4");
