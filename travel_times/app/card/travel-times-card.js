/* Travel Times card for Home Assistant
 * Shows every Travel Times sensor (routes, groups, trackers) in one compact, scrollable, resizable card.
 *
 *   type: custom:travel-times-card
 *   title: Travel Times        # show_title: false hides it
 *   layout: list               # list | tiles
 *   group_by: person           # person | place | none   (list layout)
 *   sort: name                 # name | minutes
 *   people: [Jon, MJ]          # optional filter (names as shown in Travel Times)
 *   places: [Home, Work]       # optional filter
 *   entities: [sensor.jon_to_work]   # optional: only these sensors
 *   show_groups: true          # groups of people
 *   show_trackers: true        # "between people" trackers
 *   show_distance: true        # distance under each place
 *   show_via: false            # road name under each place
 *   show_members: true         # each person's time inside a group row
 *   warn_above: 30             # minutes: value turns orange
 *   alert_above: 60            # minutes: value turns red
 *   font_scale: 1              # 0.8 = smaller, 1.3 = bigger (TV friendly)
 *   columns: 3                 # tiles layout: fixed number of columns
 *   max_height: 320            # px, scrolls inside the card (masonry view)
 *   transparent: false         # no card background
 */
const TT_CARD_VERSION = "1.0.0";

const ttEsc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const ttNum = (v) => { const n = parseFloat(v); return Number.isFinite(n) ? n : NaN; };
const ttLow = (v) => String(v ?? "").trim().toLowerCase();

class TravelTimesCard extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._sig = "";
    this._count = 0;
  }

  setConfig(config) {
    if (config.entities && !Array.isArray(config.entities)) throw new Error("entities must be a list");
    this._config = {
      title: "Travel Times", show_title: true, layout: "list", group_by: "person", sort: "name",
      show_distance: true, show_via: false, show_groups: true, show_trackers: true, show_members: true,
      font_scale: 1, warn_above: null, alert_above: null, transparent: false, max_height: null, columns: null,
      ...config,
    };
    this._build();
    this._sig = "";
    if (this._hass) this._update();
  }

  set hass(hass) {
    this._hass = hass;
    this._update();
  }

  getCardSize() {
    let rows = Math.max(2, Math.ceil(this._count / 2) + 1);
    if (this._config && this._config.max_height) rows = Math.min(rows, Math.ceil(this._config.max_height / 50));
    return rows;
  }

  // Sections view: lets you drag the card bigger or smaller. The content scrolls if it doesn't fit.
  getGridOptions() {
    return { columns: 12, rows: 4, min_columns: 3, min_rows: 2 };
  }

  static getStubConfig() {
    return {};
  }

  _build() {
    const c = this._config;
    const mh = c.max_height ? `${Number(c.max_height)}px` : "none";
    this.shadowRoot.innerHTML = `
      <style>
        :host{display:block;height:100%}
        ha-card{height:100%;box-sizing:border-box;display:flex;flex-direction:column;overflow:hidden;
          font-size:calc(14px * ${Number(c.font_scale) || 1});
          ${c.transparent ? "background:none;box-shadow:none;border:none;" : ""}}
        .hd{padding:10px 14px 0;font-size:1.15em;font-weight:500;color:var(--primary-text-color)}
        .bd{flex:1 1 auto;min-height:0;overflow-y:auto;padding:6px 8px 10px;max-height:${mh};
          scrollbar-width:thin;scrollbar-color:var(--divider-color) transparent}
        .sec{font-size:.72em;letter-spacing:.08em;text-transform:uppercase;color:var(--secondary-text-color);
          margin:8px 6px 2px;display:flex;align-items:center;gap:6px}
        .sec:first-child{margin-top:2px}
        .row{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:5px 8px;
          border-radius:10px;cursor:pointer}
        .row:hover,.tile:hover{background:rgba(127,127,127,.14)}
        .l{min-width:0}
        .t{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:500;color:var(--primary-text-color)}
        .s{color:var(--secondary-text-color);font-size:.78em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .v{font-size:1.35em;font-weight:700;color:var(--primary-color);white-space:nowrap;text-align:right}
        .v i{font-size:.55em;font-style:normal;font-weight:500;color:var(--secondary-text-color);margin-left:2px}
        .arrived .v{color:var(--success-color,#4caf50)}
        .warn .v{color:var(--warning-color,#ffa600)}
        .alert .v{color:var(--error-color,#db4437)}
        .tiles{display:grid;gap:8px;padding:4px 2px;
          grid-template-columns:${c.columns ? `repeat(${Number(c.columns)},minmax(0,1fr))` : "repeat(auto-fill,minmax(8.5em,1fr))"}}
        .tile{background:rgba(127,127,127,.1);border-radius:12px;padding:8px 10px;cursor:pointer;min-width:0}
        .tile .v{text-align:left;margin-top:2px}
        .empty{padding:18px 12px;color:var(--secondary-text-color);text-align:center}
      </style>
      <ha-card>
        ${c.show_title && c.title ? `<div class="hd">${ttEsc(c.title)}</div>` : ""}
        <div class="bd"></div>
      </ha-card>`;
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
  }
}

if (!customElements.get("travel-times-card")) customElements.define("travel-times-card", TravelTimesCard);
window.customCards = window.customCards || [];
if (!window.customCards.some((x) => x.type === "travel-times-card")) {
  window.customCards.push({
    type: "travel-times-card",
    name: "Travel Times",
    description: "Compact, scrollable drive times for people, groups and trackers from the Travel Times add-on.",
    preview: false,
  });
}
console.info(`%c TRAVEL-TIMES-CARD %c ${TT_CARD_VERSION} `, "color:#fff;background:#03a9f4;font-weight:700", "color:#03a9f4");
