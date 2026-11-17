/* HA Theme Bridge v1.2.0
 * Makes an add-on web UI follow the active Home Assistant theme (colors, dark/light,
 * dashboard wallpaper, glass blur, card corners and shadows) and updates live when
 * the theme changes. It reads the theme from the Home Assistant page that holds the
 * add-on (ingress runs on the same origin) and remaps this page's own colors to it.
 * Opened outside Home Assistant it does nothing, so the page keeps its own look.
 * Wallpaper, first match wins: the add-on's own "wallpaper" option (Configuration tab), then the
 * shared add-on wallpaper picked in Photo Hub (/local/addon-wallpaper/wallpaper.json), then the theme's.
 * The add-on option:
 *   empty = use the theme's wallpaper, "none" = no wallpaper, or an image:
 *   /local/pic.jpg (= /config/www/pic.jpg), just pic.jpg, /config/www/..., or https://...
 * Opt out on a page: <html data-ha-theme-off>
 * Variables you can use in new pages: --hb-bg --hb-card --hb-text --hb-text2 --hb-line
 * --hb-primary --hb-accent --hb-on --hb-error --hb-warning --hb-success --hb-field,
 * plus every core HA variable (--primary-color, --card-background-color, ...).
 */
(function () {
  'use strict';
  if (window.__haThemeBridge) return;
  window.__haThemeBridge = 1;
  var doc = document, root = doc.documentElement;
  if (root.hasAttribute('data-ha-theme-off')) return;

  /* ---------- find the Home Assistant window above us ---------- */
  var HA = null;
  try {
    var w = window;
    while (w.parent && w.parent !== w) {
      w = w.parent;
      if (w.document && w.document.querySelector('home-assistant')) HA = w;
    }
  } catch (e) { /* cross-origin parent: stop climbing */ }
  if (!HA) return;
  var nested = window.parent !== HA;

  /* ---------- color helpers ---------- */
  function parse(c) {
    if (!c) return null;
    c = String(c).trim().toLowerCase();
    var m;
    if (c.charAt(0) === '#') {
      var h = c.slice(1);
      if (!/^[0-9a-f]+$/.test(h)) return null;
      if (h.length === 3 || h.length === 4) h = h.replace(/./g, '$&$&');
      if (h.length !== 6 && h.length !== 8) return null;
      return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16),
        h.length === 8 ? parseInt(h.slice(6, 8), 16) / 255 : 1];
    }
    if ((m = c.match(/^rgba?\(([^)]+)\)$/))) {
      var p = m[1].split(/[\s,\/]+/).filter(Boolean);
      if (p.length < 3) return null;
      var f = function (v, alpha) {
        return /%$/.test(v) ? parseFloat(v) * (alpha ? 0.01 : 2.55) : parseFloat(v);
      };
      var out = [f(p[0]), f(p[1]), f(p[2]), p.length > 3 ? f(p[3], 1) : 1];
      for (var i = 0; i < 4; i++) if (isNaN(out[i])) return null;
      return out;
    }
    if (c === 'white') return [255, 255, 255, 1];
    if (c === 'black') return [0, 0, 0, 1];
    if (c === 'transparent') return [0, 0, 0, 0];
    return null;
  }
  function info(c) {
    var r = c[0] / 255, g = c[1] / 255, b = c[2] / 255;
    var mx = Math.max(r, g, b), mn = Math.min(r, g, b), ch = mx - mn, h = 0;
    if (ch) {
      if (mx === r) h = ((g - b) / ch) % 6;
      else if (mx === g) h = (b - r) / ch + 2;
      else h = (r - g) / ch + 4;
      h *= 60;
      if (h < 0) h += 360;
    }
    return { l: (mx + mn) / 2, ch: ch, h: h, a: c[3] };
  }
  /* greys, plus the dark navy/indigo shades many dark UIs use as their "grey" */
  function isNeutral(i) {
    return i.ch < 0.12 || (i.ch < 0.2 && i.h >= 200 && i.h <= 290 && (i.l < 0.32 || i.l > 0.85));
  }
  function lum(c) { return c ? (0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]) / 255 : 0; }
  function rgbStr(c) { return Math.round(c[0]) + ',' + Math.round(c[1]) + ',' + Math.round(c[2]); }
  function hueRole(h) {
    return (h < 15 || h >= 335) ? 'error' : h < 65 ? 'warning' : h < 170 ? 'success' : 'primary';
  }
  function tint(role, a) { return 'rgba(var(--hb-' + role + '-rgb),' + (Math.round(a * 1000) / 1000) + ')'; }
  function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

  var probe = null;
  function toRGB(v) {
    v = (v || '').trim();
    if (!v) return null;
    var p = parse(v);
    if (p) return p;
    if (!probe) {
      probe = doc.createElement('i');
      probe.style.display = 'none';
      (doc.body || root).appendChild(probe);
    }
    probe.style.color = '';
    probe.style.color = v;
    if (!probe.style.color) return null;
    return parse(getComputedStyle(probe).color);
  }

  /* ---------- what the page looked like before we touched it ---------- */
  var REFS = { dark: { b: 0.08, t: 0.92 }, light: { b: 0.96, t: 0.12 } };
  var designScheme = 'dark';
  (function measure() {
    var el = doc.body || root, cs = getComputedStyle(el);
    var txt = parse(cs.color), bg = parse(cs.backgroundColor);
    if (!bg || bg[3] < 0.5) bg = parse(getComputedStyle(root).backgroundColor);
    if (txt) designScheme = info(txt).l > 0.5 ? 'dark' : 'light';
    else if (bg && bg[3] > 0.5) designScheme = info(bg).l < 0.5 ? 'dark' : 'light';
    var ref = REFS[designScheme];
    if (txt) ref.t = info(txt).l;
    if (bg && bg[3] > 0.5 && Math.abs(info(bg).l - ref.t) > 0.3) ref.b = info(bg).l;
  })();

  /* ---------- color rewriting rules ---------- */
  var COLOR_RE = /#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)|\b(?:white|black)\b/g;
  var cardVars = {}, solidVars = {}, pageVars = {};

  function lastCompound(sel) {
    return (sel || '').split(',').map(function (s) {
      var parts = s.trim().split(/\s*[\s>+~]\s*/);
      return parts[parts.length - 1] || '';
    });
  }
  function kindOf(sel) {
    var lasts = lastCompound(sel), k = '';
    for (var i = 0; i < lasts.length; i++) {
      var s = lasts[i].toLowerCase();
      if (/^(html|body|:root)\b/.test(s)) return 'page';
      if (/\.(overlay|backdrop|scrim|mask|shade|sheet|modal|lightbox)\b|::backdrop/.test(s)) return 'scrim';
      if (/^(input|select|textarea)\b|\.(input|field-input|textfield)\b/.test(s)) k = k || 'input';
      else if (/^(header|nav|footer)\b|\.(top|topbar|nav|navbar|header|footer|bar|toolbar|appbar|bottom|tabbar|savebar)\b/.test(s)) k = k || 'bar';
      else if (/\.(card|panel|surface|sheet|modal|dialog|popup|menu|toast|drawer|pane|tile)\b|^(dialog|details)\b/.test(s)) k = k || 'card';
    }
    return k;
  }
  function ctxOf(p) {
    if (/^background(-color|-image)?$/.test(p)) return 'bg';
    if (p === 'color' || p === 'caret-color' || p === '-webkit-text-fill-color' || p === 'text-decoration-color') return 'text';
    if (/^border(-(top|right|bottom|left|block|inline)(-(start|end))?)?-color$/.test(p) || p === 'outline-color' || p === 'column-rule-color') return 'line';
    if (p === 'accent-color') return 'accent';
    return '';
  }

  /* returns {v, solid, card} or null to leave the color alone */
  function mapColor(c, ctx, ref, kind) {
    var i = info(c), a = i.a, dark = ref.b < 0.5;
    if (a === 0) return null;
    var span = ref.t - ref.b, t = span ? (i.l - ref.b) / span : 0, at = Math.abs(t);
    var neutral = isNeutral(i) || (ctx === 'line' && i.ch < 0.26 && (dark ? i.l < 0.45 : i.l > 0.6));
    if (neutral) {
      if (a < 0.98) {
        if (t > 0.5) return { v: tint('text', a) };            /* light-on-dark wash -> text tint */
        if (ctx !== 'bg' || kind === 'scrim') return null;     /* overlays and shadows stay */
        if (i.l < 0.05 && a < 0.5) return { v: 'var(--hb-inset)' };  /* black wash = inset box */
        if (i.l < 0.05) return null;
        if (kind === 'bar' || a > 0.8) return { v: 'var(--hb-bar)', card: true };
        if (kind === 'card' || a > 0.4) return { v: 'var(--hb-card)', card: true };
        return { v: 'var(--hb-inset)' };
      }
      if (ctx === 'text') return { v: t > 0.72 ? 'var(--hb-text)' : 'var(--hb-text2)' };
      if (ctx === 'line') return { v: 'var(--hb-line)' };
      if (ctx === 'accent') return { v: 'var(--hb-primary)' };
      if (kind === 'page') return { v: 'var(--hb-bg)' };
      if (kind === 'input') return { v: 'var(--hb-field)' };
      if (kind === 'bar') return { v: 'var(--hb-bar)', card: true };
      if (kind === 'card' && t < 0.45) return { v: 'var(--hb-card)', card: true };
      if (at < 0.04) return { v: 'var(--hb-inset)' };
      if (t > 0.6) return null;                                 /* bright chip on dark page: leave */
      return { v: tint('text', clamp(0.03 + at * 0.4, 0.05, 0.16)) };
    }
    var role = hueRole(i.h);
    if (a < 0.98) return { v: tint(role, a) };
    if (ctx === 'bg') {
      if (dark ? i.l < 0.3 : i.l > 0.82) return { v: tint(role, 0.2) };   /* tinted note/chip bg */
      return { v: 'var(--hb-' + role + ')', solid: true };
    }
    return { v: 'var(--hb-' + role + ')' };
  }

  var SEM_NAMES = [
    [/^(ok|good|success|live|green|pos|positive)(-?\d)?$/, 'success'],
    [/^(bad|red|danger|err|error|neg|negative)(-?\d)?$/, 'error'],
    [/^(warn|warning|amber)(-?\d)?$/, 'warning'],
    [/^(acc|accent|ac|primary|brand|link)$/, 'primary'],
    [/^(acc|accent|ac|primary)-?2$/, 'accent']
  ];
  function mapVar(name, val, ref) {
    var n = name.slice(2).toLowerCase();
    if (/^hb-/.test(n)) return null;
    var c = parse(val);
    if (!c) return null;
    var i = info(c), neutral = isNeutral(i), dark = ref.b < 0.5;
    var span = ref.t - ref.b, t = span ? (i.l - ref.b) / span : 0, at = Math.abs(t);
    if (n === 'on' || /^(acc|accent|ac|primary)-?(ink|on|fg|text)$|^on-(acc|accent|primary)$/.test(n)) return { v: 'var(--hb-on)' };
    var soft = /(-|^)(soft|tint|wash|bg|bg2|weak)$|soft$/.test(n), base = n.replace(/-?(soft|tint|wash|bg|bg2|weak|ink)$|soft$/, '');
    for (var k = 0; k < SEM_NAMES.length; k++) {
      if (SEM_NAMES[k][0].test(base) && (!neutral || base !== n)) {
        var role = SEM_NAMES[k][1];
        if (/-ink$/.test(n)) return { v: 'var(--hb-text)' };
        if (soft || i.a < 0.98) return { v: tint(role === 'accent' ? 'primary' : role, i.a < 0.98 ? i.a : 0.16) };
        return { v: 'var(--hb-' + role + ')', solid: true };
      }
    }
    if (/-ink$/.test(n)) return { v: 'var(--hb-text)' };
    if (!neutral) return null;                                 /* unknown colorful var (state colors): leave */
    if (i.a < 0.98) return t > 0.5 ? { v: tint('text', i.a) } : null;
    if (/line|border|sep|divid|rule|stroke|outline/.test(n)) return { v: 'var(--hb-line)' };
    if (/^(mut|muted|dim|mu|sub|sec|secondary|faint|hint|txt2|text2|tx2|ink2|subtle)(-?\d)?$/.test(n)) return { v: 'var(--hb-text2)' };
    if (/^(text|txt|tx|fg|fore|ink|c|font-color|text-color)(-?\d)?$/.test(n) && t > 0.3) return { v: t > 0.72 ? 'var(--hb-text)' : 'var(--hb-text2)' };
    if (/field|input|code|well/.test(n)) return { v: 'var(--hb-field)' };
    if (/chip|card-?2|panel-?2|surface-?2|bg-?3|raised|hover|alt|elev/.test(n)) return { v: tint('text', dark ? 0.08 : 0.06) };
    if (/card|panel|surface|sheet|tile|box|pane/.test(n)) return { v: 'var(--hb-card)', card: true };
    if (/^(bg|bg-?[0-2]|background|base|page|canvas|app-bg|ink)$/.test(n) || at < 0.04) return { v: 'var(--hb-bg)' };
    if (t > 0.72) return { v: 'var(--hb-text)' };
    if (t > 0.4) return { v: 'var(--hb-text2)' };
    return { v: tint('text', clamp(0.03 + at * 0.4, 0.05, 0.14)) };
  }

  function refVarNames(v) {
    var out = [], re = /var\(\s*(--[\w-]+)/g, m;
    while ((m = re.exec(v || ''))) out.push(m[1]);
    return out;
  }

  function doStyle(st, sel, scheme) {
    var ref = REFS[scheme], kind = kindOf(sel), names = [], k;
    for (k = 0; k < st.length; k++) names.push(st[k]);
    var solidBg = false, isCard = false;
    names.forEach(function (p) {
      var v = st.getPropertyValue(p), pr = st.getPropertyPriority(p);
      if (p.indexOf('--') === 0) {
        var r = mapVar(p, v, ref);
        pageVars[p] = 1;
        if (r) {
          if (r.card) cardVars[p] = 1;
          if (r.solid) solidVars[p] = 1;
          st.setProperty(p, r.v, pr);
        }
        return;
      }
      var ctx = ctxOf(p);
      if (!ctx || !v) return;
      var nv = v.replace(COLOR_RE, function (tok) {
        var c = parse(tok);
        if (!c) return tok;
        var r = mapColor(c, ctx, ref, kind);
        if (!r) return tok;
        if (ctx === 'bg' && r.solid) solidBg = true;
        if (ctx === 'bg' && r.card) isCard = true;
        return r.v;
      });
      if (nv !== v) st.setProperty(p, nv, pr);
    });
    /* shorthands that mix var() with literal colors only show up on the shorthand itself */
    ['background', 'border', 'border-top', 'border-right', 'border-bottom', 'border-left', 'outline'].forEach(function (p) {
      var v = st.getPropertyValue(p);
      if (!v || v.indexOf('var(') < 0) return;
      var ctx = p === 'background' ? 'bg' : 'line', pr = st.getPropertyPriority(p);
      var nv = v.replace(COLOR_RE, function (tok) {
        var c = parse(tok), r = c && mapColor(c, ctx, REFS[scheme], kind);
        if (!r) return tok;
        if (ctx === 'bg' && r.card) isCard = true;
        return r.v;
      });
      if (nv !== v) st.setProperty(p, nv, pr);
    });
    /* backgrounds written as var(--card) / var(--accent) */
    ['background', 'background-color', 'background-image'].forEach(function (p) {
      refVarNames(st.getPropertyValue(p)).forEach(function (n) {
        if (cardVars[n]) isCard = true;
        if (solidVars[n]) solidBg = true;
      });
    });
    if (solidBg) {
      var col = st.getPropertyValue('color');
      if (!col || /var\(--hb-text2?\)/.test(col) || refVarNames(col).some(function (n) {
        return /^--(text|txt|tx|ink|fg|c)$/.test(n);
      })) st.setProperty('color', 'var(--hb-on)');
    }
    if (isCard && kind !== 'page') {
      st.setProperty('-webkit-backdrop-filter', 'var(--hb-blur)');
      st.setProperty('backdrop-filter', 'var(--hb-blur)');
      ['top-left', 'top-right', 'bottom-right', 'bottom-left'].forEach(function (c) {
        var p = 'border-' + c + '-radius', v = st.getPropertyValue(p);
        if (v && v.indexOf('--hb-radius') < 0) st.setProperty(p, 'var(--hb-radius,' + v + ')');
      });
      var sh = st.getPropertyValue('box-shadow');
      if (sh.indexOf('--hb-shadow') < 0) st.setProperty('box-shadow', 'var(--hb-shadow,' + (sh || 'none') + ')');
      ['top', 'right', 'bottom', 'left'].forEach(function (s) {
        var p = 'border-' + s + '-color', v = st.getPropertyValue(p);
        if (v === 'var(--hb-line)') st.setProperty(p, 'var(--hb-card-border,var(--hb-line))');
        var pw = 'border-' + s + '-width', w = st.getPropertyValue(pw);
        if (w && w.indexOf('--hb-') < 0) st.setProperty(pw, 'var(--hb-card-border-width,' + w + ')');
      });
    }
  }

  function schemeOfMedia(text, fallback) {
    var m = /prefers-color-scheme\s*:\s*(dark|light)/.exec(text || '');
    return m ? m[1] : fallback;
  }
  function walk(rules, scheme) {
    for (var i = 0; i < rules.length; i++) {
      var r = rules[i];
      if (r.style && r.selectorText != null) {
        doStyle(r.style, r.selectorText, scheme);
        if (r.cssRules && r.cssRules.length) walk(r.cssRules, scheme);
      } else if (r.cssRules && !(r.type === 7)) {           /* media / supports / layer / container */
        walk(r.cssRules, r.media ? schemeOfMedia(r.media.mediaText, scheme) : scheme);
      }
    }
  }
  var done = typeof WeakSet === 'function' ? new WeakSet() : null, doneList = [];
  function seen(s) {
    if (done) { if (done.has(s)) return true; done.add(s); return false; }
    if (doneList.indexOf(s) >= 0) return true; doneList.push(s); return false;
  }
  function baseScheme(rules) {
    var dark = false, light = false;
    (function scan(rs) {
      for (var i = 0; i < rs.length; i++) {
        var r = rs[i];
        if (r.media) {
          var s = schemeOfMedia(r.media.mediaText, '');
          if (s === 'dark') dark = true;
          if (s === 'light') light = true;
        }
      }
    })(rules);
    if (dark && !light) return 'light';
    if (light && !dark) return 'dark';
    return designScheme;
  }
  function processSheets() {
    for (var i = 0; i < doc.styleSheets.length; i++) {
      var s = doc.styleSheets[i], rules;
      if (s.ownerNode && s.ownerNode.id === 'hb-style') continue;
      try { rules = s.cssRules; } catch (e) { continue; }   /* other-origin CSS (CDN): skip */
      if (!rules || seen(s)) continue;
      walk(rules, baseScheme(rules));
    }
  }

  /* ---------- read the HA theme and publish it as variables ---------- */
  var CORE = ['primary-color', 'dark-primary-color', 'light-primary-color', 'accent-color', 'text-primary-color',
    'primary-text-color', 'secondary-text-color', 'disabled-text-color', 'text-light-primary-color',
    'primary-background-color', 'secondary-background-color', 'card-background-color', 'ha-card-background',
    'divider-color', 'error-color', 'warning-color', 'success-color', 'info-color', 'state-icon-color',
    'paper-item-icon-color', 'sidebar-background-color', 'sidebar-text-color', 'app-header-background-color',
    'app-header-text-color', 'ha-card-border-radius', 'ha-card-box-shadow', 'ha-card-border-color',
    'ha-card-border-width', 'primary-font-family', 'paper-font-body1_-_font-family', 'lovelace-background',
    'input-fill-color', 'mdc-theme-primary', 'switch-checked-color', 'table-row-background-color',
    'table-row-alternative-background-color', 'code-editor-background-color', 'markdown-code-background-color'];
  var BLUR_VARS = ['ha-card-backdrop-filter', 'card-backdrop-filter', 'ha-card-backdrop', 'backdrop-filter',
    'glass-blur', 'ha-blur', 'blur', 'card-blur', 'ha-card-blur'];
  var MOD_VARS = ['card-mod-card', 'card-mod-card-yaml', 'card-mod-root', 'card-mod-view', 'card-mod-row'];
  var lastKey = '', copied = {}, wallOverride = null;   /* null = no add-on option set */
  var globalWall = null;                               /* null = no shared wallpaper from Photo Hub */

  function themeSource() {
    var d = HA.document;
    return d.querySelector('home-assistant') || d.documentElement;
  }
  function readTheme() {
    var cs = HA.getComputedStyle(themeSource());
    var g = function (n) { return (cs.getPropertyValue('--' + n) || '').trim(); };
    var th = { vars: {} };
    var names = CORE.slice(), i, st = HA.document.documentElement.style;
    for (i = 0; i < st.length; i++) if (st[i].indexOf('--') === 0) names.push(st[i].slice(2));
    for (i = 0; i < cs.length; i++) if (cs[i].indexOf('--') === 0 && names.length < 900) names.push(cs[i].slice(2));
    names.forEach(function (n) {
      var v = g(n);
      if (v && v.length < 400) th.vars['--' + n] = v;
    });
    var blur = '';
    for (i = 0; i < BLUR_VARS.length && !blur; i++) blur = g(BLUR_VARS[i]);
    for (i = 0; i < MOD_VARS.length && !blur; i++) {
      var m = /backdrop-filter\s*:\s*([^;}!\n]+)/i.exec(g(MOD_VARS[i]));
      if (m) blur = m[1].trim();
    }
    if (/^[\d.]+(px|rem|em)$/.test(blur)) blur = 'blur(' + blur + ')';
    if (/var\(/.test(blur)) blur = '';
    th.blur = blur;
    th.bg = toRGB(g('primary-background-color')) || [17, 17, 17, 1];
    th.bg2 = toRGB(g('secondary-background-color')) || th.bg;
    th.text = toRGB(g('primary-text-color')) || (lum(th.bg) < 0.5 ? [225, 225, 225, 1] : [33, 33, 33, 1]);
    th.text2 = toRGB(g('secondary-text-color')) || th.text;
    th.cardRaw = g('ha-card-background') || g('card-background-color');
    th.card = toRGB(th.cardRaw) || th.bg2;
    th.line = g('divider-color') || 'rgba(' + rgbStr(th.text) + ',.12)';
    th.primary = toRGB(g('primary-color')) || [3, 169, 244, 1];
    th.accent = toRGB(g('accent-color')) || th.primary;
    th.on = toRGB(g('text-primary-color')) || [255, 255, 255, 1];
    th.error = toRGB(g('error-color')) || [219, 68, 55, 1];
    th.warning = toRGB(g('warning-color')) || [255, 166, 0, 1];
    th.success = toRGB(g('success-color')) || [67, 160, 71, 1];
    th.info = toRGB(g('info-color')) || [3, 155, 229, 1];
    th.wall = g('lovelace-background');
    if (th.wall && !/url\(|gradient\(/.test(th.wall) && !toRGB(th.wall)) th.wall = '';
    if (wallOverride !== null) th.wall = wallOverride;
    else if (globalWall !== null) th.wall = globalWall;
    th.radius = g('ha-card-border-radius');
    th.shadow = g('ha-card-box-shadow');
    th.cardBorder = g('ha-card-border-color');
    th.cardBorderW = g('ha-card-border-width');
    th.dark = th.wall ? lum(th.text) > 0.5 : lum(th.bg) < 0.5;
    return th;
  }

  function set(n, v) {
    if (v == null || v === '') root.style.removeProperty(n);
    else root.style.setProperty(n, v);
  }
  function css(c) {
    return c[3] < 1 ? 'rgba(' + rgbStr(c) + ',' + (Math.round(c[3] * 1000) / 1000) + ')' : 'rgb(' + rgbStr(c) + ')';
  }

  function apply() {
    var th;
    try { th = readTheme(); } catch (e) { return; }
    var key = JSON.stringify(th);
    if (key === lastKey) return;
    lastKey = key;

    /* every HA variable, so new pages can just use var(--primary-color) */
    Object.keys(copied).forEach(function (n) { if (!th.vars[n]) root.style.removeProperty(n); });
    copied = {};
    Object.keys(th.vars).forEach(function (n) {
      if (pageVars[n]) return;
      root.style.setProperty(n, th.vars[n]);
      copied[n] = 1;
    });

    var cardSame = Math.abs(lum(th.card) - lum(th.bg)) < 0.01 && th.card[3] > 0.95 && !th.wall;
    set('--hb-bg', th.wall ? 'rgba(' + rgbStr(th.bg) + ',.45)' : css(th.bg));
    set('--hb-bg2', css(th.bg2));
    set('--hb-card', cardSame ? 'rgba(' + rgbStr(th.text) + ',.05)' : (th.cardRaw && !toRGB(th.cardRaw) ? th.cardRaw : css(th.card)));
    set('--hb-text', css(th.text));
    set('--hb-text2', css(th.text2));
    set('--hb-line', th.line);
    set('--hb-primary', css(th.primary));
    set('--hb-accent', css(th.accent));
    set('--hb-on', css(th.on));
    ['error', 'warning', 'success', 'info'].forEach(function (k) { set('--hb-' + k, css(th[k])); });
    ['text', 'bg', 'primary', 'accent', 'error', 'warning', 'success', 'info'].forEach(function (k) {
      set('--hb-' + k + '-rgb', rgbStr(th[k]));
    });
    set('--hb-field', 'rgba(' + rgbStr(th.text) + ',' + (th.dark ? '.07' : '.05') + ')');
    set('--hb-inset', 'rgba(' + rgbStr(th.bg) + ',' + (th.wall ? '.35' : '.6') + ')');
    set('--hb-bar', th.card[3] < 0.95 || th.wall ? 'var(--hb-card)' : 'rgba(' + rgbStr(th.bg) + ',.92)');
    set('--hb-blur', th.blur || 'none');
    set('--hb-radius', th.radius);
    set('--hb-shadow', th.shadow);
    set('--hb-card-border', th.cardBorder);
    set('--hb-card-border-width', th.cardBorderW);
    set('--hb-page', th.wall || css(th.bg));
    root.style.setProperty('color-scheme', th.dark ? 'dark' : 'light');
    root.classList.toggle('hb-dark', th.dark);
    root.classList.toggle('hb-light', !th.dark);
    root.classList.toggle('hb-wallpaper', !!th.wall);
    root.classList.toggle('hb-glass', !!th.blur);

    var s = doc.getElementById('hb-style');
    if (!s) {
      s = doc.createElement('style');
      s.id = 'hb-style';
      (doc.head || root).appendChild(s);
    }
    var page = nested && th.wall ? 'transparent' : 'var(--hb-page)';
    s.textContent =
      'html.hb-themed{background:' + page + ' !important;min-height:100%}' +
      'html.hb-themed body{background:transparent !important;color:var(--hb-text)}' +
      (th.wall ? 'html.hb-themed iframe{background:transparent !important}' : '') +
      'html.hb-themed ::placeholder{color:var(--hb-text2);opacity:.8}';
    root.classList.add('hb-themed');
  }

  /* ---------- per add-on wallpaper option ---------- */
  function wallCss(v) {
    v = String(v == null ? '' : v).trim();
    if (!v || /^(theme|default|auto)$/i.test(v)) return null;
    if (/^(none|off|no)$/i.test(v)) return '';
    if (/url\(|gradient\(/.test(v)) return v;
    v = v.replace(/^\/?(config|homeassistant)\/www\//, '/local/').replace(/^www\//, '/local/');
    if (!/^(https?:)?\/\//.test(v) && v.charAt(0) !== '/') v = '/local/' + v;
    return 'center / cover no-repeat fixed url("' + v.replace(/"/g, '%22') + '")';
  }
  function loadWallpaperOption() {
    var m = /^\/api\/hassio_ingress\/[^/]+\//.exec(location.pathname);
    var el = themeSource(), hass = el && el.hass;
    if (!m || !hass || !hass.callWS || !(hass.user && hass.user.is_admin)) return;
    var base = m[0], key = 'hb-addon:' + base, hint = window.__hbAddon || '';
    var call = function (ep) {
      return hass.callWS({ type: 'supervisor/api', endpoint: ep, method: 'get' });
    };
    var ingressOf = function (info) { return String((info && (info.ingress_url || info.ingress_entry)) || ''); };
    var useInfo = function (info) {
      var o = (info && info.options) || {};
      wallOverride = 'wallpaper' in o ? wallCss(o.wallpaper) : null;
      lastKey = '';
      apply();
    };
    var cached = '';
    try { cached = sessionStorage.getItem(key) || ''; } catch (e) {}
    var find = function () {
      return call('/addons').then(function (r) {
        var list = (r && r.addons) || r || [], slugs = list.map(function (a) { return a.slug; });
        if (hint) slugs.sort(function (a, b) {
          var ha = a === hint || a.slice(-hint.length - 1) === '_' + hint, hb = b === hint || b.slice(-hint.length - 1) === '_' + hint;
          return (hb ? 1 : 0) - (ha ? 1 : 0);
        });
        var i = 0;
        var next = function () {
          if (i >= slugs.length || i >= 60) return null;
          var slug = slugs[i++];
          return call('/addons/' + slug + '/info').then(function (info) {
            if (ingressOf(info) && base.indexOf(ingressOf(info).replace(/\/?$/, '/')) === 0) {
              try { sessionStorage.setItem(key, slug); } catch (e) {}
              return info;
            }
            return next();
          }, next);
        };
        return next();
      });
    };
    (cached ? call('/addons/' + cached + '/info').then(function (info) {
      return ingressOf(info) && base.indexOf(ingressOf(info).replace(/\/?$/, '/')) === 0 ? info : find();
    }, find) : find()).then(function (info) { if (info) useInfo(info); }, function () {});
  }

  /* ---------- shared add-on wallpaper (picked in Photo Hub) ---------- */
  function loadGlobalWallpaper() {
    if (!window.fetch) return;
    fetch('/local/addon-wallpaper/wallpaper.json?t=' + Date.now(), { cache: 'no-store', credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        var next = d && d.url ? wallCss(d.url) : null;
        if (next !== globalWall) { globalWall = next; lastKey = ''; apply(); }
      }, function () {});
  }

  /* ---------- run now, then follow changes ---------- */
  function run() { try { processSheets(); } catch (e) {} apply(); }
  run();
  var tm = null;
  function later() { clearTimeout(tm); tm = setTimeout(run, 120); }
  try {
    var mo = new HA.MutationObserver(later);
    mo.observe(HA.document.documentElement, { attributes: true, attributeFilter: ['style', 'class'] });
    mo.observe(themeSource(), { attributes: true, attributeFilter: ['style', 'class'] });
    if (HA.document.head) mo.observe(HA.document.head, { childList: true, subtree: true, characterData: true });
    var mq = HA.matchMedia('(prefers-color-scheme: dark)');
    if (mq.addEventListener) mq.addEventListener('change', later); else if (mq.addListener) mq.addListener(later);
  } catch (e) {}
  try {
    new MutationObserver(function (list) {
      for (var i = 0; i < list.length; i++) {
        for (var j = 0; j < list[i].addedNodes.length; j++) {
          var n = list[i].addedNodes[j];
          if (n.id === 'hb-style') continue;
          if (n.tagName === 'STYLE') later();
          if (n.tagName === 'LINK') n.addEventListener('load', later);
        }
      }
    }).observe(doc.head || root, { childList: true });
  } catch (e) {}
  doc.addEventListener('visibilitychange', function () { if (!doc.hidden) { later(); loadGlobalWallpaper(); } });
  window.addEventListener('load', later);
  try { loadWallpaperOption(); } catch (e) {}
  try { loadGlobalWallpaper(); } catch (e) {}
  window.haThemeBridge = { refresh: function () { lastKey = ''; run(); }, reloadWallpaper: loadGlobalWallpaper };
})();
