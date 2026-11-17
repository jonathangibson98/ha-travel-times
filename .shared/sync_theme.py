#!/usr/bin/env python3
"""Copy .shared/ha-theme.js into every add-on page so its web UI follows the Home Assistant theme.

Each add-on builds from its own folder, so each page carries its own copy of the script inside a
marked block. Edit .shared/ha-theme.js, then run this from the repository root:

    python3 .shared/sync_theme.py            # update every page that already has the block
    python3 .shared/sync_theme.py PATH ...   # also add the block to new pages (put right after <body>)
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
START = "<!-- ha-theme"
END = "<!-- /ha-theme -->"
BLOCK_RE = re.compile(re.escape(START) + r".*?" + re.escape(END), re.S)
SKIP_DIRS = {".git", "node_modules"}
SELF = {"ha-theme.js", "sync_theme.py"}
EXTS = (".html", ".htm", ".py", ".js")


def addon_slug(path):
    """Slug of the add-on folder a page belongs to (config.yaml/json above it), or ''."""
    d = os.path.dirname(os.path.abspath(path))
    while d.startswith(ROOT):
        for name in ("config.yaml", "config.yml", "config.json"):
            f = os.path.join(d, name)
            if os.path.isfile(f):
                m = re.search(r'["\']?slug["\']?\s*:\s*["\']?([a-z0-9_]+)', open(f, encoding="utf-8").read())
                if m:
                    return m.group(1)
        if d == ROOT:
            break
        d = os.path.dirname(d)
    return ""


def block(slug=""):
    js = open(os.path.join(HERE, "ha-theme.js"), encoding="utf-8").read().strip()
    m = re.search(r"HA Theme Bridge v([\d.]+)", js)
    ver = m.group(1) if m else "?"
    for bad in ("{{", "{%", "{#", '"""', "</script"):
        if bad in js:
            sys.exit("ha-theme.js must not contain %r (it is pasted into templates and Python strings)" % bad)
    hint = 'window.__hbAddon = "%s";\n' % slug if slug else ""
    return "%s v%s: copied from .shared/ha-theme.js, edit it there -->\n<script>\n%s%s\n</script>\n%s" % (
        START, ver, hint, js, END)


def update(path, text, new_block, add):
    if START in text:
        return BLOCK_RE.sub(lambda _m: new_block, text, count=1)
    if not add:
        return text
    m = re.search(r"<body\b[^>]*>", text)
    if not m:
        print("  no <body> tag, skipped: %s" % path)
        return text
    return text[:m.end()] + "\n" + new_block + "\n" + text[m.end():]


def main():
    block()  # validates the script once
    add = [os.path.abspath(p) for p in sys.argv[1:]]
    targets = set(add)
    for d, dirs, files in os.walk(ROOT):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
        for f in files:
            if f.endswith(EXTS) and not (d == HERE and f in SELF):
                p = os.path.join(d, f)
                try:
                    if START in open(p, encoding="utf-8").read():
                        targets.add(p)
                except (UnicodeDecodeError, OSError):
                    pass
    changed = 0
    for p in sorted(targets):
        text = open(p, encoding="utf-8").read()
        out = update(p, text, block(addon_slug(p)), p in add)
        if out != text:
            open(p, "w", encoding="utf-8").write(out)
            changed += 1
            print("  themed: %s" % os.path.relpath(p, ROOT))
    print("ha-theme synced (%d page%s changed)" % (changed, "" if changed == 1 else "s"))


if __name__ == "__main__":
    main()
