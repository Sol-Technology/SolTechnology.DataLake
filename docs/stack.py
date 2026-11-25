#!/usr/bin/env python3
"""The stack picture in the README: the logo's layers taken apart, one tile per technology.
Change a status in ROWS when a version ships, then run: python3 docs/stack.py
Writes docs/stack.png (2x, needs Chrome)."""
from pathlib import Path
import subprocess, tempfile

DONE, NEXT, LATER, OPEN = "done", "next", "later", "open"   # OPEN: the empty slot

CODE = "lake.Orders.Where(o => o.Date == yesterday).ReadAsync()"
ROWS = [
    ("TABLES", [("Folders of files", "hive partitions", DONE),
                ("In memory", "for tests", DONE),
                ("Iceberg", "read 0.3 · write 1.0", NEXT),
                ("Delta Lake", "later", LATER)]),
    ("FORMATS", [("Parquet", "read + write", DONE),
                 ("Avro", "read", DONE),
                 ("Arrow", "IPC files, ADBC results", DONE),
                 ("...", "", OPEN)]),
    ("STORAGE", [("Local disk", "", DONE),
                 ("S3", "AWS, MinIO, R2", DONE),
                 ("Azure", "Blob, ADLS Gen2, OneLake", DONE),
                 ("Google Cloud", "later", LATER)]),
]

DOCS = Path(__file__).resolve().parent
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

SOLAR, INK, WHITE, SLATE, MUTED = "#F59E0B", "#0B1F33", "#FFFFFF", "#4C6A8A", "#8FA3B7"
FONT = "Inter, 'Helvetica Neue', Arial, sans-serif"
MONO = "'SF Mono', Menlo, Consolas, monospace"
W, H = 1000, 630
CX, A, B, THICK = 170, 135, 50, 14      # plate centre x, half width, half height, thickness
TOP, STEP = 95, 135                     # first plate centre y, distance between plates
X0, CHIP_H, GAP = 350, 52, 8            # chips start here


def poly(points, **attrs):
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return f'<polygon points="{pts}" ' + " ".join(f'{k.replace("_", "-")}="{v}"' for k, v in attrs.items()) + "/>"


def tile(cy, a0, a1, status, across=False):
    """One strip of a plate with its two visible sides. Strips lie side by side from the left corner to the front one,
    or across (from the front to the back) so that neighbouring layers cross like in Jenga."""
    def p(s, t, dy=0):
        return CX - A + (s + t) * A, cy + (s - t) * B + dy
    if across:
        top = [p(0, a0), p(1, a0), p(1, a1), p(0, a1)]
        left = [p(0, a0), p(1, a0), p(1, a0, THICK), p(0, a0, THICK)]
        right = [p(1, a0), p(1, a1), p(1, a1, THICK), p(1, a0, THICK)]
    else:
        top = [p(a0, 0), p(a1, 0), p(a1, 1), p(a0, 1)]
        left = [p(a0, 0), p(a1, 0), p(a1, 0, THICK), p(a0, 0, THICK)]
        right = [p(a1, 0), p(a1, 1), p(a1, 1, THICK), p(a1, 0, THICK)]
    if status == DONE:
        looks = (dict(fill=SOLAR), dict(fill="#B45309"), dict(fill="#D97706"))
    elif status == "code":
        looks = (dict(fill=WHITE), dict(fill="#8FA3B7"), dict(fill="#C3CFDB"))
    else:
        line = dict(fill=INK, stroke=SLATE if status == LATER else SOLAR, stroke_width=1.6, stroke_linejoin="round")
        if status in (LATER, OPEN):
            line["stroke_dasharray"] = "4 4"
        looks = (dict(line, fill="#1A2C3A" if status == NEXT else INK), line, line)
    return "".join(poly(pts, **look) for pts, look in zip((left, right, top), (looks[1], looks[2], looks[0])))


def plate(cy, statuses, across=False):
    n, gap = len(statuses), 0.03
    tiles = [tile(cy, i / n + (gap if i else 0), (i + 1) / n - (gap if i < n - 1 else 0), s, across)
             for i, s in enumerate(statuses)]
    return "".join(reversed(tiles) if across else tiles)   # across: the back strip first, the front one on top


def text(x, y, value, size, fill, weight=400, font=FONT, extra=""):
    value = value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<text x="{x:.1f}" y="{y:.1f}" font-family="{font}" font-size="{size}" font-weight="{weight}" fill="{fill}" {extra}>{value}</text>'


def width(value, unit):
    """Close enough without font metrics: narrow letters count as half, m and w as a bit more."""
    return unit * sum(0.55 if c in "iltfjrI .,·1" else 1.25 if c in "mwMW" else 1 for c in value)


def chip(x, y, name, sub, status):
    w = max(width(name, 10.5), width(sub, 7.2), 34) + 26
    if status == DONE:
        box, name_fill, sub_fill = f'fill="{SOLAR}"', INK, "#5C3B02"
    elif status == NEXT:
        box, name_fill, sub_fill = f'fill="#1A2C3A" stroke="{SOLAR}" stroke-width="1.6"', SOLAR, "#F8C56A"
    elif status == OPEN:
        box, name_fill, sub_fill = f'fill="none" stroke="{SOLAR}" stroke-width="1.6" stroke-dasharray="4 4"', SOLAR, "#F8C56A"
    else:
        box, name_fill, sub_fill = f'fill="none" stroke="{SLATE}" stroke-width="1.6" stroke-dasharray="4 4"', "#B7C4D1", MUTED
    out = f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="{CHIP_H}" rx="10" {box}/>'
    out += text(x + 13, y + (23 if sub else 32), name, 17, name_fill, 700)
    if sub:
        out += text(x + 13, y + 41, sub, 12, sub_fill, 500)
    return out, w


def row(cy, label, body):
    return (f'<path d="M{CX + A + 8} {cy} H{X0 - 12}" stroke="{SLATE}" stroke-width="1.4" stroke-dasharray="2 5" stroke-linecap="round"/>'
            + text(X0, cy - 32, label, 12, MUTED, 600, extra='letter-spacing="2.2"') + body)


def stack():
    centres = [TOP + i * STEP for i in range(len(ROWS) + 1)]
    out = [f'<rect width="{W}" height="{H}" rx="26" fill="{INK}"/>',
           '<defs><radialGradient id="glow"><stop offset="0" stop-color="#F59E0B" stop-opacity=".12"/>'
           '<stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient></defs>',
           f'<ellipse cx="{CX}" cy="{(centres[0] + centres[-1]) / 2}" rx="230" ry="300" fill="url(#glow)"/>']
    for x, dy in ((CX - A, 0), (CX + A, 0), (CX, B)):   # the lines that hold the layers together
        out.append(f'<path d="M{x} {centres[0] + dy} V{centres[-1] + dy}" stroke="{SLATE}" stroke-width="1.2" stroke-dasharray="3 5" opacity=".7"/>')

    out.append(plate(centres[0], ["code"]))
    code_w = len(CODE) * 8.45 + 28
    code = (f'<rect x="{X0}" y="{centres[0] - 20}" width="{code_w:.0f}" height="{CHIP_H}" rx="10" fill="#132B43" stroke="#24405C"/>'
            + text(X0 + 14, centres[0] + 11, CODE, 14, "#E6EDF4", 500, MONO))
    out.append(row(centres[0], "YOUR CODE", code))

    for i, (cy, (label, items)) in enumerate(zip(centres[1:], ROWS)):
        out.append(plate(cy, [status for _, _, status in items], across=i % 2 == 1))
        x, chips = X0, ""
        for name, sub, status in items:
            drawn, w = chip(x, cy - 20, name, sub, status)
            chips += drawn
            x += w + GAP
        out.append(row(cy, label, chips))

    x = X0
    for label, status in (("works today", DONE), ("next", NEXT), ("later", LATER)):
        box = {DONE: f'fill="{SOLAR}"', NEXT: f'fill="#1A2C3A" stroke="{SOLAR}" stroke-width="1.6"',
               LATER: f'fill="none" stroke="{SLATE}" stroke-width="1.6" stroke-dasharray="3 3"'}[status]
        out.append(f'<rect x="{x}" y="{H - 44}" width="14" height="14" rx="4" {box}/>' + text(x + 22, H - 32, label, 13, MUTED, 500))
        x += 22 + len(label) * 7.2 + 24
    return "\n  ".join(out)


def render(name, body, height, scale=2):
    out = DOCS / name
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "picture.svg"
        src.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {height}" width="{W * scale}" height="{height * scale}">\n  {body}\n</svg>\n',
                       encoding="utf-8")
        cmd = [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--default-background-color=00000000",
               f"--window-size={W * scale},{height * scale}", f"--screenshot={out}", src.as_uri()]
        for _ in range(3):   # headless Chrome occasionally exits 2 when launched back-to-back
            out.unlink(missing_ok=True)
            if subprocess.run(cmd, capture_output=True).returncode == 0 and out.exists():
                print(out)
                return
        raise RuntimeError(f"screenshot failed: {out}")


if __name__ == "__main__":
    render("stack.png", stack(), H)
