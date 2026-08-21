#!/usr/bin/env python3
"""Annotate viewer screenshots by overlaying HTML on them and re-capturing.

No PIL or ImageMagick on this machine, and the annotations want real type
anyway. Each figure is an <img> at natural size with absolutely positioned
callouts over it, screenshotted by headless Chrome at the same dimensions.

`crop` trims the right-hand Fields drawer by shrinking the frame and letting
overflow hide the rest -- the image is not rescaled, so pixel coordinates in
the callouts stay valid whether or not a figure is cropped.
"""
import json, subprocess, sys, os

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
HERE = os.path.dirname(os.path.abspath(__file__))

TPL = """<!doctype html><meta charset="utf-8">
<style>
  html,body{{margin:0;padding:0;background:#0d0f18}}
  .cap{{width:{fw}px;box-sizing:border-box;padding:14px 20px;background:#0d0f18;
        border-bottom:2px solid #FFB020;
        font:600 17px/1.4 "Arial Bold",Arial,Helvetica,sans-serif;color:#FFB020}}
  .cap small{{display:block;font-weight:400;font-size:14px;color:#c9ceda;margin-top:3px}}
  .frame{{position:relative;width:{fw}px;height:{fh}px;overflow:hidden}}
  .frame img{{position:absolute;top:0;left:0;width:{iw}px;height:{ih}px;display:block}}
  .box{{position:absolute;border:2.5px solid #FFB020;border-radius:5px;
        box-shadow:0 0 0 2px rgba(13,15,24,.55), 0 0 18px rgba(255,176,32,.30)}}
  .lab{{position:absolute;font:600 15px/1.35 "Arial Bold",Arial,Helvetica,sans-serif;
        color:#0d0f18;background:#FFB020;padding:6px 11px;border-radius:5px;
        white-space:pre;box-shadow:0 3px 12px rgba(0,0,0,.5)}}
  .lab.ghost{{background:rgba(13,15,24,.93);color:#FFB020;
              border:2px solid #FFB020}}
  svg{{position:absolute;top:0;left:0;width:{fw}px;height:{fh}px;overflow:visible;pointer-events:none}}
</style>
{caption}<div class="frame">
  <img src="{img}">
  {boxes}
  <svg>{arrows}</svg>
  {labels}
</div>
"""

def build(spec):
    src = spec["src"]
    fw = spec.get("crop_w", spec["w"])
    fh = spec.get("crop_h", spec["h"])
    boxes = "".join(
        f'<div class="box" style="left:{b[0]}px;top:{b[1]}px;width:{b[2]}px;height:{b[3]}px"></div>'
        for b in spec.get("boxes", []))
    labels = "".join(
        f'<div class="lab{" ghost" if l.get("ghost") else ""}" '
        f'style="left:{l["x"]}px;top:{l["y"]}px">{l["t"]}</div>'
        for l in spec.get("labels", []))
    arrows = "".join(
        f'<line x1="{a[0]}" y1="{a[1]}" x2="{a[2]}" y2="{a[3]}" stroke="#FFB020" '
        f'stroke-width="3" marker-end="url(#h)"/>'
        for a in spec.get("arrows", []))
    if arrows:
        arrows = ('<defs><marker id="h" markerWidth="11" markerHeight="8" refX="10" refY="4" '
                  'orient="auto"><path d="M0,0 L11,4 L0,8 z" fill="#FFB020"/></marker></defs>') + arrows
    cap = ""
    if spec.get("cap"):
        sub = f'<small>{spec["sub"]}</small>' if spec.get("sub") else ""
        cap = f'<div class="cap">{spec["cap"]}{sub}</div>'
    html = TPL.format(img=src, w=spec["w"], h=spec["h"], iw=spec["w"], ih=spec["h"],
                      fw=fw, fh=fh, boxes=boxes, labels=labels, arrows=arrows, caption=cap)
    page = os.path.join(HERE, "_wrap_" + spec["out"] + ".html")
    open(page, "w").write(html)
    out = os.path.join(HERE, spec["out"] + ".png")
    subprocess.run([CHROME, "--headless", "--disable-gpu", "--hide-scrollbars",
                    "--allow-file-access-from-files",
                    f"--window-size={fw},{fh+(78 if spec.get('cap') else 0)}",
                    "--virtual-time-budget=4000",
                    f"--screenshot={out}", "file://" + page],
                   capture_output=True)
    os.remove(page)
    return out, os.path.getsize(out) if os.path.exists(out) else 0

if __name__ == "__main__":
    specs = json.load(open(sys.argv[1]))
    for s in specs:
        out, size = build(s)
        print(f"  {os.path.basename(out):38s} {size:>8,} bytes")
