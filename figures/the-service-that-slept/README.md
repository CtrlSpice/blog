# Figure sources — The Service That Slept

Raw captures and the annotation tooling for the post.
Nothing here is published: Hugo only reads `content/`, `static/`, `assets/`, `layouts/`, `data/`, `i18n/` and `archetypes/`, so this directory is version-controlled source material only.
The finished figures that the post actually references live in `static/images/the-service-that-slept/`.

## What the raw captures are

| file | what it shows |
| --- | --- |
| `reveal-quote.png` | The 3 h 22 m trace with a `quote` span selected, so the stranded spans are scrolled into view. Becomes `fig1-stranded-quote.png`. |
| `skew-0-normal-1946.png` | A clean trace from 19:46, before the first suspend. |
| `skew-50min-2120.png` | The same journey at 21:20, ~50 minutes of skew. |
| `skew-130min-0411.png` | 04:11, 2 h 10 m of skew. |
| `skew-202min-0849.png` | 08:49, 3 h 22 m of skew. |
| `power-correlation.svg` | The `pmset` chart. Edit the SVG directly, or regenerate it. |

All four `skew-*` captures are the same 159-span `user_checkout_multi` journey.
Only the time axis differs, which is the entire point of the sequence.

## How they were captured

`otel-desktop-viewer` running against the overnight store, screenshotted headlessly so the output is a real PNG at a known size rather than a window grab:

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --hide-scrollbars \
  --window-size=1600,1000 --virtual-time-budget=9000 \
  --screenshot=out.png \
  "http://localhost:8000/traces/<traceID>"
```

Append `?span=<spanID>` to have the viewer reveal and select a specific span.
That is how `reveal-quote.png` gets the `quote` spans on screen — they sit deep enough in the tree to be below the fold otherwise.

## Re-annotating

There is no PIL or ImageMagick on this machine, so `annotate.py` overlays HTML on the PNG and re-screenshots it.
That also means the annotations are real type rather than bitmap scribbles, and the coordinates are plain CSS pixels against the original image.

```bash
python3 annotate.py specs-fig1.json
python3 annotate.py specs-fig2.json
```

Output lands next to the script; copy the finished files into `static/images/the-service-that-slept/`.

Each entry in a spec file takes:

- `src`, `out` — input PNG and output basename
- `w`, `h` — the source image's true dimensions
- `crop_w`, `crop_h` — optional; shrinks the frame to trim the right-hand Fields drawer. The image is never rescaled, so annotation coordinates stay valid whether or not a figure is cropped.
- `boxes` — `[x, y, w, h]` highlight rectangles
- `arrows` — `[x1, y1, x2, y2]`, arrowhead at the second point
- `labels` — `{x, y, t}`, with `"ghost": true` for dark-on-amber instead of amber-on-dark
- `cap`, `sub` — caption band rendered *above* the frame

Captions go above rather than on top of the screenshot on purpose: the first version placed them over the waterfall and covered the root span row.

## Regenerating the correlation chart

`power-correlation.svg` was generated from two sources that were never fitted to each other — per-trace skew measured from span timestamps, and sleep durations read from `pmset -g log`.

One trap worth recording: take each `Sleep` line's own trailing duration field.
Pairing `Sleep` events with the next `Wake` gives 31 seconds total instead of 12,153, because a `Sleep` is routinely followed by a `DarkWake` a second or two later and the machine then goes straight back to sleep.

`wrap-power.html` is the caption wrapper; screenshot it at 1238×638 to rebuild the PNG.
