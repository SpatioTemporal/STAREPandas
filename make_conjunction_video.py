"""Record the conjunction demo video (notebook v10, narration v4) without a camera.

What it does, following docs/conjunction_demo_video_v10_recording_script.md:

1. Voice: every narration segment of narration v4 is synthesized with a macOS
   voice (default "Zoe (Premium)") via ``say`` and measured.
2. Picture: the executed notebook, exported to HTML with the code hidden, is
   opened in headless Chromium (Playwright) at 1920x1080 / 125 % zoom and
   *recorded as video* while the script scrolls to each output, parks on the
   six figures for the five-second pauses, and moves a drawn cursor with a
   highlight onto the exact values the voice names.
3. Sound and picture are joined with ffmpeg (imageio-ffmpeg's binary): each
   segment is placed at the moment it started during the recording (a red
   calibration flash pins the recording's zero), then a 2 s hold and 1 s fade.

    conda run -n starepandas_3.12_v3 python make_conjunction_video.py            # all 9 scenes
    conda run -n starepandas_3.12_v3 python make_conjunction_video.py --scenes 3,4
    conda run -n starepandas_3.12_v3 python make_conjunction_video.py --voice "Ava (Premium)"

Output: conjunction_demo_video_v10_draft.mp4 (repo root). Work files under the
scratch directory given by --work (default .scratch/video_build).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import time

import imageio_ffmpeg
from PIL import Image
from playwright.sync_api import sync_playwright

REPO = os.path.dirname(os.path.abspath(__file__))
NB = os.path.join(REPO, "starepandas", "s3_starepods_examples_video_v10.ipynb")
FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
W, H = 1920, 1080
ZOOM = 1.25
PAUSE = 5.0          # silence on each figure
HOLD = 2.0           # after the last word
FADE = 1.0
SCROLL_MS = 1100     # smooth-scroll duration

# ────────────────────────────── spoken text ──────────────────────────────────
SPEAK = [
    (r"\bSTARE-PODS\b", "STARE pods"),
    (r"\bAMSR2\b", "A M S R two"),
    (r"\bATMS\b", "A T M S"),
    (r"\bGMI\b", "G M I"),
    (r"\bSSMIS\b", "S S M I S"),
    (r"\bNOAA-21\b", "NOAA twenty-one"),
    (r"\bNOAA-20\b", "NOAA twenty"),
    (r"\bSuomi NPP\b", "Suomi N P P"),
    (r"\bPostgreSQL\b", "Postgres"),
    (r"\bS3\b", "S three"),
    (r"\bLevel 1C\b", "Level one C"),
    (r"\bQ-L-four\b", "Q L four"),
    (r"\bdelta-t\b", "delta T"),
    (r"\b2025\b", "twenty twenty-five"),
    (r"\bD\.C\.", "D C"),
]


def speakable(text: str) -> str:
    for pat, rep in SPEAK:
        text = re.sub(pat, rep, text)
    return text


# ───────────────────────────── scene definitions ─────────────────────────────
# Targets:  ("title", t)  scroll so the table/heading text t sits near the top
#           ("cell", title, value[, nth])  a cell inside the table titled `title`
#           ("text", t[, nth])  any element whose text contains t
#           ("img", k)  figure k (0-based)      ("imgpt", k, fx, fy)  a point on it
#           ("top",)  page top
# Steps: dict(scroll=target, pause=sec, say=text, cues=[(phrase, target), ...],
#             park=True)  — cues fire when the voice reaches `phrase`.
TW = "Time-Window Dependence of Conjunctions"
PW = "Pairwise Conjunctions with a 10-min Window"
HO = "STARE-PODS Holdings — 2025Q1 by Instrument"
PO = "Pod Occupancy at QL4 for 2025Q1"
VA = "Conjunctions in Virginia's QL4 Pods, 2025Q1"
SW = "Orbit Swaths over Pod q111332 around the Example Conjunction"
CR = "One Query, Both Criteria — the Whole Store"
SC = "The Conjunction Engine at Scale — the Whole Store, Δt = 10 min"
WM = "Who Met Whom over 2025Q1"

ANCHORS = {1: ("top",), 2: ("title", HO), 3: ("title", TW), 4: ("text", "Catalog-first discovery", 0, -90),
           5: ("title", SW), 6: ("img", 3), 7: ("title", CR), 8: ("title", SC), 9: ("text", "Recap", 0, -60)}

SCENES = [
    # ── 1 ──
    [
        dict(scroll=("top",), say=(
            "Comparing observations from different satellites usually starts by regridding them onto one raster, and that quietly throws away each observation's native geometry. STARE-PODS, the STARE Parallel Optimized Data Store, keeps that geometry. For this demo we took Level 1C swaths from four microwave radiometers, GMI, SSMIS, AMSR2 and ATMS, and partitioned the data using the STARE quad-tree at level four, or Q-L-four. There are two thousand forty-eight Q-L-four cells in total, each about seven hundred kilometers on a side. We call them pods (p-o-d-s), and each pod holds a chunk from every swath that crosses it.")),
        dict(say=(
            "A PostgreSQL catalog records each chunk's pod, scan group and time span. From those alone it can tell us where and when different instruments observed the same pod within a chosen time window, delta-t, before we open any data. We call such a co-observation a conjunction: a meeting of instruments over one pod. Finding these conjunctions in the catalog first is what avoids wasteful data retrieval.")),
        dict(bridge=("title", HO), say="So, first: what's actually in the store?"),
    ],
    # ── 2 ──
    [
        dict(say=(
            "The whole first quarter of 2025: every Level 1C granule of the four instruments, seven thousand three hundred sixty of them, sixteen scan groups, twelve and a half million chunks, in all two thousand forty-eight pods. This represents six satellites, including three copies of ATMS on different satellites. Footprints span from five to seventy-five kilometers, and nothing is regridded."),
            cues=[("seven thousand", ("text", "7,360 granules")),
                  ("six satellites", ("cell", HO, "Satellites")),
                  ("Footprints", ("cell", HO, "Res (km)"))]),
        dict(scroll=("title", PO), say=(
            "Every chunk's database record carries its pod code and its time span, measured from the scan times inside the chunk. Three months (one quarter) of swaths are enough to populate every pod on Earth: tropical pods hold under four thousand chunks, polar pods as many as twenty-six thousand."),
            cues=[("tropical", ("cell", PO, "3,831")), ("polar", ("cell", PO, "26,367"))]),
        dict(bridge=("title", TW), say=(
            "Where, when, which instrument: that's all a conjunction search needs. Let's run one on the whole quarter.")),
    ],
    # ── 3 ──
    [
        dict(say=(
            "From twelve and a half million records the search reads four fields: pod code, scan group, start and end times. From those, pod by pod, it finds every multi-instrument conjunction: swaths from different instruments over the same pod within delta-t of each other. Each arrival that joins one is a single event.")),
        dict(say=(
            "With delta-t at five minutes of observation time: one and a half million events. Every pod sees at least two instruments meet, fourteen hundred see three, nineteen see all four. Widen delta-t to ten minutes and the four-instrument conjunction count climbs to sixty-five; at thirty, three hundred and ninety."),
            cues=[("one and a half", ("cell", TW, "1,499,727")), ("Every pod", ("cell", TW, "2048", 0)),
                  ("fourteen hundred", ("cell", TW, "1414")), ("nineteen", ("cell", TW, "19")),
                  ("sixty-five", ("cell", TW, "65")), ("three hundred and ninety", ("cell", TW, "390"))]),
        dict(scroll=("title", PW), say=(
            "The pair matrix tells us who meets whom. For example, the near-co-orbiting pair of AMSR2 and ATMS meet in all pods. GMI's inclined orbit meets all the others almost everywhere at some point. In contrast, SSMIS's orbital plane keeps it largely apart from AMSR2 and ATMS."),
            cues=[("all pods", ("cell", PW, "2048", 0)), ("GMI's inclined", ("cell", PW, "1976", 1)),
                  ("almost everywhere", ("cell", PW, "1963", 0)), ("SSMIS's orbital", ("cell", PW, "473", 1)),
                  ("apart from", ("cell", PW, "653", 1))]),
        dict(bridge=("text", "Catalog-first discovery", 0, -90), say="That was the planet. Now one state."),
    ],
    # ── 4 ──
    [
        dict(say=(
            "With STARE, a region becomes a set of pods. At Q-L-four, Virginia is covered by four pods, with the biggest spanning about half the state. So the search pulls only those four pods' records: twenty thousand, out of twelve and a half million for the first quarter of 2025, six hundred sixteen times fewer. Ninety-nine point eight percent of the quarter's records are set aside before a single science file is touched. That reduction is the point of this part."),
            cues=[("twenty thousand", ("text", "Catalog-first discovery")),
                  ("Ninety-nine", ("text", "Catalog-first discovery"))]),
        dict(scroll=("img", 0), pause=PAUSE, say=(
            "The cover is bigger than the state, as it must be at this scale; each deeper STARE level halves the pod size.")),
        dict(scroll=("title", VA), say=(
            "In those four pods, no four-instrument conjunction occurred in the entire first quarter. Three-instrument ones did, within five minutes, and we'll follow the first of them in the pod that covers half of Virginia: AMSR2, GMI and ATMS on NOAA-21, on February eighth."),
            cues=[("no four-instrument", ("cell", VA, "0", -1)), ("half of Virginia", ("cell", VA, "50%"))]),
        dict(bridge=("title", SW), say="Here's everyone who passed over that pod that morning."),
    ],
    # ── 5 ──
    [
        dict(say=(
            "All six satellites crossed this pod within seventy-five minutes of the conjunction. ATMS on NOAA-20 came first, fifty-two minutes early. Then, within five minutes of each other, came AMSR2, GMI and ATMS on NOAA-21: the three members, marked with a check. Suomi NPP passed twenty-six minutes late, while SSMIS passed over an hour after the window. The last column says how far each pass missed the window."),
            cues=[("fifty-two", ("cell", SW, "-51.8")), ("three members", ("cell", SW, "✓", 1)),
                  ("twenty-six", ("cell", SW, "25.6")), ("over an hour", ("cell", SW, "72.0")),
                  ("The last column", ("cell", SW, "Missing by"))]),
        dict(scroll=("img", 1), pause=PAUSE, park=True, say=(
            "The six swaths around the globe. Each one is drawn as the set of pods it crossed, which is all the catalog knows about it; the pixels come later. Each instrument keeps one color: ATMS orange, AMSR2 blue, GMI red, SSMIS green."),
            cues=[("ATMS orange", ("imgpt", 1, 0.25, 0.18)), ("AMSR2 blue", ("imgpt", 1, 0.75, 0.18)),
                  ("GMI red", ("imgpt", 1, 0.25, 0.5)), ("SSMIS green", ("imgpt", 1, 0.75, 0.84))]),
        dict(scroll=("img", 2), pause=PAUSE, park=True, say=(
            "Zoomed in on the eastern United States: Virginia's outline, its four cover pods. The pod we picked is in black.")),
        dict(bridge=("img", 3), say="Let's go inside that black pod."),
    ],
    # ── 6 ──
    [
        dict(pause=PAUSE, park=True, say=(
            "Inside the pod, and this is the first time we open the chunks. The left panel maps where each pass crossed the pod; the right panel is a timeline of the overpasses. The three members keep their colors, the others fade to grey, and the shaded band is the five-minute window that ends with ATMS's arrival at seven thirty-six. All six overpasses crossed the pod; their timing determines their qualification for the conjunction."),
            cues=[("left panel", ("imgpt", 3, 0.25, 0.5)), ("right panel", ("imgpt", 3, 0.75, 0.5)),
                  ("shaded band", ("imgpt", 3, 0.78, 0.45))]),
        dict(scroll=("img", 4), pause=PAUSE, park=True, say=(
            "And here's the conjunction itself: AMSR2 at seven thirty-one, GMI at seven thirty-four, ATMS on NOAA-21 at seven thirty-six. An overpass qualifies for the conjunction only if it starts no more than delta-t after an earlier member's overpass ended.")),
        dict(bridge=("title", CR), say="That's who met. Now, what did they see?"),
    ],
    # ── 7 ──
    [
        dict(say=(
            "What did all four instruments see over Virginia that morning? That's two criteria in one query: the four Virginia pods, and the six hours around the conjunction. The catalog intersects them, and only the matching chunks are read from S3.")),
        dict(say=(
            "Time alone, anywhere on Earth: thirty-eight thousand chunks. Space alone, Virginia over the whole first quarter of 2025: twenty thousand. Both together: one hundred twenty-eight chunks from eleven swaths, all six satellites in there."),
            cues=[("thirty-eight", ("cell", CR, "38,574")), ("twenty thousand", ("cell", CR, "20,216")),
                  ("one hundred twenty-eight", ("cell", CR, "128"))]),
        dict(scroll=("img", 5), pause=PAUSE, park=True, say=(
            "Above, the data elements read from the matching chunks, over Virginia and its cover pods. Below, the day's chunks in those pods on a timeline; the hatched ones fall outside the six-hour window, so the query excludes them."),
            cues=[("Above", ("imgpt", 5, 0.5, 0.3)), ("Below", ("imgpt", 5, 0.5, 0.8))]),
        dict(bridge=("title", SC), say="One question left: what happens as the scope grows?"),
    ],
    # ── 8 ──
    [
        dict(say=(
            "Same workflow, four scopes: a day, a week, a month, the first quarter of 2025, ranging from a hundred fifty thousand to twelve and a half million chunks. In this run, reading the quarter's catalog took {READ}, finding every conjunction {FIND}, aggregating everything in {AGG}. One run's timings, not a scaling law; but no chunk was opened and S3 was never accessed. And one pass over the catalog yields the two-, three-, and four-instrument counts at once."),
            cues=[("a day", ("cell", SC, "149,621")), ("a week", ("cell", SC, "1,032,897")),
                  ("a month", ("cell", SC, "4,142,972")), ("the first quarter of 2025,", ("cell", SC, "12,451,867")),
                  ("forty-two", ("cell", SC, "42.5")), ("twelve seconds", ("cell", SC, "12.2")),
                  ("a second and a half", ("cell", SC, "1.5"))]),
        dict(scroll=("title", WM), say="The cross-check: this matrix is, cell for cell, the one from the global search.",
             cues=[("cell for cell", ("cell", WM, "2048", 0))]),
        dict(bridge=("text", "Recap", 0, -60), say="Which brings us back to where we started."),
    ],
    # ── 9 ──
    [
        dict(park=True, say=(
            "Three takeaways. First, STARE harmonizes very different observations through one hierarchical index while keeping their native geometry. Second, the catalog finds the relevant observations before any expensive data access: six hundred times fewer records for Virginia, and just a hundred twenty-eight chunks for a six-hour window over the state. And third, the same hierarchy refines a pod-level discovery all the way down to the data elements.")),
        dict(say="Thank you very much for watching."),
    ],
]

# ────────────────────────────── this run's scene-8 timings ───────────────────
_ONES = "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen".split()
_TENS = "_ _ twenty thirty forty fifty sixty seventy eighty ninety".split()


def _words(n: int) -> str:
    n = int(round(n))
    if n < 20: return _ONES[n]
    return _TENS[n // 10] + ("" if n % 10 == 0 else "-" + _ONES[n % 10])


def quarter_timings(nb_path=NB):
    """(catalog read, find, aggregate) seconds of the quarter row in Part 6, from the executed notebook."""
    import html as _h
    nb = json.load(open(nb_path))
    for c in nb["cells"]:
        for o in c.get("outputs", []):
            x = "".join(o.get("data", {}).get("text/html", []))
            if "Conjunction Engine at Scale" in x:
                x = _h.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<style.*?</style>", "", x, flags=re.S)))
                m = re.search(r"the quarter\s+12,451,867\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", x)
                if m: return tuple(float(v) for v in m.groups())
    raise SystemExit("quarter row not found in the notebook")


def spoken_timings(read, find, agg):
    a = "a second and a half" if abs(agg - 1.5) < 0.06 else f"{_words(int(agg))} point {_words(round(agg * 10) % 10)} seconds"
    return f"{_words(read)} seconds", f"{_words(find)} seconds", a


_READ, _FIND, _AGG = spoken_timings(*quarter_timings())
for _step in SCENES[7]:
    if "say" in _step and "{READ}" in _step["say"]:
        _step["say"] = _step["say"].format(READ=_READ, FIND=_FIND, AGG=_AGG)
        _step["cues"] = [c for c in _step["cues"] if c[0] not in ("forty-two", "twelve seconds", "a second and a half")] + [
            (_READ.split()[0], ("rowcell", SC, "the quarter", 2)), (_FIND, ("rowcell", SC, "the quarter", 3)),
            (_AGG, ("rowcell", SC, "the quarter", 4))]

# ────────────────────────────── browser helpers ──────────────────────────────
JS_SETUP = """
() => {
  const s = document.createElement('style');
  s.textContent = `
    body { zoom: %(zoom)s; padding-bottom: 900px !important; }
    .jp-RenderedImage img, .jp-OutputArea img { max-height: 800px !important; width: auto !important;
                                                display: block; margin: 0 auto; }
    #cur { position: fixed; left: 0; top: 0; width: 34px; height: 46px; z-index: 99999; pointer-events: none;
           transition: transform .7s cubic-bezier(.4,0,.2,1); transform: translate(-200px,-200px);
           filter: drop-shadow(1px 2px 2px rgba(0,0,0,.45)); }
    .hl { box-shadow: 0 0 0 4px rgba(255,140,0,1), 0 0 18px 6px rgba(255,170,0,.55) !important;
          background: rgba(255,230,120,.55) !important; border-radius: 4px; transition: box-shadow .3s, background .3s; }
    #flash { position: fixed; inset: 0; background: #ff0000; z-index: 100000; }
  `;
  document.head.appendChild(s);
  const c = document.createElement('div'); c.id = 'cur';
  c.innerHTML = `<svg viewBox="0 0 34 46" width="34" height="46"><path d="M2 2 L2 36 L11 27 L17 42 L23 39 L17 25 L30 25 Z"
                 fill="#111" stroke="#fff" stroke-width="2.5" stroke-linejoin="round"/></svg>`;
  document.documentElement.appendChild(c);
  // hide the bookkeeping the plan never shows: timer lines, "Processing Time" tables, the Run time section
  document.querySelectorAll('pre').forEach(p => { if (p.textContent.trim().startsWith('⏱')) p.style.display = 'none'; });
  document.querySelectorAll('table').forEach(tb => { if (/Processing Time of Each/.test(tb.textContent)) {
    const o = tb.closest('.jp-OutputArea-child') || tb.parentElement; o.style.display = 'none'; } });
  document.querySelectorAll('h2').forEach(h => { if (h.textContent.trim().startsWith('Run time')) {
    const c = h.closest('.jp-Cell'); if (c) { c.style.display = 'none';
    if (c.nextElementSibling) c.nextElementSibling.style.display = 'none'; } } });
  window.__scrollTo = (y, ms) => new Promise(res => {
    const y0 = window.scrollY, dy = y - y0, t0 = performance.now();
    const ease = t => t < .5 ? 4*t*t*t : 1 - Math.pow(-2*t + 2, 3) / 2;
    (function step(now) {
      const t = Math.min(1, (now - t0) / ms);
      window.scrollTo(0, y0 + dy * ease(t));
      if (t < 1) requestAnimationFrame(step); else res();
    })(t0);
  });
  window.__cursor = (x, y) => { document.getElementById('cur').style.transform = `translate(${x}px,${y}px)`; };
  window.__park = () => { document.getElementById('cur').style.transform = 'translate(-200px,-200px)'; };
  window.__flash = on => { let f = document.getElementById('flash');
    if (on && !f) { f = document.createElement('div'); f.id = 'flash'; document.documentElement.appendChild(f); }
    if (!on && f) f.remove(); };
}
"""


class Stage:
    def __init__(self, page):
        self.page = page
        self.hl = None

    # ---- locate -------------------------------------------------------------
    def locate(self, target):
        kind = target[0]
        pg = self.page
        if kind == "top":
            return None
        if kind == "img":
            return pg.locator(".jp-RenderedImage img, .jp-OutputArea img").nth(target[1])
        if kind == "imgpt":
            return pg.locator(".jp-RenderedImage img, .jp-OutputArea img").nth(target[1])
        if kind == "title":
            return pg.get_by_text(target[1], exact=False).first
        if kind == "text":
            nth = target[2] if len(target) > 2 else 0
            return pg.get_by_text(target[1], exact=False).nth(nth)
        if kind == "rowcell":                      # ("rowcell", title, row_text, nth_td)
            title, row_text, nth = target[1], target[2], target[3]
            table = pg.get_by_text(title, exact=False).first.locator("xpath=ancestor::table[1]")
            row = table.locator("tr", has=pg.get_by_text(row_text, exact=False)).first
            return row.locator("td").nth(nth)
        if kind == "cell":
            title, value = target[1], target[2]
            nth = target[3] if len(target) > 3 else 0
            table = pg.get_by_text(title, exact=False).first.locator("xpath=ancestor::table[1]")
            cells = table.get_by_text(value, exact=True)
            if cells.count() == 0:  # two-line headers etc.
                cells = table.get_by_text(value, exact=False)
            return cells.nth(nth if nth >= 0 else cells.count() + nth)
        raise ValueError(target)

    def box(self, target):
        loc = self.locate(target)
        if loc is None:
            return None
        loc.first.wait_for(state="attached", timeout=5000)
        return loc.bounding_box()

    # ---- act ----------------------------------------------------------------
    def scroll(self, target, ms=SCROLL_MS):
        if target[0] == "top":
            y = 0
        else:
            b = self.box(target)
            margin = target[3] if target[0] == "text" and len(target) > 3 else (
                -24 if target[0] in ("img", "imgpt") else -70)
            y = max(0, self.page.evaluate("window.scrollY") + b["y"] + margin)
        self.page.evaluate("([y, ms]) => window.__scrollTo(y, ms)", [y, ms])

    def point(self, target):
        b = self.box(target)
        if b is None:
            return
        if target[0] == "imgpt":
            x, y = b["x"] + b["width"] * target[2], b["y"] + b["height"] * target[3]
        else:
            x, y = b["x"] + b["width"] * 0.5, b["y"] + b["height"] * 0.5
        # keep the pointer on screen; scroll a little if the cell is below the fold
        if y > H - 60 or y < 10:
            self.page.evaluate("([d, ms]) => window.__scrollTo(window.scrollY + d, ms)", [y - H * 0.6, 500])
            time.sleep(0.55)
            b = self.box(target)
            y = b["y"] + b["height"] * 0.5
        self.page.evaluate("([x, y]) => window.__cursor(x, y)", [x + 6, y + 6])
        if target[0] not in ("imgpt", "img"):
            self.unhighlight()
            self.locate(target).evaluate("e => e.classList.add('hl')")
            self.hl = target

    def unhighlight(self):
        if self.hl is not None:
            try:
                self.locate(self.hl).evaluate("e => e.classList.remove('hl')")
            except Exception:
                pass
            self.hl = None

    def park(self):
        self.unhighlight()
        self.page.evaluate("window.__park()")


# ────────────────────────────── audio helpers ────────────────────────────────
def synth(text, path, voice, rate):
    subprocess.run(["say", "-v", voice, "-r", str(rate), "-o", path, speakable(text)], check=True)
    out = subprocess.run(["afinfo", path], capture_output=True, text=True).stdout
    return float(re.search(r"estimated duration: ([\d.]+)", out).group(1))


def cue_fraction(text, phrase):
    words = text.split()
    pw = phrase.split()
    low = [re.sub(r"[^\w'-]", "", w).lower() for w in words]
    plow = [re.sub(r"[^\w'-]", "", w).lower() for w in pw]
    for i in range(len(low) - len(plow) + 1):
        if low[i:i + len(plow)] == plow:
            return i / len(words)
    raise ValueError(f"cue {phrase!r} not in segment")


def run(args):
    work = os.path.abspath(args.work)
    os.makedirs(work, exist_ok=True)
    html_name = os.path.splitext(os.path.basename(NB))[0] + ".html"
    html = os.path.join(work, html_name)
    if not os.path.exists(html) or os.path.getmtime(html) < os.path.getmtime(NB):
        subprocess.run(["jupyter", "nbconvert", "--to", "html", "--no-input", "--output-dir", work,
                        "--output", html_name, NB], check=True)
    scenes = [int(s) for s in args.scenes.split(",")] if args.scenes else list(range(1, 10))
    for si in scenes:                       # fail fast: every cue phrase must occur in its segment
        for step in SCENES[si - 1]:
            for ph, _ in step.get("cues", []):
                cue_fraction(step["say"], ph)

    # 1. voice
    segs = []  # (scene, step_index, path, duration)
    for si in scenes:
        for k, step in enumerate(SCENES[si - 1]):
            if "say" in step:
                p = os.path.join(work, f"s{si}_{k}.aiff")
                d = synth(step["say"], p, args.voice, args.rate)
                segs.append((si, k, p, d))
    voice_total = sum(d for *_, d in segs)
    print(f"voice: {len(segs)} segments, {voice_total:.1f} s")

    # 2. record
    vdir = os.path.join(work, "capture")
    shutil.rmtree(vdir, ignore_errors=True)
    events = []  # (t_mono, path)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": W, "height": H}, record_video_dir=vdir,
                                  record_video_size={"width": W, "height": H}, device_scale_factor=1)
        page = ctx.new_page()
        page.goto("file://" + html)
        page.wait_for_load_state("load")
        page.evaluate(JS_SETUP % {"zoom": ZOOM})
        page.wait_for_timeout(400)
        page.evaluate("window.__flash(true)")
        page.wait_for_timeout(600)
        page.evaluate("window.__flash(false)")
        t_flash_off = time.monotonic()
        page.wait_for_timeout(300)
        st = Stage(page)
        seg_iter = iter(segs)
        prev = None
        for si in scenes:
            print(f"scene {si}")
            if prev != si - 1:                 # partial run: start where the scene starts
                st.scroll(ANCHORS[si], ms=400)
                time.sleep(0.6)
            prev = si
            for k, step in enumerate(SCENES[si - 1]):
                if step.get("park"):
                    st.park()
                settled = 0.0
                if "scroll" in step:
                    st.park()
                    st.scroll(step["scroll"])
                    settled = SCROLL_MS / 1000 + 0.15
                    time.sleep(settled)
                if step.get("pause"):
                    st.park()
                    time.sleep(max(0.0, step["pause"] - settled + 0.3))
                if "say" in step:
                    _, _, path, dur = next(seg_iter)
                    t0 = time.monotonic()
                    events.append((t0, path, dur))
                    if "bridge" in step:          # scroll while the bridge is spoken
                        time.sleep(min(0.8, dur * 0.3))
                        st.park()
                        st.scroll(step["bridge"], ms=int(min(1800, max(900, (dur - 0.8) * 1000))))
                    cues = sorted(((cue_fraction(step["say"], ph), tg) for ph, tg in step.get("cues", [])))
                    for frac, tg in cues:
                        wait = t0 + frac * dur - time.monotonic()
                        if wait > 0:
                            time.sleep(wait)
                        st.point(tg)
                    rem = t0 + dur - time.monotonic()
                    if rem > 0:
                        time.sleep(rem)
                    if step.get("cues"):
                        time.sleep(0.2)
        st.park()
        time.sleep(HOLD + FADE + 0.5)
        t_end = time.monotonic()
        ctx.close()
        browser.close()
    webm = glob.glob(os.path.join(vdir, "*.webm"))[0]

    # 3. find the flash in the video → recording zero
    fdir = os.path.join(work, "frames")
    shutil.rmtree(fdir, ignore_errors=True)
    os.makedirs(fdir)
    subprocess.run([FFMPEG, "-loglevel", "error", "-y", "-t", "4", "-i", webm, "-vf", "fps=50,scale=64:36",
                    os.path.join(fdir, "f%04d.png")], check=True)
    last_red = None
    for f in sorted(glob.glob(os.path.join(fdir, "*.png"))):
        r, g, b = Image.open(f).convert("RGB").getpixel((32, 18))
        if r > 180 and g < 80 and b < 80:
            last_red = int(os.path.basename(f)[1:5])
    if last_red is None:
        raise SystemExit("calibration flash not found in the capture")
    v_flash_off = last_red / 50.0  # first frame after the flash
    offset = t_flash_off - v_flash_off
    print(f"flash off at video {v_flash_off:.2f}s")

    # 4. audio track at the recorded moments, then mux + fade
    inputs, filt = [], []
    for i, (t0, path, dur) in enumerate(events):
        inputs += ["-i", path]
        ms = max(0, int((t0 - offset) * 1000))
        filt.append(f"[{i + 1}:a]adelay={ms}|{ms}[a{i}]")
    n = len(events)
    filt.append("".join(f"[a{i}]" for i in range(n)) + f"amix=inputs={n}:normalize=0:dropout_transition=0[mix]")
    video_len = t_end - offset
    fade_start = max(0.0, (events[-1][0] - offset) + events[-1][2] + HOLD)
    out = os.path.join(REPO, args.out)
    cmd = [FFMPEG, "-loglevel", "error", "-y", "-i", webm, *inputs,
           "-filter_complex", ";".join(filt),
           "-map", "0:v", "-map", "[mix]",
           "-vf", f"fade=t=out:st={fade_start:.2f}:d={FADE},format=yuv420p",
           "-t", f"{fade_start + FADE:.2f}",
           "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-r", "25",
           "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2", out]
    subprocess.run(cmd, check=True)
    json.dump({"offset": offset, "events": [(t - offset, os.path.basename(p), d) for t, p, d in events],
               "video_len": video_len, "voice_total": voice_total, "voice": args.voice, "rate": args.rate},
              open(os.path.join(work, "timeline.json"), "w"), indent=1)
    print(f"wrote {out}  ({fade_start + FADE:.1f} s; voice {voice_total:.1f} s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenes", default="", help="comma list, e.g. 3,4 (default all)")
    ap.add_argument("--voice", default="Zoe (Premium)")
    ap.add_argument("--rate", type=int, default=150, help="say -r; 150 ≈ the script's 155 wpm for Zoe")
    ap.add_argument("--work", default=os.path.join(REPO, ".scratch", "video_build"))
    ap.add_argument("--out", default="conjunction_demo_video_v10_draft.mp4")
    run(ap.parse_args())
