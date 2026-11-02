# Recording script — "STARE-PODS: Conjunction Analytics" (notebook v10, narration v4, ≈ 7:59)

Companion notebook: `starepandas/s3_starepods_examples_video_v10.ipynb`
(built 2026-10-31 from v9 with the reviewers' wording, executed the same day —
record by scrolling through it, nothing re-executes on scroll). Plan of
record: `conjunction_demo_video_v10_plan.html` (repo root). Spoken text:
narration v3 (`conjunction_demo_video_v10_narration_v3.docx`, 2026-10-31,
after Mike Bauer's and Kwo-Sen Kuo's review of 2026-10-30), reproduced here
verbatim so the recording needs this one file open. A scripted draft of the
whole film exists: `make_conjunction_video.py` →
`conjunction_demo_video_v10_draft.mp4`.

Nine scenes, about 1,154 spoken words at 155 words per minute (7:27 of
voice), six five-second pauses on the figures and a two-second hold at the
end: **≈ 7:59**. Decisions taken 2026-10-24: conversational register as
written; nothing runs live on camera; the film closes on the Recap cell.

## 1. Pre-flight (the day of recording)

1. **Run the notebook end to end** (`Kernel → Restart & Run All`, or
   `jupyter nbconvert --to notebook --execute --inplace` in
   `starepandas_3.12_v3`). 5–7 min; the holdings cell (≈ 110 s), Part 2
   (≈ 85 s) and Part 6 (≈ 80 s) dominate. It runs from the laptop against
   RDS and S3 — the StarePodsInfraStack's idle network posture (no NAT) does
   not affect it.
2. **Check the run**: 19 cells, 6 figures, no error output. The last cell's
   table should read about 350 s total.
3. **Read the scene 8 numbers off this run** (Part 6, "The Conjunction Engine
   at Scale", the quarter row's three Seconds columns) and say those instead
   of the ones printed in §3 if they differ by more than a second or two.
   They are the only timings the voice says (narration v4 dropped scene 3's
   "forty seconds … twelve" line when Part 2's step table left the notebook).
4. **Display**: 1080p or higher; notebook zoom 125–150 %; collapse or hide
   every code input (JupyterLab: `View → Collapse All Code`, or the
   `hide-input` toolbar); turn off cell execution counts if the theme shows
   them; light theme; browser chrome and dock hidden; notifications off.
5. **Scroll to the top**; the title cell fills the frame. Cursor parked in
   the left margin. Start the screen capture, then the voice take.
6. **Voice**: record separately from the screen if possible, then cut to the
   capture with the pauses. Read at about 155 words per minute; a 161-word
   scene 1 should land near 1:02.

Pronunciation: Δt → "delta-t"; QL4 → "Q-L-four"; pod codes are never read
digit by digit ("the big pod", "the selected pod", "that black pod");
satellites as written ("NOAA-21", "Suomi NPP", "GCOM-W1" is never said).
Vocabulary: conjunction (introduced in scene 1; never "meeting" as a noun
after that definition), overpass, pod code; count instruments ("three
instruments"), never "three-way". No algorithm names.

## 2. Timeline

| Scene | Starts | Voice | Pauses | Ends | Cell on screen |
|---|---|---|---|---|---|
| 1 Cold open | 0:00 | 1:05 | — | 1:05 | title cell (In [–], first markdown) |
| 2 The store and its catalog | 1:05 | 0:48 | — | 1:53 | holdings table (In [2]) → pod occupancy (In [3]) |
| 3 Who met whom | 1:53 | 1:00 | — | 2:53 | Part 2 (In [4]): Δt table, pair matrix |
| 4 Virginia | 2:53 | 1:00 | 1 × 5 s | 3:58 | Part 3 (In [5]): callout, figure 1, per-pod table |
| 5 Six passes | 3:58 | 0:52 | 2 × 5 s | 5:00 | Part 3 (In [5]): swath table, figures 2–3 |
| 6 Up close | 5:00 | 0:46 | 2 × 5 s | 5:57 | Part 4 (In [6]): figures 4–5 |
| 7 One query | 5:57 | 0:47 | 1 × 5 s | 6:48 | Part 5 (In [7]): criteria table, figure 6 |
| 8 Scale of a quarter | 6:48 | 0:41 | — | 7:29 | Part 6 (In [8]): scope table, matrix |
| 9 Takeaways | 7:29 | 0:28 | hold 2 s | 7:59 | Recap cell |

Each scene's bridge sentence is the last line of its voice and is spoken
*while scrolling* to the next scene's first cell, so the screen is never
silent and moving at once. Timestamps are targets, not marks to hit: if a
scene runs long, the next ones shift; §5 says what to cut.

**Skipped on screen (scroll past during a bridge):** Part 2's q033303
drill-down; Part 4's two arrival
tables; Part 5's "Region-of-Interest Query" and by-instrument tables;
Part 6's one-pass table and "In this run …" line; the Run time cell.

## 3. Shot list and narration

Format per scene: **Shots** (what is in frame, what the cursor touches, in
order) · **Say** (narration v2 verbatim; `…pause…` = five seconds of silence,
the next sentence is spoken over the figure) · **Bridge** (spoken while
scrolling to the next scene).

### Scene 1 — Cold open: the problem and the idea · 0:00–1:05 · 169 words

**Shots**
1. Title cell "STARE-PODS — Conjunction Analytics": heading and the first
   paragraphs fill the frame. Hold through the first two paragraphs of voice.
2. On "A PostgreSQL catalog records …", begin a slow scroll so the numbered
   part list (Parts 1–6) is in frame on the last sentence.
3. Cursor stays in the margin throughout.

**Say**

> Comparing observations from different satellites usually starts by regridding them onto one raster, and that quietly throws away each observation's native geometry.
>
> STARE-PODS, the STARE Parallel Optimized Data Store, keeps that geometry. For this demo we took Level 1C swaths from four microwave radiometers, GMI, SSMIS, AMSR2 and ATMS, and partitioned the data using the STARE quad-tree at level four, or Q-L-four. There are two thousand forty-eight Q-L-four cells in total, each about seven hundred kilometers on a side. We call them pods (p-o-d-s), and each pod holds a chunk from every swath that crosses it.
>
> A PostgreSQL catalog records each chunk's pod, scan group and time span. From those alone it can tell us where and when different instruments observed the same pod within a chosen time window, delta-t, before we open any data. We call such a co-observation a conjunction: a meeting of instruments over one pod. Finding these conjunctions in the catalog first is what avoids wasteful data retrieval.

**Bridge** (scrolling to the holdings table) → *So, first: what's actually in the store?*

### Scene 2 — The store and its catalog · 1:05–1:53 · 124 words

**Shots**
1. "STARE-PODS Holdings — 2025Q1 by Instrument" fills the frame. Cursor
   rests under the subtitle "7,360 granules → 16 scan groups → 12,451,867
   chunks in 2048 QL4 pods" on "seven thousand three hundred sixty …".
2. Cursor to the **Satellites** column on "six satellites" (ATMS row lists
   Suomi NPP / NOAA-20 / NOAA-21).
3. Cursor to **Res (km)** on "five to seventy-five" (AMSR2 5–42 … ATMS 16–75).
4. On "Every chunk's record …", scroll past Part 1's prose and the
   "Temporal Catalog per Scan Group" table to **"Pod Occupancy at QL4 for
   2025Q1"**; cursor down its last three rows on "tropical … polar"
   (Fewest 3,831 q130203 tropical · Median 4,730 · Most 26,367 q033313 polar).

**Say**

> The whole first quarter of 2025: every Level 1C granule of the four instruments, seven thousand three hundred sixty of them, sixteen scan groups, twelve and a half million chunks, in all two thousand forty-eight pods. This represents six satellites, including three copies of ATMS on different satellites. Footprints span from five to seventy-five kilometers, and nothing is regridded.
>
> Every chunk's database record carries its pod code and its time span, measured from the scan times inside the chunk. Three months (one quarter) of swaths are enough to populate every pod on Earth: tropical pods hold under four thousand chunks, polar pods as many as twenty-six thousand.

**Bridge** (scrolling to Part 2's Δt table) → *Where, when, which instrument: that's all a conjunction search needs. Let's run one on the whole quarter.*

### Scene 3 — Who met whom, from records alone · 1:53–2:53 · 154 words

**Shots**
1. "Time-Window Dependence of Conjunctions" fills the frame; cursor in the
   margin for the first paragraph.
2. On "one and a half million events", cursor on **Events 1,499,727** (Δt = 5
   row); then along that row: 2048 · 1414 · 19 on "every pod … fourteen
   hundred … nineteen".
3. On "climbs to sixty-five; at thirty, three hundred ninety", cursor **down
   the "4 instruments" column**: 19 → 65 → 390.
4. Scroll to "Pairwise Conjunctions with a 10-min Window". Cursor on
   **AMSR2–ATMS 2048** on "meet in all pods"; along GMI's row (1976 · 1980 ·
   1963) on "almost everywhere"; along **SSMIS's row** (473 · 653 · 1963) on
   "largely apart from AMSR2 and ATMS".
5. The SSMIS sentence ends over the matrix; cursor back to the margin. Do
   **not** scroll to the q033303 drill-down.

**Say**

> From twelve and a half million records the search reads four fields: pod code, scan group, start and end times. From those, pod by pod, it finds every multi-instrument conjunction: swaths from different instruments over the same pod within delta-t of each other. Each arrival that joins one is a single event.
>
> With delta-t at five minutes of observation time: one and a half million events. Every pod sees at least two instruments meet, fourteen hundred see three, nineteen see all four. Widen delta-t to ten minutes and the four-instrument conjunction count climbs to sixty-five; at thirty, three hundred and ninety.
>
> The pair matrix tells us who meets whom. For example, the near-co-orbiting pair of AMSR2 and ATMS meet in all pods. GMI's inclined orbit meets all the others almost everywhere at some point. In contrast, SSMIS's orbital plane keeps it largely apart from AMSR2 and ATMS.

**Bridge** (scrolling past the drill-down to Part 3) → *That was the planet. Now one state.*

### Scene 4 — Virginia: twelve and a half million records to twenty thousand · 2:53–3:58 · 154 words + pause

**Shots**
1. Part 3's headline callout fills the top of the frame: "12,451,867 chunk
   records → 20,216 for Virginia's 4 QL4 pods — a 616× reduction, 99.84 %
   excluded". Cursor under **20,216**, then **616×**, then **99.84 %** as
   each is said.
2. Scroll so **figure 1** (Virginia, its four-pod cover, Washington D.C.
   marked) fills the frame. **…pause 5 s…** The cover sentence is spoken
   over it.
3. Scroll to "Conjunctions in Virginia's QL4 Pods, 2025Q1". Cursor **down
   the all-zero "4 instruments" column** on "all four instruments never
   met"; then on the **bold q111332 rows** (Share 50 %; Δt = 5: 440 · 6 · 0)
   on "Three did … in the big pod".

**Say**

> With STARE, a region becomes a set of pods. At Q-L-four, Virginia is covered by four pods, with the biggest spanning about half the state. So the search pulls only those four pods' records: twenty thousand, out of twelve and a half million for the first quarter of 2025, six hundred sixteen times fewer. Ninety-nine point eight percent of the quarter's records are set aside before a single science file is touched. That reduction is the point of this part.
>
> …pause on figure 1… The cover is bigger than the state, as it must be at this scale; each deeper STARE level halves the pod size.
>
> In those four pods, no four-instrument conjunction occurred in the entire first quarter. Three-instrument ones did, within five minutes, and we'll follow the first of them in the pod that covers half of Virginia: AMSR2, GMI and ATMS on NOAA-21, on February eighth.

**Bridge** (scrolling to the swath table) → *Here's everyone who passed over that pod that morning.*

### Scene 5 — Six passes, three in the window · 3:58–5:00 · 135 words + 2 pauses

**Shots**
1. "Orbit Swaths over Pod q111332 around the Example Conjunction" fills the
   frame. Cursor **down the "Missing by (min)" column** as the passes are
   named: −51.8 (ATMS NOAA-20) · ✓ ✓ ✓ (AMSR2, GMI, ATMS NOAA-21) · 25.6
   (ATMS Suomi NPP) · 72.0 (SSMIS F18). On "The last column says …" cursor
   rests on the column header.
2. Scroll so **figure 2** (six panels, one swath each, whole globe) fills
   the frame. **…pause 5 s…** Colour sentence over it; cursor may touch one
   panel per color named (ATMS orange, AMSR2 blue, GMI red, SSMIS green).
3. Scroll so **figure 3** (eastern United States; Virginia outline, four
   cover pods, selected pod in black) fills the frame. **…pause 5 s…**
   Cursor circles the black pod once on "in black".

**Say**

> All six satellites crossed this pod within seventy-five minutes of the conjunction. ATMS on NOAA-20 came first, fifty-two minutes early. Then, within five minutes of each other, came AMSR2, GMI and ATMS on NOAA-21: the three members, marked with a check. Suomi NPP passed twenty-six minutes late, while SSMIS passed over an hour after the window. The last column says how far each pass missed the window.
>
> …pause on figure 2… The six swaths around the globe. Each one is drawn as the set of pods it crossed, which is all the catalog knows about it; the pixels come later. Each instrument keeps one color: ATMS orange, AMSR2 blue, GMI red, SSMIS green.
>
> …pause on figure 3… Zoomed in on the eastern United States: Virginia's outline, its four cover pods. The pod we picked is in black.

**Bridge** (scrolling into Part 4, past its six-swath table) → *Let's go inside that black pod.*

### Scene 6 — Up close: three satellites in five minutes · 5:00–5:57 · 120 words + 2 pauses

**Shots**
1. Scroll past "Pod q111332 — All 6 Orbit Swaths in the Pod" (not read) so
   **figure 4** fills the frame: left panel the pod with all six passes
   (members in color, three others grey), right panel the timeline with
   the shaded Δt band. **…pause 5 s…** On "left panel … right panel" the
   cursor moves left, then right; on "shaded band" it rests on the band.
2. Scroll past "Pod q111332 — a 3-Instrument Conjunction" (not read) so
   **figure 5** fills the frame. **…pause 5 s…** On the three times the
   cursor touches the three pass bars in order (07:31 AMSR2 · 07:34 GMI ·
   07:36 ATMS NOAA-21).

**Say**

> …pause on figure 4… Inside the pod, and this is the first time we open the chunks. The left panel maps where each pass crossed the pod; the right panel is a timeline of the overpasses. The three members keep their colors, the others fade to grey, and the shaded band is the five-minute window that ends with ATMS's arrival at seven thirty-six. All six overpasses crossed the pod; their timing determines their qualification for the conjunction.
>
> …pause on figure 5… And here's the conjunction itself: AMSR2 at seven thirty-one, GMI at seven thirty-four, ATMS on NOAA-21 at seven thirty-six. An overpass qualifies for the conjunction only if it starts no more than delta-t after an earlier member's overpass ended.

**Bridge** (scrolling into Part 5, past "The Region-of-Interest Query") → *That's who met. Now, what did they see?*

### Scene 7 — One query, space and time together · 5:57–6:48 · 121 words + pause

**Shots**
1. "One Query, Both Criteria — the Whole Store" fills the frame (subtitle
   "128 chunks of 11 orbit swaths match both criteria — in 4 of the 4 cover
   pods"). Cursor in the margin for the first paragraph.
2. Cursor **steps down the Chunks column** as each is said: 38,574 (time
   alone) → 20,216 (space alone, quarter) → 252 (space alone, the day; not
   said, pass over it) → **128** (both).
3. Scroll past "Chunks Matching Both Criteria, by Instrument and Satellite"
   (not read) so **figure 6** fills the frame: data elements over Virginia
   above, the day's chunk timeline with hatched drops below. **…pause 5 s…**
   Cursor to the upper panel on "Above", to the hatched bars on "hatched".

**Say**

> What did all four instruments see over Virginia that morning? That's two criteria in one query: the four Virginia pods, and the six hours around the conjunction. The catalog intersects them, and only the matching chunks are read from S3.
>
> Time alone, anywhere on Earth: thirty-eight thousand chunks. Space alone, Virginia over the whole first quarter of 2025: twenty thousand. Both together: one hundred twenty-eight chunks from eleven swaths, all six satellites in there.
>
> …pause on figure 6… Above, the data elements read from the matching chunks, over Virginia and its cover pods. Below, the day's chunks in those pods on a timeline; the hatched ones fall outside the six-hour window, so the query excludes them.

**Bridge** (scrolling to Part 6) → *One question left: what happens as the scope grows?*

### Scene 8 — The engine at the scale of a quarter · 6:48–7:29 · 105 words

**Shots**
1. "The Conjunction Engine at Scale — the Whole Store, Δt = 10 min" fills
   the frame. Cursor **down the Chunks column** on "a day, a week, a month,
   the quarter" (149,621 → 1,032,897 → 4,142,972 → 12,451,867).
2. Cursor **across the quarter row's three Seconds columns** (Catalog read ·
   Find matches · Aggregate) as the three timings are said.
3. Scroll past the "In this run …" line and the one-pass table (not read)
   to **"Who Met Whom over 2025Q1"**; cursor on **AMSR2–ATMS 2048** on
   "cell for cell".

**Say** — the three timings are **this run's**; the values below are the
2026-10-31 v10 run's (see §4) and are re-read off the screen before voicing.
The scripted draft reads them from the executed notebook automatically.

> Same workflow, four scopes: a day, a week, a month, the first quarter of 2025, ranging from a hundred fifty thousand to twelve and a half million chunks. In this run, reading the quarter's catalog took **forty-two** seconds, finding every conjunction **thirteen** seconds, aggregating everything in **one point four seconds**. One run's timings, not a scaling law; but no chunk was opened and S3 was never accessed. And one pass over the catalog yields the two-, three-, and four-instrument counts at once.
>
> The cross-check: this matrix is, cell for cell, the one from the global search.

**Bridge** (scrolling past the Run time cell to the Recap) → *Which brings us back to where we started.*

### Scene 9 — Three takeaways · 7:29–7:59 · 72 words + hold

**Shots**
1. The **Recap** cell, scrolled so the numbered list of three fills the
   frame. Cursor in the margin; optionally rests on each numbered item as
   it is said.
2. After "Thank you very much for watching", **hold two seconds**, then fade
   to black. The Run time cell is never shown.

**Say**

> Three takeaways. First, STARE harmonizes very different observations through one hierarchical index while keeping their native geometry. Second, the catalog finds the relevant observations before any expensive data access: six hundred times fewer records for Virginia, and just a hundred twenty-eight chunks for a six-hour window over the state. And third, the same hierarchy refines a pod-level discovery all the way down to the data elements.
>
> Thank you very much for watching.

## 4. Numbers the voice says, and where they are on screen

Checked 2026-10-24 against the executed v9 notebook; v10's tables are identical. Re-check the last two
rows after every re-run; the rest only change if the store is re-ingested.

| Said | Value on screen | Where |
|---|---|---|
| seven thousand three hundred sixty granules · sixteen scan groups · twelve and a half million chunks · two thousand forty-eight pods | 7,360 · 16 · 12,451,867 · 2048 | Holdings subtitle |
| six satellites · footprints five to seventy-five | GPM CO, DMSP F18, GCOM-W1, Suomi NPP / NOAA-20 / NOAA-21 · 5–75 km | Holdings: Satellites, Res (km) |
| under four thousand … twenty-six thousand | 3,831 (q130203, tropical) · 26,367 (q033313, polar) | Pod Occupancy |
| one and a half million events · every pod · fourteen hundred · nineteen | 1,499,727 · 2048 · 1414 · 19 | Δt table, Δt = 5 min |
| sixty-five · three hundred ninety | 65 · 390 | Δt table, "4 instruments" at 10 / 30 min |
| AMSR2 and ATMS in all pods · SSMIS largely apart | 2048 · SSMIS row 473 / 653 / 1963 | Pair matrix |
| twenty thousand · six hundred sixteen times · ninety-nine point eight | 20,216 · 616× · 99.84 % | Part 3 callout |
| the pod that covers half of Virginia | q111332, 50 % | Virginia per-pod table |
| no four-instrument conjunction all quarter · three-instrument ones within five minutes | "4 instruments" column all 0 · q111332 Δt = 5: 6 | Virginia per-pod table |
| fifty-two early · twenty-six late · over an hour | −51.8 · 25.6 · 72.0 | Swath table, Missing by (min) |
| seven thirty-one · seven thirty-four · seven thirty-six | 07:31:17 · 07:34:04 · 07:36:00 | Swath table, Pass start; figure 5 |
| thirty-eight thousand · twenty thousand · one hundred twenty-eight · eleven swaths | 38,574 · 20,216 · 128 · 11 | Criteria table and subtitle |
| a hundred fifty thousand → twelve and a half million | 149,621 → 12,451,867 | Scale table, Chunks |
| **catalog read / find / aggregate (this run)** | 42.4 / 12.9 / 1.4 s (v10 run 2026-10-31; v9 pre-run 2026-10-24: 42.5 / 12.2 / 1.5) | Scale table, quarter row |
| total run time (not shown) | 341 s (5.7 min) | Run time cell |

## 5. If the take runs long or short

**Long** (first cuts, ≈ 10 s each): scene 2's tropical/polar-pod sentence;
scene 8's "not a scaling law" clause;
scene 4's cover sentence after the pause.

**Short** (first additions, 10–20 s each): scene 3, after the matrix, the
q033303 drill-down's subtlety ("the triple AMSR2, ATMS and SSMIS even
outnumbers the pair AMSR2 and SSMIS, because the table counts arrivals and
SSMIS usually arrives last" — then the drill-down is shown); scene 8, over
the one-pass table ("two million one hundred thousand events where two
instruments meet, two hundred thirty thousand with three, five hundred
seventy-five with four" — 2,104,183 / 233,532 / 575); scene 5, over
figure 2 ("You can see why GMI meets everyone: its tilted track cuts across
the others"); scene 9, before the thank-you ("All of it live, against a
quarter of multi-instrument data, in minutes").
