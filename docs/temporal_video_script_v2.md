# Video script v2 — "STARE-PODS: Rendezvous Analytics" (~5 min)

Companion notebook: `starepandas/s3_starepods_examples_video_v2.ipynb`
(executed, outputs current — record by scrolling through it). Revision of
`docs/temporal_video_script.md` per the 260826 edit: the period-filter and
VCF roll-up parts are dropped, the remaining parts renumbered 1–5, and the
narration replaced with the revised text, then extended as the notebook
grew (inventory, pod occupancy, per-swath splits, the two Part-5 figures).
Full transcript ≈ 1,370 spoken words → ~9½ min at a comfortable 145 wpm;
the "optional beat" sections and the Part-1 occupancy paragraph can be cut
to shorten it.

## Structure

| Part | On screen | Beat |
|---|---|---|
| Intro | Title cell + part list | What STARE-PODS is; QL4 decomposition; the catalog as a rendezvous engine |
| The data in the store | Cell 3 output (instrument inventory) | The cast: 6 L1C granules, 4 instruments on 4 satellites — short optional beat in the transcript, or scroll it during the intro's "four microwave radiometers" line |
| 1. The temporal catalog | Cell 5 output (per-dataset table + pod-occupancy stats + sample rows) | Every chunk carries a measured `[t_start, t_end]` + pod code; 1220 of 2048 QL4 pods occupied, busiest (18 chunks) is q003200 — the 4-way pod |
| 2. Rendezvous analytics | Cell 7 output (Δt table → matrix → drill-down) | Who met whom, simultaneously — from database records alone |
| 3. Maps of rendezvous in swaths | Figure 1 (4 panels, outlined pods) | Panel counts, orbit shapes, where each outline color sits |
| 4. Rendezvous up close | Figures 2–3 (2-way q023003, 4-way q003200) | Each case spatially (data elements) and temporally (pass times) |
| 5. Rendezvous over a region of interest | Cell 13 output (query GT → figure 4: bbox → STARE cover map → 1478 / 154 / 116 → figure 5: returned data elements above the chunk timeline) | Space AND time in one query — the hatched cluster is the SSMIS morning pass the window drops |
| Outro | Recap cell | One-breath summary |

## Recording notes

- Scroll to each cell *before* its narration begins; let figures sit ~5 s in silence where marked (…pause…).
- The Δt table in Part 2 is the dramatic beat — consider highlighting the n-way columns with the cursor as you read them.
- Pod codes read aloud: "q-zero-zero-three-two-zero-zero" (q003200), "q-zero-two-three-zero-zero-three" (q023003).
- All numbers in the transcript match the executed notebook — if you re-execute against re-ingested data, re-check them.

## Transcript

### Intro  *(title cell)*

STARE Parallel Optimized Data Store, STARE-PODS, leverages the STARE
quad-tree hierarchy to partition and organize geospatial data and thus
enables distributed parallel processing of native data without requiring
re-gridding, i.e., resampling and/or re-projection.

For this demo of our STARE-PODS prototype, we took swaths of level 1C data
from four microwave radiometers, i.e., GPM's GMI, DMSP's SSMIS, GCOM-W1's
AMSR2, and JPSS's ATMS, and decomposed them at quad-furcation level four,
QL4. This results in 2048 pods, corresponding to 2048 QL4 STARE spherical
triangles, or trixels, each with a length of ~600+ km on a side, or an area
of approximately 200,000 km square.

The decomposition process extracts the data elements of each swath that
overlap with a given QL4 trixel or pod and packages them into a parquet chunk
file. Consequently, a pod (a logical construct) contains data chunks from all
the swaths that overlap (non-empty intersect) with the pod area, a natural
spatial co-location.

During the decomposition process, a database management system, PostgreSQL,
catalogues all necessary information to reconstitute the swath granule,
including the chunk's granule and pod identities, space-time extents, etc.,
as well as the metadata in the original HDF/netCDF file. Since the PostgreSQL
database possesses the chunks' spatiotemporal information, it can serve as a
cross-instrument rendezvous engine.

Everything you see in this demo runs live against AWS S3 and a Postgres
catalog.

### The data in the store  *(cell 3 output; optional beat)*

What did the decomposition put in the store? Six real granules from the
first of January 2025 — four microwave radiometers on four different
satellites, all Level 1C intercalibrated brightness temperatures: GMI on
GPM, SSMIS on DMSP F-18, AMSR2 on GCOM-W1, and ATMS on NOAA-21. Six
granules become fourteen datasets — one per scan group — and about seven
thousand chunks across twelve hundred pods.

### Part 1 — the temporal catalog  *(cell 5 output)*

When a granule is decomposed, the database records the start and end times of
every chunk, i.e., t-start and t-end — the earliest and latest scan times of
the data elements in the chunk — together with the chunk's spatial address,
i.e., pod code: in this case, the STARE index in a quaternary, base-4,
number. Similar to STARE indices, pod codes encode STARE's quad-tree
hierarchy — each digit refines the previous, similar to postal codes. The
scan times are actual times extracted from the granule's content, not the
time in the granule's file name.

Of the 2048 level-4 pods on the globe, 1220 hold at least one chunk. And
the fullest pod in the whole store — 18 chunks — is pod q003200. Keep an
eye on that one: it is exactly where all four instruments are about to
meet.

### Part 2 — rendezvous analytics  *(cell 7 output; cursor on the Δt table)*

Whether gridded, swath, or point data, once they are decomposed and
distributed into STARE-PODS, rendezvous analytics becomes straightforward; in
fact, it can be done with the database records, without needing to open and
read the chunks. A rendezvous here is a visit to the same pod area within a
given time window, delta-t, by two or more instruments.

Watch what happens as the window widens. With a 15-minute window, nearly 400
pods contain 2-way rendezvous and only 8 pods contain 3-way rendezvous, and
none contain 4-way rendezvous. At 30 minutes, the number of pods containing
2-way and 3-way rendezvous increases — nearly 100 pods have 3-way
rendezvous — but still none have 4-way. Finally, a 45-minute window yields 2
pods with 4-way rendezvous.

This matrix shows the rendezvous of each pair of instruments. AMSR2 and ATMS
have the most rendezvous, in 359 pods, because they have similar orbital
characteristics and are nearly co-orbiting. Conversely, GMI has fewer
rendezvous with other instruments, due to GPM's uncommon orbit.

Now we look into one of the two pods with 4-way rendezvous and list all the
rendezvous that occur there: 6 2-way combinations, 4 3-way, and one 4-way.

### Part 3 — maps of rendezvous in swaths  *(figure 1; …pause on it…)*

Where exactly are these rendezvous? This figure shows one instrument per
panel, shading every quad-furcation-level-4 pod in the instrument's orbit
swath. Panel titles list the number of level-4 pods for the corresponding
instruments: 359 for AMSR2, 502 for ATMS, 509 for GMI, and 672 for SSMIS —
the same F18 instrument seen on two passes.

The shapes of the shaded swath areas tell the orbital story. GMI is in a
65-degree inclined orbit, which never reaches the poles. The other three are
in sun-synchronous orbits and cover, or come very close to, the poles. AMSR2
and ATMS are both in a sun-synchronous orbit with (initially) the same
equator-crossing time. They trace almost the same ground track, which is why
they have the most pairwise rendezvous.

Black-outlined trixels mark the two pods with 4-way rendezvous, down in the
southern Indian Ocean. Dark red outlines the pods with 3-way rendezvous up
near the North Pole, whereas dark green outlines those with 2-way rendezvous,
along the eastern Pacific.

### Part 4 — rendezvous up close  *(figures 2 and 3; …pause on each…)*

Let's take a look at the rendezvous up close. A rendezvous requires visits of
the same pod within a specified time window, i.e., satisfying both spatial
and temporal criteria. Thus, each row of figures shows both: on the left,
data-element locations (QL27 trixels) in the pod, and on the right, each
instrument's overpass interval on the time axis.

An instrument in these figures and tables merges all of its scan groups —
the by-swath column shows the split — and each case is drawn with the
window it needs: fifteen minutes is already enough for the pair; only the
4-way needs the full forty-five.

First, the figures show a pod, pod q023003, with the closest 2-way
rendezvous. Both GMI and SSMIS swaths overlap the pod considerably, each with
eight to nine thousand data elements. So, the pod's area is almost painted
twice. Temporally, SSMIS arrived over the same pod on GMI's heels — just ten
seconds after GMI's last data element.

Now the main attraction, pod q003200, where a 4-way rendezvous occurred
within a 45-minute window. The temporal plot on the right reads like a relay.
SSMIS arrived first; AMSR2 followed ~17 minutes later, then ATMS ~2 more
minutes later. GMI arrived last, as the 45-minute window closed. The spatial
overlaps of the 4 instruments varied widely. Over 7,000 GMI data elements
intersected the pod, and AMSR2 intersected with ~1100 data elements. ATMS and
SSMIS, however, just clipped the pod.

Rendezvous here is performed and demonstrated at the pod level, providing a
fast but broad way to find overlaps between instruments' coverages. Once the
rendezvous are identified, the STARE hierarchy can be leveraged again to
quickly find exact spatial overlaps at the data-element level.

### Part 5 — rendezvous over a region of interest  *(cell 13 output; figures 4 and 5)*

At last, a closer-to-the-real-world scenario: rendezvous over a user-defined
region in a specified time window, requiring a spatial query for the
user-defined region with the pods and a temporal query for the time window.

In this example, the region of interest (ROI) is a bounding box covering the
pod containing the 4-way rendezvous. This ROI is first converted into a STARE
cover composed of STARE trixels of different levels, demonstrating STARE's
adaptive resolution feature. On the map, the dashed box is the region, and
the outlined triangles are its cover — thirteen level-4 trixels.

Time criterion alone produces 1478 chunks; space alone produces 154. Applying
both criteria produces 116 chunks. The difference of 38 chunks belongs to
SSMIS, which flew over the same region on a morning orbit too, outside the
time window.

The final figure shows the result itself. On top, the data elements the
query returns — whole chunks, at pod-level granularity, so they extend past
the box. Below, every spatially-selected chunk on the time axis: four
instruments inside the shaded window, and one hatched cluster — SSMIS's
morning visit, ten hours outside it. That is the AND of space and time in
one picture.

### Outro  *(recap cell)*

In this video, we demonstrated a construction of STARE Parallel Optimized
Data Store, STARE-PODS. During the construction, a DBMS is used to catalog
relevant metadata and supports pod-level rendezvous analytics, often the
first step in the integrative analysis of diverse data. These rendezvous can
be efficiently refined by leveraging the STARE hierarchy to obtain
data-element-level rendezvous.

Since the parquet chunks' file names contain pod codes and their contents
contain spatiotemporal information of the data, rendezvous analytics can also
be performed using the parquet chunks — albeit less efficiently than with the
database, it is still better than the existing way.
