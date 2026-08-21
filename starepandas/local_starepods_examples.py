#!/usr/bin/env python3
"""
Demo: Local STARE-PODS pipeline — no AWS or RDS required.

This script mirrors demo_reconstitute_hdf5_from_s3.py but uses the local
filesystem for Parquet storage and SQLite for metadata.  No cloud
credentials are needed.

Workflow
--------
1. Ingest granules from four instruments (GMI, SSMIS, AMSR2, ATMS) → local
   Parquet + SQLite metadata (four instruments so the overlap analytics in
   step 10 can show 2-, 3- and 4-way rendezvous)
2. Find intersecting data via STARE SIDs (bbox filter optional; default
   loads the full granule)
3. Load intersecting Parquet partitions from disk
4. Reconstitute HDF5 (S1 + S2 scans)
5. Structure comparison — reconstituted vs original
6. SQLite metadata verification

Temporal features
-----------------
7.  Temporal catalog — every chunk carries ``[t_start, t_end]`` + podcode
8.  Period-filtered intersection — data-level ``[t_start, t_end]`` overlap
9.  VCF temporal roll-up — union range per pod, on the fly
10. Multi-instrument overlap analytics — 2-, 3- and 4-way rendezvous

Plots
-----
11. Pod coverage map — the level-4 pods each instrument's chunks occupy;
    the rendezvous pods of steps 12-14 outlined (2-/3-/4-way)
12. Three 2-way rendezvous up close — the swaths (where) beside their pass
    windows (when)
13. Three 3-way rendezvous — the same view for trios
14. Two 4-way rendezvous — all four instruments over one pod

Spatial + temporal combined
---------------------------
15. Spatial+temporal intersection — a bbox region (→ STARE SIDs) ANDed with
    the 4-way rendezvous window in a single ``find_intersecting_data`` call

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/local_starepods_examples.py
"""

import os
import h5py
import matplotlib
matplotlib.use("Agg")          # headless: steps 11-14 write PNGs, never a window
import matplotlib.pyplot as plt
import pandas as pd
from starepandas.demo_lib import LocalStarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
LOCAL_ROOT = "/tmp/stare_pods_local"   # Parquet store + SQLite DB live here

# Resolve the sample granule from the in-repo test-data dir so the example is
# safe to run anywhere (no dependency on an external sample directory). Override
# with the STAREPODS_SAMPLE_GRANULE env var to point at your own granule.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GRANULE_DIR = os.path.join(_REPO_ROOT, "tests", "data", "granules")

# GMI + SSMIS are a co-located pair (both 2025-01-01, concurrent orbits) whose
# ground tracks cross within ~3 min in 42 shared pods — the tightest
# rendezvous in the demo data. Override with STAREPODS_SAMPLE_GRANULE /
# STAREPODS_SAMPLE_GRANULE_SSMIS.
GRANULE_FILE = os.environ.get(
    "STAREPODS_SAMPLE_GRANULE",
    os.path.join(_GRANULE_DIR,
                 "1C.GPM.GMI.XCAL2016-C.20250101-S112952-E130304.061572.V07B.HDF5"),
)

SSMIS_GRANULE_FILE = os.environ.get(
    "STAREPODS_SAMPLE_GRANULE_SSMIS",
    os.path.join(_GRANULE_DIR,
                 "1C.F18.SSMIS.XCAL2021-V.20250101-S112813-E131004.078441.V07B.HDF5"),
)

# A pair only ever fills the n=2 column of the slide-9 table. These four
# granules — one per instrument, all later the same day — are a verified
# **4-way** rendezvous: over pods q003200 and q003203 the passes arrive
# SSMIS 21:29 → AMSR2 21:46 → ATMS 21:47 → GMI 22:13, i.e. all four within
# ~45 min. Ingesting them alongside the pair above populates every cell of
# the slide-8 matrix and the n=2/3/4 columns of the slide-9 table from real
# data. The two windows are ~9 h apart, so they never cross-contaminate.
RENDEZVOUS_GRANULES = [
    ("SSMIS", os.path.join(_GRANULE_DIR,
              "1C.F18.SSMIS.XCAL2021-V.20250101-S195732-E213923.078446.V07B.HDF5")),
    ("AMSR2", os.path.join(_GRANULE_DIR,
              "1C.GCOMW1.AMSR2.XCAL2016-V.20250101-S201914-E215806.067167.V07A.HDF5")),
    ("ATMS",  os.path.join(_GRANULE_DIR,
              "1C.NOAA21.ATMS.XCAL2023-V.20250101-S201707-E215835.011117.V07A.HDF5")),
    ("GMI",   os.path.join(_GRANULE_DIR,
              "1C.GPM.GMI.XCAL2016-C.20250101-S204910-E222221.061578.V07B.HDF5")),
]

INSTRUMENTS = ["GMI", "SSMIS", "AMSR2", "ATMS"]

# Coincidence window for step 10. The four passes above span ~45 min, so a
# narrower window still shows 2- and 3-way rendezvous but no 4-way; step 10
# prints the whole Δt progression to make that visible.
OVERLAP_DT = pd.Timedelta(minutes=45)

# Steps 2–5 reconstitute GRANULE_FILE specifically, but two GMI granules are
# now ingested. This granule-level bound on "RawData Collected Time" (the
# filename-derived collection time, i.e. each granule's S… start: 11:29 vs
# 20:49) keeps those steps scoped to the first — distinct from the data-level
# ``period`` filter demonstrated in step 8.
PRIMARY_GRANULE_END_DATE = "2025-01-01T12:00:00"

# An instant lying in the ~7 h gap between the two GMI passes (which cover
# 11:29–13:03 and 20:49–22:22). Step 8 uses it to split the chunks by their
# data-level range — note it differs from the granule-level bound above,
# which compares against a granule's start rather than its chunks' spans.
PASS_SPLIT = pd.Timestamp("2025-01-01T16:00:00")

# STARE partition level used for both ingestion and bbox → SIDs lookup.
# Capped at MAX_PARTITION_LEVEL = 4 (~256 cells/granule), the regime
# where each Parquet partition is multi-MB — ideal for S3.
STARE_LEVEL = 4

# Bounding box filter — set to None to reconstitute the full granule,
# or e.g. (115, -30, 120, -25) to restrict to SW Australia / Perth.
BBOX = None   # (lon_min, lat_min, lon_max, lat_max) or None

DATASETS = ["GMI_S1", "GMI_S2"]

OUTPUT_HDF5 = "/tmp/reconsitution/gmi_local_reconstituted.h5"

# Steps 12-14 each draw rendezvous of one width, building up from pairs to
# the full four — ``(step, width, how many examples)``. Several examples per
# width keeps a step from resting on a single pod; they are spread across
# different instrument combinations wherever the data allows, so the three
# 2-way panels show three *different* pairs rather than the same pair three
# times. A width with fewer qualifying pods than asked for simply shows
# fewer; one with none is skipped.
RENDEZVOUS_PLAN = [
    (12, 2, 3),
    (13, 3, 3),
    (14, 4, 2),
]

# Where steps 11-14 write their PNGs.
PLOT_DIR = "/tmp/stare_pods_plots"

# Set to True to wipe LOCAL_ROOT before each run.
# IMPORTANT: re-running without cleaning causes duplicate SQLite entries,
# which inflates the reconstituted HDF5 (e.g. 3× the expected scan count).
# Keep True unless you intentionally want to append more granules.
CLEAN_BEFORE_RUN = True

# Reuse whatever a previous run already ingested under LOCAL_ROOT instead of
# wiping and re-ingesting (~85 s). On by default, like the S3 demo's flag;
# run with STAREPODS_SKIP_INGEST=0 to force a fresh ingest (the verification
# script does exactly that, so its end-to-end check still exercises ingest).
# When skipping, the CLEAN_BEFORE_RUN wipe is skipped too.
SKIP_INGEST = os.environ.get("STAREPODS_SKIP_INGEST", "1") != "0"
# ─────────────────────────────────────────────────────────────────────────────


def dump_structure(path, label):
    """Print HDF5 group/dataset tree with shapes and dtypes."""
    print(f"\n--- {label} ---")
    with h5py.File(path, "r") as f:
        def _visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                print(f"  /{name:50s} {str(obj.shape):20s} {obj.dtype}")
            elif isinstance(obj, h5py.Group) and name != "/":
                print(f"  /{name:50s} Group")
        f.visititems(_visit)


def main():
    print("=" * 60)
    print("Local STARE-PODS Demo  (no AWS / no RDS)")
    print("=" * 60)
    print(f"Granule   : {os.path.basename(GRANULE_FILE)}")
    print(f"Datasets  : {DATASETS}")
    print(f"BBox      : {BBOX}  (lon_min, lat_min, lon_max, lat_max)")
    print(f"Local root: {LOCAL_ROOT}")
    print()

    # ── Clean up previous run ─────────────────────────────────────────────────
    if CLEAN_BEFORE_RUN and not SKIP_INGEST and os.path.exists(LOCAL_ROOT):
        import shutil
        print(f"Cleaning up {LOCAL_ROOT} (CLEAN_BEFORE_RUN=True) ...")
        shutil.rmtree(LOCAL_ROOT)
        print()

    demo = LocalStarePodsDemo(local_root=LOCAL_ROOT)

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Ingest granules → local Parquet + SQLite")
    print("=" * 60)
    if SKIP_INGEST:
        from starepandas.io.granules import load_local_temporal_catalog
        existing = (load_local_temporal_catalog(demo.db_path)
                    if os.path.exists(demo.db_path) else pd.DataFrame())
        if existing.empty:
            raise RuntimeError(
                f"SKIP_INGEST is on but nothing is stored under {LOCAL_ROOT}. "
                f"Run with STAREPODS_SKIP_INGEST=0 to ingest the granules first."
            )
        print(f"SKIP_INGEST=True — reusing the {len(existing)} chunk(s) already "
              f"under {LOCAL_ROOT}, across {existing['Dataset'].nunique()} dataset(s).")
        print("Run with STAREPODS_SKIP_INGEST=0 to re-ingest from the granule files.")
    else:
        local_paths = demo.ingest_granules(GRANULE_FILE, instrument='GMI', level=STARE_LEVEL)
        print(f"GMI  : written {len(local_paths)} scan path(s).")
        ssmis_paths = demo.ingest_granules(SSMIS_GRANULE_FILE, instrument='SSMIS', level=STARE_LEVEL)
        print(f"SSMIS: written {len(ssmis_paths)} scan path(s).")
        for instrument, path in RENDEZVOUS_GRANULES:
            paths = demo.ingest_granules(path, instrument=instrument, level=STARE_LEVEL)
            print(f"{instrument:5s}: written {len(paths)} scan path(s).  "
                  f"({os.path.basename(path)})")
    print()

    # ── Step 2: Find intersecting data ────────────────────────────────────────
    print("=" * 60)
    print("Step 2: Find intersecting data via STARE SIDs")
    print("=" * 60)
    if BBOX is not None:
        location_sids = demo.get_sids_for_bbox(*BBOX, level=STARE_LEVEL)
        print(f"Generated {len(location_sids)} SIDs for bbox {BBOX}")
    else:
        location_sids = None
        print("No bbox filter — all groups will be loaded (full granule reconstitution)")

    intersecting = demo.find_intersecting_data(location_sids, instruments=['GMI'],
                                               end_date=PRIMARY_GRANULE_END_DATE)
    print(f"Found {len(intersecting)} intersecting metadata row(s) "
          f"for {os.path.basename(GRANULE_FILE)}.")
    if not intersecting.empty:
        print(intersecting[['Dataset', 'grouped_id', 'group_path']].to_string(index=False))
    print()

    # ── Step 3: Load intersecting partitions ─────────────────────────────────
    print("=" * 60)
    print("Step 3: Load intersecting Parquet partitions from disk")
    print("=" * 60)
    if not intersecting.empty:
        data_dict = demo.download_and_analyze(intersecting, instruments=list(intersecting['Dataset'].unique()))
        for ds_name, sdf in data_dict.items():
            print(f"  {ds_name}: {len(sdf)} rows, columns: {list(sdf.columns[:6])} …")
    else:
        print("  (No intersecting chunks found — skipping download step)")
        data_dict = {}
    print()

    # ── Step 4: Reconstitute HDF5 ─────────────────────────────────────────────
    print("=" * 60)
    print("Step 4: Reconstitute HDF5 (S1 + S2)")
    print("=" * 60)

    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]

    recon_path = demo.reconstitute_hdf5(
        dataset=DATASETS,
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        granule_name=granule_basename,
    )
    print(f"Written to: {recon_path}")
    print()

    # ── Step 5: Structure comparison ──────────────────────────────────────────
    print("=" * 60)
    print("Step 5: Structure comparison")
    print("=" * 60)
    dump_structure(recon_path, f"RECONSTITUTED  ({os.path.basename(recon_path)})")
    dump_structure(GRANULE_FILE, f"ORIGINAL       ({os.path.basename(GRANULE_FILE)})")

    # ── Step 6: SQLite verification ───────────────────────────────────────────
    print()
    print("=" * 60)
    print("Step 6: SQLite metadata verification")
    print("=" * 60)
    import sqlite3
    conn = sqlite3.connect(demo.db_path)
    rows = conn.execute(
        'SELECT Dataset, COUNT(*) as cnt FROM "PodsMetadata" GROUP BY Dataset ORDER BY Dataset'
    ).fetchall()
    conn.close()
    print(f"SQLite DB: {demo.db_path}")
    for dataset_name, cnt in rows:
        print(f"  {dataset_name}: {cnt} partition(s)")
    print()

    # ══════════════════════════════════════════════════════════════════════════
    # TEMPORAL FEATURES
    # ══════════════════════════════════════════════════════════════════════════
    from starepandas.io.granules import (
        load_local_metadata, load_local_temporal_catalog, load_local_vcf,
    )
    from starepandas.overlap import (
        rendezvous_events, overlap_matrix, overlap_pod_table, pair_drilldown,
        pod_drilldown, fold_instrument,
    )

    # ── Step 7: Temporal catalog columns ──────────────────────────────────────
    print("=" * 60)
    print("Step 7: Temporal catalog — each chunk carries [t_start, t_end] + podcode")
    print("=" * 60)
    catalog = load_local_temporal_catalog(demo.db_path)
    print(f"Thin catalog: {len(catalog)} chunk(s) across "
          f"{catalog['Dataset'].nunique()} dataset(s)")
    per_ds = catalog.groupby('Dataset').agg(
        chunks=('podcode', 'size'),
        first_start=('t_start', 'min'),
        last_end=('t_end', 'max'),
    )
    print(per_ds.to_string())
    print("\nSample rows (podcode / dataset / temporal range):")
    print(catalog.head(6).to_string(index=False))
    print()

    # ── Step 8: Period-filtered intersection ──────────────────────────────────
    print("=" * 60)
    print("Step 8: Period-filtered intersection (data-level [t_start,t_end] overlap)")
    print("=" * 60)
    # Two GMI passes are ingested, ~9 h apart. The data-level period filter
    # tells them apart — the same chunks, selected purely on their temporal
    # range rather than on which granule they came from.
    gmi = catalog[catalog['Dataset'].str.startswith('GMI')]
    passes = {
        "first GMI pass ": gmi[gmi['t_start'] < PASS_SPLIT],
        "second GMI pass": gmi[gmi['t_start'] >= PASS_SPLIT],
    }
    for label, chunks in passes.items():
        window = (chunks['t_start'].min(), chunks['t_end'].max())
        hit = demo.find_intersecting_data(None, ['GMI'], period=window)
        print(f"{label}: [{window[0]}, {window[1]}]  ({len(chunks)} chunk(s))")
        print(f"  -> {len(hit):3d} of {len(gmi)} GMI chunk(s) match this period")
    miss_period = (PASS_SPLIT - pd.Timedelta(days=10),
                   PASS_SPLIT - pd.Timedelta(days=9))
    miss = demo.find_intersecting_data(None, ['GMI'], period=miss_period)
    print(f"period 9–10 days earlier {miss_period}")
    print(f"  -> {len(miss):3d} chunk(s)")
    print()

    # ── Step 9: VCF temporal roll-up ──────────────────────────────────────────
    print("=" * 60)
    print("Step 9: VCF temporal roll-up — union [t_start,t_end] per pod (level 1)")
    print("=" * 60)
    vcf = load_local_vcf(demo.db_path, level=1)
    print(f"{len(vcf)} level-1 VCF node(s) (one per octant subtree):")
    print(vcf.to_string(index=False))
    print()

    # ── Step 10: Multi-instrument overlap analytics ───────────────────────────
    print("=" * 60)
    print("Step 10: Multi-instrument overlap analytics")
    print("=" * 60)
    print("How the coincidence window Δt widens what counts as a rendezvous:")
    for dt in (pd.Timedelta(minutes=15), pd.Timedelta(minutes=30), OVERLAP_DT):
        ev = rendezvous_events(catalog, dt)
        table = overlap_pod_table(ev)
        by_n = {int(n): int(table[n].gt(0).sum()) for n in table.columns}
        print(f"  Δt={str(dt).split()[-1]}  {len(ev):5d} event(s)  "
              f"{ev['podcode'].nunique():4d} pod(s)   pods by n-way: {by_n}")
    print("  (A 4-way needs Δt≥45 min: the four passes over q003200/q003203")
    print("   arrive SSMIS 21:29 → AMSR2 21:46 → ATMS 21:47 → GMI 22:13.)")
    print()

    events = rendezvous_events(catalog, OVERLAP_DT)
    pod_table = overlap_pod_table(events)
    widest = max(pod_table.columns)
    print(f"Headline views at Δt={OVERLAP_DT}:")
    print()
    print("Instrument×instrument matrix — pods where A & B rendezvous (slide 8):")
    print(overlap_matrix(events).to_string())
    print("\nPer-pod n-way combination counts — cell (pod, n) = distinct")
    print("n-instrument combos rendezvousing in that pod (slide 9):")
    print(pod_table.sort_values(sorted(pod_table.columns, reverse=True),
                                ascending=False).head(10).to_string())
    print(f"\n{int(pod_table[widest].gt(0).sum())} pod(s) see all {widest} "
          f"instruments; {int(pod_table.get(3, pd.Series(dtype=int)).gt(0).sum())} "
          f"see a 3-way.")
    print("\nGMI–SSMIS pair drill-down (first 8 pods + times):")
    print(pair_drilldown(events, 'GMI', 'SSMIS').head(8).to_string(index=False))

    for pod in pod_table[pod_table[widest].gt(0)].index[:1]:
        print(f"\nPod drill-down for {pod} — every combination meeting there:")
        detail = pod_drilldown(events, pod)
        print(detail.drop(columns='times').to_string(index=False))
        meeting = detail[detail['n_instruments'] == widest]
        for _, row in meeting.iterrows():
            print(f"  all {widest} at: "
                  f"{', '.join(str(t) for t in row['times'])}")
    print()

    # ══════════════════════════════════════════════════════════════════════════
    # PLOTS
    # ══════════════════════════════════════════════════════════════════════════
    from starepandas.demo_plots import (
        plot_pod_coverage, plot_rendezvous, pod_pixels, pod_trixels,
        rendezvous_examples,
    )

    # ── Step 11: Pod coverage map ─────────────────────────────────────────────
    print("=" * 60)
    print("Step 11: Where each instrument flew — pod coverage map")
    print("=" * 60)
    # The pods steps 12-14 visit up close are outlined here so they can be
    # located on the world map: every 4-way pod in black, and the 3-way /
    # 2-way *example* pods in darkred / darkgreen. (Outlining every pod of a
    # width would blanket the map — 341 pods hold a 2-way.) The examples are
    # selected once, before the map, and steps 12-14 reuse the same picks.
    demo_meta = load_local_metadata(demo.db_path)
    examples_by_width = {
        n_way: rendezvous_examples(events, n_way, count=wanted,
                                   metadata=demo_meta, window=OVERLAP_DT)
        for _, n_way, wanted in RENDEZVOUS_PLAN
    }
    quad_pods = list(pod_table[pod_table[widest].gt(0)].index)
    outlines = [
        (quad_pods, 'black', f'{widest}-way'),
        ([p for p, _, _ in examples_by_width.get(3, [])], 'darkred',
         '3-way examples'),
        ([p for p, _, _ in examples_by_width.get(2, [])], 'darkgreen',
         '2-way examples'),
    ]
    fig = plot_pod_coverage(catalog, highlight=outlines)
    coverage_path = os.path.join(PLOT_DIR, "pod_coverage.png")
    os.makedirs(PLOT_DIR, exist_ok=True)
    fig.savefig(coverage_path, dpi=110)
    plt.close(fig)
    print(f"One panel per instrument, level-4 pods shaded; the {widest}-way pod(s)")
    print(f"{quad_pods} outlined in black, the 3-way example pods in dark red")
    print("and the 2-way example pods in dark green.")
    print("GMI's 65° inclination confines it to a ±67° band, while the")
    print("sun-synchronous instruments run pole to pole — which is why a")
    print("4-way needs them to converge at a mid-southern latitude.")
    print(f"Written to: {coverage_path}")
    print()

    # ── Steps 12-14: rendezvous up close, from pairs up to all four ───────────
    # Left: the swaths inside the pod's trixel. Right: their pass windows, all
    # landing inside one Δt. Both halves are required — same pod alone is not a
    # rendezvous, and neither is same time. Note the granularity: co-location
    # means the same level-4 pod (~500 km across), not the same pixel, so the
    # swaths need not touch.
    for step, n_way, wanted in RENDEZVOUS_PLAN:
        print("=" * 60)
        print(f"Step {step}: {n_way}-way rendezvous up close — where *and* when")
        print("=" * 60)
        # Pods whose widest rendezvous is *exactly* n_way, so a "2-way" is not
        # illustrated with a picture of four instruments. The examples spread
        # across different instrument combinations, and `window` scopes the
        # legibility score to the chunks present *at* the meeting — without it
        # a pod qualifies on an instrument that crossed it on another orbit.
        # Selected up front in step 11, whose map outlines these same pods.
        examples = examples_by_width[n_way]
        print(f"{len(examples)} example(s) of a {n_way}-way "
              f"({wanted} requested):")
        for pod, meeting, _ in examples:
            window = (meeting - OVERLAP_DT, meeting + OVERLAP_DT)
            passes = pod_pixels(demo, demo_meta, pod, window)
            print(f"\nPod {pod}: {' + '.join(sorted(passes))}, "
                  f"rendezvous completes {meeting}")
            for instrument, pixels in sorted(passes.items(),
                                             key=lambda kv: kv[1]['timestamp'].min()):
                print(f"  {instrument:6s} {len(pixels):6d} px  "
                      f"{pixels['timestamp'].min()} .. {pixels['timestamp'].max()}")
            fig = plot_rendezvous(pod, passes, meeting, OVERLAP_DT)
            rendezvous_path = os.path.join(PLOT_DIR,
                                           f"rendezvous_{n_way}way_{pod}.png")
            fig.savefig(rendezvous_path, dpi=110, bbox_inches='tight')
            plt.close(fig)
            print(f"  Written to: {rendezvous_path}")
        print()

    # ── Step 15: Spatial+temporal intersection — both filters in one query ────
    print("=" * 60)
    print("Step 15: Spatial+temporal intersection over the 4-way pod")
    print("=" * 60)
    # The production question behind the rendezvous plots: "which chunks are
    # over THIS region during THIS window?" — the spatial pod match and the
    # data-level period filter ANDed inside one find_intersecting_data call.
    # The region is a bbox around the 4-way pod's trixel (a scientist's query
    # arrives as lat/lon, not as a pod code); the window is that rendezvous's
    # own [meeting ± Δt]. Steps 2 and 8 each showed one filter alone — this is
    # the first query to apply both at once.
    pod, meeting, _ = examples_by_width[widest][0]
    lon_min, lat_min, lon_max, lat_max = pod_trixels([pod]).total_bounds
    location_sids = demo.get_sids_for_bbox(lon_min, lat_min, lon_max, lat_max,
                                           level=STARE_LEVEL)
    window = (meeting - OVERLAP_DT, meeting + OVERLAP_DT)
    print(f"Region: bbox around pod {pod}'s trixel — "
          f"lon [{lon_min:.1f}, {lon_max:.1f}], lat [{lat_min:.1f}, {lat_max:.1f}]"
          f" → {len(location_sids)} level-{STARE_LEVEL} cover SID(s)")
    print(f"Window: {window[0]} .. {window[1]}  "
          f"(the {widest}-way meeting ± Δt)")
    print()

    spatial_only = demo.find_intersecting_data(location_sids, INSTRUMENTS)
    temporal_only = load_local_metadata(demo.db_path, period=window)
    both = demo.find_intersecting_data(location_sids, INSTRUMENTS,
                                       period=window)
    print(f"Temporal only (whole demo footprint in the window): "
          f"{len(temporal_only):4d} chunk(s)")
    print(f"Spatial only  (this region, any time of day):       "
          f"{len(spatial_only):4d} chunk(s)")
    print(f"Spatial AND temporal:                               "
          f"{len(both):4d} chunk(s)")
    print()
    # The gap between the last two is SSMIS: its morning granule crossed the
    # same region ~10 h before the rendezvous, so the period filter drops
    # those chunks, while the other three instruments (one pass each over the
    # region) keep every spatial match.
    per_instrument = (
        both.assign(instrument=both['Dataset'].map(fold_instrument))
            .groupby('instrument')
            .agg(both_filters=('podcode', 'size'),
                 first_start=('t_start', 'min'),
                 last_end=('t_end', 'max'))
    )
    per_instrument.insert(0, 'spatial_only', (
        spatial_only.assign(
            instrument=spatial_only['Dataset'].map(fold_instrument))
        .groupby('instrument').size()
    ))
    print("Per instrument — chunks matching the region alone vs both filters,")
    print("and the surviving pass ranges (all four inside the window):")
    print(per_instrument.to_string())
    print(f"\nThe surviving chunks lie in {both['podcode'].nunique()} pod(s), "
          f"among them the {widest}-way pod(s) "
          f"{sorted(p for p in both['podcode'].unique() if p in quad_pods)}.")
    print()

    print("Done.")


if __name__ == "__main__":
    main()
