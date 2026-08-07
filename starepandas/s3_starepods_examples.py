#!/usr/bin/env python3
"""
Demo: STARE-PODS pipeline on AWS S3 + RDS Postgres.

S3-counterpart of local_starepods_examples.py. Mirrors that script's
six-step flow but runs against real AWS S3 (Parquet partitions) and a
real RDS Postgres `PodsMetadata` table.

Workflow
--------
1. Ingest granules from four instruments (GMI, SSMIS, AMSR2, ATMS) → S3
   Parquet + RDS metadata (clean_before_run wipes prior data for the same
   prefix before the first ingest; the rest append).
2. Find intersecting data via STARE SIDs + RDS (bbox filter optional; default
   loads the full granule).
3. Download intersecting Parquet partitions from S3.
4. Reconstitute HDF5 (S1 + S2 scans).
5. Structure comparison — reconstituted vs original.
6. RDS metadata verification.

Temporal features
-----------------
7.  Temporal catalog — every chunk carries ``[t_start, t_end]`` + podcode
8.  Period-filtered load — data-level ``[t_start, t_end]`` overlap
9.  VCF temporal roll-up — union range per pod, on the fly
10. Multi-instrument overlap analytics — 2-, 3- and 4-way rendezvous

Plots
-----
11. Pod coverage map — the level-4 pods each instrument's chunks occupy
12-14. One rendezvous up close at each width (4-, 3- and 2-way) — the swaths
    (where) beside their pass windows (when)

Note: the S3/RDS temporal loaders read the **shared** ``PodsMetadata``
catalog — every ingest in the RDS table, not only this demo's granule. That
is the production query surface, so the temporal counts below reflect the
whole catalog (filtered by instrument), unlike the local demo's fresh SQLite.

Requirements
------------
- starepandas/.config (next to this script) with AWS + RDS credentials.
- Sample granules. Default to the in-repo GMI + SSMIS pair plus the four
  rendezvous granules (``RENDEZVOUS_GRANULES``); override the pair with the
  ``STAREPODS_SAMPLE_GRANULE`` / ``STAREPODS_SAMPLE_GRANULE_SSMIS`` env vars.

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/s3_starepods_examples.py
"""

import os
import time
import h5py
import matplotlib
matplotlib.use("Agg")          # headless: steps 11-14 write PNGs, never a window
import matplotlib.pyplot as plt
import pandas as pd
from starepandas.demo_lib import StarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
# AWS + RDS credentials. Resolved relative to this file so it works from any cwd.
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".config")

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
# **4-way** rendezvous: over pods q03200 and q03203 the passes arrive
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

# Steps 2–5 reconstitute GRANULE_FILE specifically, and two GMI granules are
# now ingested; the granule-basename marker below already scopes them, so no
# extra filter is needed. Step 8 splits the two GMI passes at this instant,
# which lies in the ~7 h gap between them (11:29–13:03 and 20:49–22:22).
PASS_SPLIT = pd.Timestamp("2025-01-01T16:00:00")

# S3 root where Parquet partitions and RDS metadata for this demo live.
S3_PREFIX = "s3://zarrpods/gmi-demo-parquet"

# The RDS catalog is shared with every other ingest, so steps 10-14 scope
# their reads to the chunks this demo actually wrote. The storage root is the
# only filter that can do that: two ingests of the same instrument covering
# the same hours are indistinguishable by Dataset or period. (Scoping by time
# window instead was both leaky — other jobs' granules straddle any window's
# edges — and lossy, since a window that excludes them also clips this demo's
# own passes.) Scoped this way the S3 catalog matches the local one row for
# row, so both demos report identical analytics.
DEMO_PATH_PREFIX = S3_PREFIX

# STARE partition level used for both ingestion and bbox → SIDs lookup.
# Capped at MAX_PARTITION_LEVEL = 4 (~256 cells/granule), the regime
# where each Parquet partition is multi-MB — ideal for S3.
STARE_LEVEL = 4

# Bounding box filter — set to None to reconstitute the full granule
# (matching local_starepods_examples.py), or e.g. (115, -30, 120, -25)
# to restrict to SW Australia / Perth.
BBOX = None   # full granule, no spatial filter — mirrors the local demo

DATASETS = ["GMI_S1", "GMI_S2"]

OUTPUT_HDF5 = "/tmp/gmi_s3_reconstituted.h5"

# Steps 12-14 each draw one rendezvous, at these widths (widest first). A
# width with no pod of exactly that size is skipped.
RENDEZVOUS_WIDTHS = [4, 3, 2]
RENDEZVOUS_STEPS = [12, 13, 14]

# Where steps 11-14 write their PNGs.
PLOT_DIR = "/tmp/stare_pods_plots_s3"

# Set to True to wipe S3_PREFIX (S3 objects + RDS metadata rows) before
# ingesting. Mirrors the local demo's CLEAN_BEFORE_RUN flag — prevents
# duplicate RDS rows on re-runs. Default True for clean reproducible runs.
CLEAN_BEFORE_RUN = True
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
    print("STARE-PODS Demo on S3 + RDS")
    print("=" * 60)
    print(f"Granule  : {os.path.basename(GRANULE_FILE)}")
    print(f"Datasets : {DATASETS}")
    print(f"BBox     : {BBOX}  (lon_min, lat_min, lon_max, lat_max)")
    print(f"S3 root  : {S3_PREFIX}")
    print(f"Clean    : {CLEAN_BEFORE_RUN}")
    print()

    demo = StarePodsDemo(aws_config_path=CONFIG_PATH)

    # Granule basename — used as a substring filter on group_path. Note:
    # as of the quaternary pod-code layout (2026-06-14) the S3 layout is FLAT
    # and the granule basename is embedded in the chunk *filename*, bracketed
    # by '-':
    #
    #   <S3_PREFIX>/<podcode>-<granule_basename>-<dataset>.parquet
    #
    # So the old "granule_s3_prefix = S3_PREFIX + '/' + basename" scoping
    # would no longer match any rows. We now scope by substring match on
    # the basename (which is unique within the bucket).
    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]
    granule_path_marker = f"-{granule_basename}-"   # matches the filename-embedded span

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Ingest granules → S3 Parquet + RDS")
    print("=" * 60)
    t0 = time.perf_counter()
    s3_paths = demo.ingest_granules(
        data_path=GRANULE_FILE,
        instrument="GMI",
        s3_prefix=S3_PREFIX,
        level=STARE_LEVEL,
        clean_before_run=CLEAN_BEFORE_RUN,   # wipes the prefix before GMI
    )
    # Second instrument appends — clean_before_run=False so it does NOT wipe
    # the GMI data just written to the same prefix.
    ssmis_paths = demo.ingest_granules(
        data_path=SSMIS_GRANULE_FILE,
        instrument="SSMIS",
        s3_prefix=S3_PREFIX,
        level=STARE_LEVEL,
        clean_before_run=False,
    )
    print(f"GMI  : stored {len(s3_paths)} dataset path(s)")
    print(f"SSMIS: stored {len(ssmis_paths)} dataset path(s)")
    for instrument, path in RENDEZVOUS_GRANULES:
        paths = demo.ingest_granules(
            data_path=path,
            instrument=instrument,
            s3_prefix=S3_PREFIX,
            level=STARE_LEVEL,
            clean_before_run=False,
        )
        print(f"{instrument:5s}: stored {len(paths)} dataset path(s)  "
              f"({os.path.basename(path)})")
    print(f"Ingest wall: {time.perf_counter() - t0:.2f} s")
    print()

    # ── Step 2: Find intersecting data ────────────────────────────────────────
    print("=" * 60)
    print("Step 2: Find intersecting data via STARE SIDs")
    print("=" * 60)
    if BBOX is not None:
        location_sids = demo.get_sids_for_bbox(*BBOX, level=STARE_LEVEL)
        print(f"Generated {len(location_sids)} SIDs for bbox {BBOX}")
    else:
        location_sids = []
        print("BBOX is None — full granule reconstitution (no spatial filter)")

    t0 = time.perf_counter()
    intersecting = demo.find_intersecting_data(location_sids, instruments=["GMI"]) \
        if location_sids else None
    if intersecting is not None:
        # Scope to our granule so other ingests' data doesn't pollute the
        # result. Substring match on the basename, which the flat pod-code
        # layout embeds in the chunk filename (bracketed by '-').
        if not intersecting.empty and "group_path" in intersecting.columns:
            intersecting = intersecting[
                intersecting["group_path"].str.contains(granule_path_marker, regex=False)
            ]
        print(f"Find wall: {time.perf_counter() - t0:.2f} s")
        print(f"Found {len(intersecting)} intersecting metadata row(s).")
    print()

    # ── Step 3: Download intersecting partitions ─────────────────────────────
    print("=" * 60)
    print("Step 3: Download intersecting Parquet partitions from S3")
    print("=" * 60)
    if intersecting is not None and not intersecting.empty:
        t0 = time.perf_counter()
        data_dict = demo.download_and_analyze(
            intersecting, instruments=list(intersecting["Dataset"].unique()),
        )
        print(f"Download wall: {time.perf_counter() - t0:.2f} s")
        for ds_name, sdf in data_dict.items():
            print(f"  {ds_name}: {len(sdf)} rows, columns: {list(sdf.columns[:6])} …")
    else:
        print("  (No intersecting partitions — skipping download step)")
        data_dict = {}
    print()

    # ── Step 4: Reconstitute HDF5 ─────────────────────────────────────────────
    print("=" * 60)
    print("Step 4: Reconstitute HDF5 (S1 + S2)")
    print("=" * 60)
    t0 = time.perf_counter()
    # Scope to the storage root AND to this granule. The prefix alone is not
    # enough now that two GMI granules are ingested: the flat layout puts the
    # granule name after the pod code, so both would be reconstituted merged
    # into one file (with twice the scan lines of the original).
    recon_path = demo.reconstitute_hdf5(
        dataset=DATASETS,
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        s3_prefix=S3_PREFIX,
        granule_name=granule_basename,
    )
    print(f"Reconstitute wall: {time.perf_counter() - t0:.2f} s")
    print(f"Written to: {recon_path}")
    print()

    # ── Step 5: Structure comparison ──────────────────────────────────────────
    print("=" * 60)
    print("Step 5: Structure comparison")
    print("=" * 60)
    dump_structure(recon_path, f"RECONSTITUTED  ({os.path.basename(recon_path)})")
    dump_structure(GRANULE_FILE, f"ORIGINAL       ({os.path.basename(GRANULE_FILE)})")

    # ── Step 6: RDS metadata verification ─────────────────────────────────────
    print()
    print("=" * 60)
    print("Step 6: RDS metadata verification")
    print("=" * 60)
    from starepandas.staredataframe import _ensure_rds_db_and_table
    conn = _ensure_rds_db_and_table("StarePodsMetadata")
    try:
        with conn.cursor() as cur:
            # Flat pod-code layout: the basename is embedded in the chunk
            # filename, so use a LIKE substring match (not a startswith prefix).
            cur.execute(
                'SELECT "Dataset", COUNT(*) '
                'FROM "PodsMetadata" '
                'WHERE "MetadataJson"->>%s LIKE %s '
                'GROUP BY "Dataset" ORDER BY "Dataset"',
                ("group_path", f"%{granule_path_marker}%"),
            )
            rows = cur.fetchall()
        print(f"RDS scope: group_path contains '{granule_path_marker}'")
        for ds, cnt in rows:
            print(f"  {ds}: {cnt} partition(s)")
    finally:
        conn.close()
    print()

    # ══════════════════════════════════════════════════════════════════════════
    # TEMPORAL FEATURES
    #
    # The S3/RDS temporal loaders read the SHARED PodsMetadata catalog — every
    # ingest in the RDS table, not only this demo's granule — filtered here by
    # instrument. That is the production query surface, so counts reflect the
    # whole catalog (contrast the local demo's fresh isolated SQLite).
    # ══════════════════════════════════════════════════════════════════════════
    from starepandas.io.granules import (
        load_s3_metadata, load_s3_temporal_catalog, load_s3_vcf,
    )
    from starepandas.overlap import (
        rendezvous_events, overlap_matrix, overlap_pod_table, pair_drilldown,
        pod_drilldown,
    )

    # ── Step 7: Temporal catalog columns ──────────────────────────────────────
    print("=" * 60)
    print("Step 7: Temporal catalog — each chunk carries [t_start, t_end] + podcode")
    print("=" * 60)
    catalog = pd.concat(
        [load_s3_temporal_catalog(dataset_prefix=instrument)
         for instrument in INSTRUMENTS],
        ignore_index=True,
    )
    print(f"Thin catalog ({'+'.join(INSTRUMENTS)}, catalog-wide): "
          f"{len(catalog)} chunk(s) across {catalog['Dataset'].nunique()} dataset(s)")
    per_ds = catalog.groupby('Dataset').agg(
        chunks=('podcode', 'size'),
        first_start=('t_start', 'min'),
        last_end=('t_end', 'max'),
    )
    print(per_ds.to_string())
    print("\nSample rows (podcode / dataset / temporal range):")
    print(catalog.head(6).to_string(index=False))
    print()

    # ── Step 8: Period-filtered load ──────────────────────────────────────────
    print("=" * 60)
    print("Step 8: Period-filtered load (data-level [t_start,t_end] overlap)")
    print("=" * 60)
    # Two GMI passes are ingested, ~9 h apart. The data-level period filter
    # tells them apart — the same chunks, selected purely on their temporal
    # range rather than on which granule they came from. The predicate pushes
    # into SQL, so RDS returns only the matching rows.
    gmi = catalog[catalog['Dataset'].str.startswith('GMI')]
    passes = {
        "first GMI pass ": gmi[gmi['t_start'] < PASS_SPLIT],
        "second GMI pass": gmi[gmi['t_start'] >= PASS_SPLIT],
    }
    for label, chunks in passes.items():
        window = (chunks['t_start'].min(), chunks['t_end'].max())
        hit = load_s3_metadata(dataset_prefix='GMI', period=window)
        print(f"{label}: [{window[0]}, {window[1]}]  ({len(chunks)} chunk(s))")
        print(f"  -> {len(hit):3d} of {len(gmi)} GMI chunk(s) match this period")
    miss_period = (PASS_SPLIT - pd.Timedelta(days=10),
                   PASS_SPLIT - pd.Timedelta(days=9))
    miss = load_s3_metadata(dataset_prefix='GMI', period=miss_period)
    print(f"period 9–10 days earlier {miss_period}")
    print(f"  -> {len(miss):3d} chunk(s)")
    print()

    # ── Step 9: VCF temporal roll-up ──────────────────────────────────────────
    print("=" * 60)
    print("Step 9: VCF temporal roll-up — union [t_start,t_end] per pod (level 1)")
    print("=" * 60)
    vcf = load_s3_vcf(1, dataset_prefix='GMI')
    print(f"{len(vcf)} level-1 VCF node(s) for GMI (one per octant subtree):")
    print(vcf.to_string(index=False))
    print()

    # ── Step 10: Multi-instrument overlap analytics ───────────────────────────
    print("=" * 60)
    print("Step 10: Multi-instrument overlap analytics")
    print("=" * 60)
    # The catalog-wide read (step 7) mixes every ingest in the shared RDS table.
    # Scope the sweep to this demo's own storage root so the result is exactly
    # this demo's rendezvous (the LIKE on the chunk's group_path pushes into
    # SQL, and one call covers every instrument).
    demo_catalog = load_s3_temporal_catalog(path_prefix=DEMO_PATH_PREFIX)
    print(f"Scoped to {DEMO_PATH_PREFIX}: {len(demo_catalog)} chunk(s) "
          f"across {demo_catalog['Dataset'].nunique()} dataset(s) — this "
          f"demo's ingests only.")
    print("How the coincidence window Δt widens what counts as a rendezvous:")
    for dt in (pd.Timedelta(minutes=15), pd.Timedelta(minutes=30), OVERLAP_DT):
        ev = rendezvous_events(demo_catalog, dt)
        table = overlap_pod_table(ev)
        by_n = {int(n): int(table[n].gt(0).sum()) for n in table.columns}
        print(f"  Δt={str(dt).split()[-1]}  {len(ev):5d} event(s)  "
              f"{ev['podcode'].nunique():4d} pod(s)   pods by n-way: {by_n}")
    print("  (A 4-way needs Δt≥45 min: the four passes over q03200/q03203")
    print("   arrive SSMIS 21:29 → AMSR2 21:46 → ATMS 21:47 → GMI 22:13.)")
    print()

    events = rendezvous_events(demo_catalog, OVERLAP_DT)
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
        plot_pod_coverage, plot_rendezvous, pod_pixels, rendezvous_of_size,
    )

    # ── Step 11: Pod coverage map ─────────────────────────────────────────────
    print("=" * 60)
    print("Step 11: Where each instrument flew — pod coverage map")
    print("=" * 60)
    quad_pods = list(pod_table[pod_table[widest].gt(0)].index)
    fig = plot_pod_coverage(demo_catalog, highlight=quad_pods)
    os.makedirs(PLOT_DIR, exist_ok=True)
    coverage_path = os.path.join(PLOT_DIR, "pod_coverage.png")
    fig.savefig(coverage_path, dpi=110)
    plt.close(fig)
    print(f"One panel per instrument, level-4 pods shaded; the {widest}-way pod(s)")
    print(f"{quad_pods} outlined in black.")
    print("GMI's 65° inclination confines it to a ±67° band, while the")
    print("sun-synchronous instruments run pole to pole — which is why a")
    print("4-way needs them to converge at a mid-southern latitude.")
    print(f"Written to: {coverage_path}")
    print()

    # ── Steps 12-14: the same view at three rendezvous widths ─────────────────
    # Left: the swaths inside the pod's trixel. Right: their pass windows, all
    # landing inside one Δt. Both halves are required — same pod alone is not a
    # rendezvous, and neither is same time. Note the granularity: co-location
    # means the same level-4 pod (~500 km across), not the same pixel, so the
    # swaths need not touch.
    demo_meta = load_s3_metadata(path_prefix=DEMO_PATH_PREFIX)
    for step, n_way in zip(RENDEZVOUS_STEPS, RENDEZVOUS_WIDTHS):
        print("=" * 60)
        print(f"Step {step}: A {n_way}-way rendezvous up close — where *and* when")
        print("=" * 60)
        # Pods whose widest rendezvous is *exactly* n_way, so a "2-way" is not
        # illustrated with a picture of four instruments.
        pod, meeting, _ = rendezvous_of_size(events, n_way, metadata=demo_meta)
        window = (meeting - OVERLAP_DT, meeting + OVERLAP_DT)
        passes = pod_pixels(demo, demo_meta, pod, window)
        print(f"Pod {pod}: {n_way} instruments, rendezvous completes {meeting}")
        for instrument, pixels in sorted(passes.items(),
                                         key=lambda kv: kv[1]['timestamp'].min()):
            print(f"  {instrument:6s} {len(pixels):6d} px  "
                  f"{pixels['timestamp'].min()} .. {pixels['timestamp'].max()}")
        fig = plot_rendezvous(pod, passes, meeting, OVERLAP_DT)
        rendezvous_path = os.path.join(PLOT_DIR, f"rendezvous_{n_way}way_{pod}.png")
        fig.savefig(rendezvous_path, dpi=110, bbox_inches='tight')
        plt.close(fig)
        print(f"Written to: {rendezvous_path}")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
