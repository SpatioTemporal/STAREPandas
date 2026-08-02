#!/usr/bin/env python3
"""
Demo: Local STARE-PODS pipeline — no AWS or RDS required.

This script mirrors demo_reconstitute_hdf5_from_s3.py but uses the local
filesystem for Parquet storage and SQLite for metadata.  No cloud
credentials are needed.

Workflow
--------
1. Ingest GMI + SSMIS granules → local Parquet + SQLite metadata (two
   instruments so the overlap analytics in step 10 have something to compare)
2. Find intersecting data via STARE SIDs (bbox filter optional; default
   loads the full granule)
3. Load intersecting Parquet partitions from disk
4. Reconstitute HDF5 (S1 + S2 scans)
5. Structure comparison — reconstituted vs original
6. SQLite metadata verification

Temporal features (temporal-stare-pods issues 01–06)
----------------------------------------------------
7.  Temporal catalog — every chunk carries ``[t_start, t_end]`` + podcode
8.  Period-filtered intersection — data-level ``[t_start, t_end]`` overlap
9.  VCF temporal roll-up — union range per pod, on the fly
10. Multi-instrument overlap analytics — GMI↔SSMIS rendezvous

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/local_starepods_examples.py
"""

import os
import h5py
import pandas as pd
from starepandas.demo_lib import LocalStarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
LOCAL_ROOT = "/tmp/stare_pods_local"   # Parquet store + SQLite DB live here

# Resolve the sample granule from the in-repo test-data dir so the example is
# safe to run anywhere (no dependency on an external sample directory). Override
# with the STAREPODS_SAMPLE_GRANULE env var to point at your own granule.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# GMI + SSMIS are a co-located pair (both 2025-01-01, concurrent orbits) whose
# ground tracks cross within ~3 min in 42 shared pods — so the overlap
# analytics (step 10) show genuine multi-instrument rendezvous. Override with
# STAREPODS_SAMPLE_GRANULE / STAREPODS_SAMPLE_GRANULE_SSMIS.
GRANULE_FILE = os.environ.get(
    "STAREPODS_SAMPLE_GRANULE",
    os.path.join(
        _REPO_ROOT, "tests", "data", "granules",
        "1C.GPM.GMI.XCAL2016-C.20250101-S112952-E130304.061572.V07B.HDF5",
    ),
)

SSMIS_GRANULE_FILE = os.environ.get(
    "STAREPODS_SAMPLE_GRANULE_SSMIS",
    os.path.join(
        _REPO_ROOT, "tests", "data", "granules",
        "1C.F18.SSMIS.XCAL2021-V.20250101-S112813-E131004.078441.V07B.HDF5",
    ),
)

# STARE partition level used for both ingestion and bbox → SIDs lookup.
# Capped at MAX_PARTITION_LEVEL = 4 (~256 cells/granule), the regime
# where each Parquet partition is multi-MB — ideal for S3.
STARE_LEVEL = 4

# Bounding box filter — set to None to reconstitute the full granule,
# or e.g. (115, -30, 120, -25) to restrict to SW Australia / Perth.
BBOX = None   # (lon_min, lat_min, lon_max, lat_max) or None

DATASETS = ["GMI_S1", "GMI_S2"]

OUTPUT_HDF5 = "/tmp/reconsitution/gmi_local_reconstituted.h5"

# Set to True to wipe LOCAL_ROOT before each run.
# IMPORTANT: re-running without cleaning causes duplicate SQLite entries,
# which inflates the reconstituted HDF5 (e.g. 3× the expected scan count).
# Keep True unless you intentionally want to append more granules.
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
    print("Local STARE-PODS Demo  (no AWS / no RDS)")
    print("=" * 60)
    print(f"Granule   : {os.path.basename(GRANULE_FILE)}")
    print(f"Datasets  : {DATASETS}")
    print(f"BBox      : {BBOX}  (lon_min, lat_min, lon_max, lat_max)")
    print(f"Local root: {LOCAL_ROOT}")
    print()

    # ── Clean up previous run ─────────────────────────────────────────────────
    if CLEAN_BEFORE_RUN and os.path.exists(LOCAL_ROOT):
        import shutil
        print(f"Cleaning up {LOCAL_ROOT} (CLEAN_BEFORE_RUN=True) ...")
        shutil.rmtree(LOCAL_ROOT)
        print()

    demo = LocalStarePodsDemo(local_root=LOCAL_ROOT)

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Ingest granules → local Parquet + SQLite")
    print("=" * 60)
    local_paths = demo.ingest_granules(GRANULE_FILE, instrument='GMI', level=STARE_LEVEL)
    print(f"GMI  : written {len(local_paths)} scan path(s).")
    ssmis_paths = demo.ingest_granules(SSMIS_GRANULE_FILE, instrument='SSMIS', level=STARE_LEVEL)
    print(f"SSMIS: written {len(ssmis_paths)} scan path(s).")
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

    intersecting = demo.find_intersecting_data(location_sids, instruments=['GMI'])
    print(f"Found {len(intersecting)} intersecting metadata row(s).")
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
    # TEMPORAL FEATURES (temporal-stare-pods issues 01–06)
    # ══════════════════════════════════════════════════════════════════════════
    from starepandas.io.granules import (
        load_local_temporal_catalog, load_local_vcf,
    )
    from starepandas.overlap import (
        rendezvous_events, overlap_matrix, overlap_pod_table, pair_drilldown,
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
    gmi = catalog[catalog['Dataset'].str.startswith('GMI')]
    gmi_start, gmi_end = gmi['t_start'].min(), gmi['t_end'].max()
    match_period = (gmi_start - pd.Timedelta(hours=1), gmi_end + pd.Timedelta(hours=1))
    miss_period = (gmi_start - pd.Timedelta(days=10), gmi_start - pd.Timedelta(days=9))
    hit = demo.find_intersecting_data(None, ['GMI'], period=match_period)
    miss = demo.find_intersecting_data(None, ['GMI'], period=miss_period)
    print(f"GMI pass window: [{gmi_start}, {gmi_end}]")
    print(f"  period bracketing the pass  -> {len(hit):3d} chunk(s)  {match_period}")
    print(f"  period 9–10 days earlier     -> {len(miss):3d} chunk(s)  {miss_period}")
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
    dt = pd.Timedelta(minutes=15)
    events = rendezvous_events(catalog, dt)
    npods = events['podcode'].nunique() if not events.empty else 0
    print(f"Rendezvous over the ingested GMI+SSMIS catalog (Δt={dt}): "
          f"{len(events)} event(s) across {npods} shared pod(s).")
    print("  (This co-located pair's ground tracks cross within ~3 min in the")
    print("   pods below — a genuine spatial + temporal intersection.)")
    print()
    print("Instrument×instrument matrix — pods where A & B rendezvous (slide 8):")
    print(overlap_matrix(events).to_string())
    print("\nPer-pod n-way combination counts — cell (pod, n) = distinct")
    print("n-instrument combos rendezvousing in that pod (slide 9):")
    print(overlap_pod_table(events).head(10).to_string())
    print("\nGMI–SSMIS pair drill-down (first 8 pods + times):")
    print(pair_drilldown(events, 'GMI', 'SSMIS').head(8).to_string(index=False))
    print()

    print("Done.")


if __name__ == "__main__":
    main()
