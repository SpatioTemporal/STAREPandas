#!/usr/bin/env python3
"""
Demo: STARE-PODS pipeline on AWS S3 + RDS Postgres.

S3-counterpart of local_starepods_examples.py. Mirrors that script's
six-step flow but runs against real AWS S3 (Parquet partitions) and a
real RDS Postgres `PodsMetadata` table.

Workflow
--------
1. Ingest a GMI **and** an SSMIS granule → Parquet partitions on S3 + RDS
   metadata (clean_before_run wipes prior data for the same prefix before the
   first ingest; the second appends).
2. Find intersecting data for a bounding box via STARE SIDs + RDS.
3. Download intersecting Parquet partitions from S3.
4. Reconstitute an HDF5 file (both S1 and S2 scans).
5. Compare the reconstituted structure with the original granule.
6. Verify RDS metadata (counts per dataset under our s3_prefix).

Temporal features (temporal-stare-pods issues 01–06)
----------------------------------------------------
7.  Temporal catalog — every chunk carries ``[t_start, t_end]`` + podcode
8.  Period-filtered intersection — data-level ``[t_start, t_end]`` overlap
9.  VCF temporal roll-up — union range per pod, on the fly
10. Multi-instrument overlap analytics — the slide-8/9 rendezvous views

Note: the S3/RDS temporal loaders read the **shared** ``PodsMetadata``
catalog — every ingest in the RDS table, not only this demo's granule. That
is the production query surface, so the temporal counts below reflect the
whole catalog (filtered by instrument), unlike the local demo's fresh SQLite.

Requirements
------------
- starepandas/.config (next to this script) with AWS + RDS credentials.
- Sample granules. Default to the in-repo GMI + SSMIS granules; override with
  the ``STAREPODS_SAMPLE_GRANULE`` / ``STAREPODS_SAMPLE_GRANULE_SSMIS`` env vars.

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/s3_starepods_examples.py
"""

import os
import time
import h5py
import pandas as pd
from starepandas.demo_lib import StarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
# AWS + RDS credentials. Resolved relative to this file so it works from any cwd.
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".config")

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

# S3 root where Parquet partitions and RDS metadata for this demo live.
S3_PREFIX = "s3://zarrpods/gmi-demo-parquet"

# Bounding box filter — set to None to reconstitute the full granule
# (matching local_starepods_examples.py), or e.g. (115, -30, 120, -25)
# to restrict to SW Australia / Perth.
BBOX = None   # full granule, no spatial filter — mirrors the local demo

DATASETS = ["GMI_S1", "GMI_S2"]

OUTPUT_HDF5 = "/tmp/gmi_s3_reconstituted.h5"

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
        clean_before_run=CLEAN_BEFORE_RUN,   # wipes the prefix before GMI
    )
    # Second instrument appends — clean_before_run=False so it does NOT wipe
    # the GMI data just written to the same prefix.
    ssmis_paths = demo.ingest_granules(
        data_path=SSMIS_GRANULE_FILE,
        instrument="SSMIS",
        s3_prefix=S3_PREFIX,
        clean_before_run=False,
    )
    print(f"Ingest wall: {time.perf_counter() - t0:.2f} s")
    print(f"GMI  : stored {len(s3_paths)} dataset path(s)")
    print(f"SSMIS: stored {len(ssmis_paths)} dataset path(s)")
    print()

    # ── Step 2: Find intersecting data ────────────────────────────────────────
    print("=" * 60)
    print("Step 2: Find intersecting data via STARE SIDs")
    print("=" * 60)
    if BBOX is not None:
        location_sids = demo.get_sids_for_bbox(*BBOX, level=10)
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
    # s3_prefix scope: with CLEAN_BEFORE_RUN=True the bucket only holds this
    # granule's data, so passing the broad S3_PREFIX is correct and avoids
    # the layout mismatch the old per-granule S3 prefix would create.
    recon_path = demo.reconstitute_hdf5(
        dataset=DATASETS,
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        s3_prefix=S3_PREFIX,
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
    # TEMPORAL FEATURES (temporal-stare-pods issues 01–06)
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
    )

    # ── Step 7: Temporal catalog columns ──────────────────────────────────────
    print("=" * 60)
    print("Step 7: Temporal catalog — each chunk carries [t_start, t_end] + podcode")
    print("=" * 60)
    catalog = pd.concat(
        [load_s3_temporal_catalog(dataset_prefix='GMI'),
         load_s3_temporal_catalog(dataset_prefix='SSMIS')],
        ignore_index=True,
    )
    print(f"Thin catalog (GMI+SSMIS, catalog-wide): {len(catalog)} chunk(s) across "
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
    print("Step 8: Period-filtered load (data-level [t_start,t_end] overlap)")
    print("=" * 60)
    gmi = catalog[catalog['Dataset'].str.startswith('GMI')]
    gmi_start, gmi_end = gmi['t_start'].min(), gmi['t_end'].max()
    match_period = (gmi_start - pd.Timedelta(hours=1), gmi_end + pd.Timedelta(hours=1))
    miss_period = (gmi_start - pd.Timedelta(days=10), gmi_start - pd.Timedelta(days=9))
    hit = load_s3_metadata(dataset_prefix='GMI', period=match_period)
    miss = load_s3_metadata(dataset_prefix='GMI', period=miss_period)
    print(f"GMI catalog window: [{gmi_start}, {gmi_end}]")
    print(f"  period bracketing the window -> {len(hit):3d} chunk(s)")
    print(f"  period 9–10 days earlier      -> {len(miss):3d} chunk(s)")
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
    print("Step 10: Multi-instrument overlap analytics (slides 8/9)")
    print("=" * 60)
    # The catalog-wide read (step 7) mixes every ingest in the shared RDS table.
    # Scope the sweep to the co-located pair's time window so the result is the
    # pair's genuine rendezvous, reproducibly (period pushes into SQL).
    pair_period = (pd.Timestamp('2025-01-01 11:00'), pd.Timestamp('2025-01-01 13:30'))
    cat_pair = pd.concat(
        [load_s3_temporal_catalog(dataset_prefix='GMI', period=pair_period),
         load_s3_temporal_catalog(dataset_prefix='SSMIS', period=pair_period)],
        ignore_index=True,
    )
    dt = pd.Timedelta(minutes=15)
    events = rendezvous_events(cat_pair, dt)
    npods = events['podcode'].nunique() if not events.empty else 0
    print(f"Rendezvous over the co-located GMI+SSMIS pair (Δt={dt}): "
          f"{len(events)} event(s) across {npods} shared pod(s).")
    print("  (Ground tracks cross within ~3 min — a spatial + temporal intersection.)")
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
