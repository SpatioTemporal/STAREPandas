#!/usr/bin/env python3
"""
Demo: STARE-PODS pipeline on AWS S3 + RDS Postgres.

S3-counterpart of local_starepods_examples.py. Mirrors that script's
six-step flow but runs against real AWS S3 (Parquet partitions) and a
real RDS Postgres `PodsMetadata` table.

Workflow
--------
1. Ingest a GMI granule → Parquet partitions on S3 + RDS metadata
   (with optional clean_before_run to wipe prior data for the same prefix).
2. Find intersecting data for a bounding box via STARE SIDs + RDS.
3. Download intersecting Parquet partitions from S3.
4. Reconstitute an HDF5 file (both S1 and S2 scans).
5. Compare the reconstituted structure with the original granule.
6. Verify RDS metadata (counts per dataset under our s3_prefix).

Requirements
------------
- starepandas/.config (next to this script) with AWS + RDS credentials.
- The granule file at ``GRANULE_FILE`` available locally.

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/s3_starepods_examples.py
"""

import os
import time
import h5py
from starepandas.demo_lib import StarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
# AWS + RDS credentials. Resolved relative to this file so it works from any cwd.
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".config")

GRANULE_FILE = (
    "/Users/thatdaihaiton/Workspace/STARE/L1C_Data_Samples/GPM/2025/Jan_1_2/"
    "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5"
)

# S3 root where Parquet partitions and RDS metadata for this demo live.
S3_PREFIX = "s3://zarrpods/gmi-demo-parquet"

# Bounding box filter — set to None to reconstitute the full granule,
# or e.g. (115, -30, 120, -25) to restrict to SW Australia / Perth.
BBOX = (115, -30, 120, -25)

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
    # as of task 12 (2026-05-25) the S3 layout puts <granule_basename>
    # INSIDE the HTM tree, not at the top:
    #
    #   <S3_PREFIX>/Q00_X/Q01_Y/.../QN_M/<granule_basename>/<dataset>.parquet
    #
    # So the old "granule_s3_prefix = S3_PREFIX + '/' + basename" scoping
    # would no longer match any rows. We now scope by substring match on
    # the basename (which is unique within the bucket).
    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]
    granule_path_marker = f"/{granule_basename}/"   # matches the HTM-buried segment

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Ingest granule → S3 Parquet + RDS")
    print("=" * 60)
    t0 = time.perf_counter()
    s3_paths = demo.ingest_granules(
        data_path=GRANULE_FILE,
        instrument="GMI",
        s3_prefix=S3_PREFIX,
        clean_before_run=CLEAN_BEFORE_RUN,
    )
    print(f"Ingest wall: {time.perf_counter() - t0:.2f} s")
    print(f"Stored {len(s3_paths)} dataset path(s):")
    for p in s3_paths:
        print(f"  {p}")
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
        # result. Substring match handles the task-12 layout where the
        # granule basename sits inside the HTM tree (not at the top).
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
    # the task-12 layout mismatch the old granule_s3_prefix would create.
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
            # Task-12 layout: the basename sits inside the HTM tree, so
            # use a LIKE substring match instead of a startswith prefix.
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
    print("Done.")


if __name__ == "__main__":
    main()
