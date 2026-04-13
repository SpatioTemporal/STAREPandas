#!/usr/bin/env python3
"""
Demo: Local STARE-PODS pipeline — no AWS or RDS required.

This script mirrors demo_reconstruct_hdf5_from_s3.py but uses the local
filesystem for zarr storage and SQLite for metadata.  No cloud credentials
are needed.

Workflow
--------
1. Ingest a GMI granule → zarr groups on local disk + SQLite metadata
2. Find intersecting data for a bounding box via STARE SIDs + SQLite
3. Load intersecting zarr chunks from disk
4. Reconstruct an HDF5 file (both S1 and S2 scans)
5. Compare the reconstructed structure with the original granule

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/local_version_starepods_demo.py
"""

import os
import h5py
from starepandas.demo import LocalStarePodsDemo

# ── Configuration ─────────────────────────────────────────────────────────────
LOCAL_ROOT = "/tmp/stare_pods_local"   # zarr store + SQLite DB live here

GRANULE_FILE = (
    "/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/"
    "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5"
)

# Bounding box filter — set to None to reconstruct the full granule,
# or e.g. (115, -30, 120, -25) to restrict to SW Australia / Perth.
BBOX = None   # (lon_min, lat_min, lon_max, lat_max) or None

DATASETS = ["GMI_S1", "GMI_S2"]

OUTPUT_HDF5 = "/tmp/gmi_local_reconstructed.h5"
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

    demo = LocalStarePodsDemo(local_root=LOCAL_ROOT)

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    print("=" * 60)
    print("Step 1: Ingest granule → local zarr + SQLite")
    print("=" * 60)
    local_paths = demo.ingest_granules(GRANULE_FILE, instrument='GMI', level=10)
    print(f"Written {len(local_paths)} scan path(s).")
    print()

    # ── Step 2: Find intersecting data ────────────────────────────────────────
    print("=" * 60)
    print("Step 2: Find intersecting data via STARE SIDs")
    print("=" * 60)
    if BBOX is not None:
        location_sids = demo.get_sids_for_bbox(*BBOX, level=10)
        print(f"Generated {len(location_sids)} SIDs for bbox {BBOX}")
    else:
        location_sids = None
        print("No bbox filter — all groups will be loaded (full granule reconstruction)")

    intersecting = demo.find_intersecting_data(location_sids, instruments=['GMI'])
    print(f"Found {len(intersecting)} intersecting metadata row(s).")
    if not intersecting.empty:
        print(intersecting[['Dataset', 'grouped_id', 'group_path']].to_string(index=False))
    print()

    # ── Step 3: Load intersecting chunks ─────────────────────────────────────
    print("=" * 60)
    print("Step 3: Load intersecting zarr chunks from disk")
    print("=" * 60)
    if not intersecting.empty:
        data_dict = demo.download_and_analyze(intersecting, instruments=list(intersecting['Dataset'].unique()))
        for ds_name, sdf in data_dict.items():
            print(f"  {ds_name}: {len(sdf)} rows, columns: {list(sdf.columns[:6])} …")
    else:
        print("  (No intersecting chunks found — skipping download step)")
        data_dict = {}
    print()

    # ── Step 4: Reconstruct HDF5 ──────────────────────────────────────────────
    print("=" * 60)
    print("Step 4: Reconstruct HDF5 (S1 + S2)")
    print("=" * 60)

    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]
    granule_local_prefix = os.path.join(LOCAL_ROOT, granule_basename)

    recon_path = demo.reconstruct_hdf5(
        dataset=DATASETS,
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        local_prefix=granule_local_prefix,
    )
    print(f"Written to: {recon_path}")
    print()

    # ── Step 5: Structure comparison ──────────────────────────────────────────
    print("=" * 60)
    print("Step 5: Structure comparison")
    print("=" * 60)
    dump_structure(recon_path, f"RECONSTRUCTED  ({os.path.basename(recon_path)})")
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
        print(f"  {dataset_name}: {cnt} group(s)")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
