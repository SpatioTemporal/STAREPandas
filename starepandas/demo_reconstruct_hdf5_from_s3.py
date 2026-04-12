#!/usr/bin/env python3
"""
Demo: Reconstruct HDF5 from S3 zarr chunks.

Reads zarr groups for a specific GMI granule from S3, filters to a geographic
bbox, and writes a valid HDF5 file whose structure matches the original granule.

Usage
-----
    conda run -n starepandas_3.12_v3 python starepandas/demo_reconstruct_hdf5_from_s3.py

Requirements
------------
- ~/.config or starepandas/.config with AWS + RDS credentials
- Zarr data previously ingested for the target granule via ingest_granules()
"""

import os
import h5py
from starepandas.demo import StarePodsDemo

# ── Configuration ────────────────────────────────────────────────────────────
CONFIG_PATH = "/Users/tonhai/workspace/Bayesics/StarePandas_par/stare_demo/starepandas/.config"

# Granule that was ingested into S3
GRANULE_FILE = (
    "/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/"
    "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5"
)

# S3 root where zarr data lives
S3_PREFIX = "s3://zarrpods/gmi-demo"

# Dataset / scan(s) to reconstruct — list writes all into the same HDF5 file
DATASET = ["GMI_S1", "GMI_S2"]

# Bounding box (lon_min, lat_min, lon_max, lat_max) within the granule's coverage.
# This granule (S034347–E051659 UTC) passes over SW Australia.
BBOX = (115, -30, 120, -25)   # SW Australia / Perth area

# Output HDF5 path
OUTPUT_HDF5 = "/tmp/gmi_s1_reconstructed.h5"
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
    print("HDF5 Reconstruction from S3 zarr Demo")
    print("=" * 60)
    print(f"Granule : {os.path.basename(GRANULE_FILE)}")
    print(f"Datasets: {DATASET}")
    print(f"BBox    : {BBOX}  (lon_min, lat_min, lon_max, lat_max)")
    print(f"S3 root : {S3_PREFIX}")
    print()

    # Derive the granule-specific S3 prefix so the reconstruction reads only
    # zarr groups belonging to this granule (not older ingestions in S3).
    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]
    granule_s3_prefix = f"{S3_PREFIX}/{granule_basename}"
    print(f"Granule S3 prefix: {granule_s3_prefix}")
    print()

    # Initialise demo with explicit config path
    demo = StarePodsDemo(aws_config_path=CONFIG_PATH)

    # ── Reconstruct ───────────────────────────────────────────────────────────
    datasets_str = ", ".join(DATASET) if isinstance(DATASET, list) else DATASET
    print(f"Reconstructing {datasets_str} over bbox {BBOX} ...")
    recon_path = demo.reconstruct_hdf5(
        dataset=DATASET,
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        s3_prefix=granule_s3_prefix,
    )
    print(f"Written to: {recon_path}")

    # ── Structure comparison ──────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("Structure comparison")
    print("=" * 60)
    dump_structure(recon_path, f"RECONSTRUCTED  ({os.path.basename(recon_path)})")
    dump_structure(GRANULE_FILE, f"ORIGINAL       ({os.path.basename(GRANULE_FILE)})")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
