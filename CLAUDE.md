# Claude's Analysis Documentation

This file captures Claude's understanding of the STAREPandas and pystare codebase structure, API functions, and development insights.

## 📁 Project Overview

### STAREPandas
**Location**: `/Users/tonhai/workspace/Bayesics/StarePandas_par/STAREPandas`
**Purpose**: STARE spatial indexing extensions for pandas DataFrames
**Language**: Python 3.12+

### pystare  
**Location**: `/Users/tonhai/workspace/Bayesics/pystare`
**Purpose**: Core STARE (SpatioTemporal Adaptive Resolution Encoding) library
**Language**: Python with C extensions

---

## 🏗️ STAREPandas Package Structure

```
starepandas/
├── __init__.py              # Main package imports + Path-C top-level re-exports
├── staredataframe.py         # Core STAREDataFrame class + to_s3 / to_local
├── demo_lib.py              # Demo classes (StarePodsDemo, LocalStarePodsDemo)
│                            # → thin shims over starepandas/ingest.py
├── ingest.py                # Path C C-1 — module-level ingest functions
│                            # (ingest_granules_s3, ingest_granules_local,
│                            #  clean_s3_prefix). Imports the cloud worker
│                            # uses directly without the demo classes.
├── metadata.py              # Path C C-1 — MetadataStore protocol +
│                            # RDSMetadataStore adapter (§C9 M4 hedge).
│                            # Single point that all PodsMetadata I/O
│                            # flows through; future DynamoDB swap is local
│                            # to one adapter implementation.
├── cloud/                   # Path C cloud-service package (C-2 onward)
│   ├── __init__.py
│   ├── ticket_sizing.py    # §C2 pure functions (split_into_tickets)
│   ├── worker.py           # C-2 SQS-driven worker entrypoint
│   │                       # (python -m starepandas.cloud.worker)
│   │                       # — Worker, WorkerConfig, §C10 #3 idempotent
│   │                       #   counter, Decision-9 graceful exit on
│   │                       #   RDS credential rotation
│   └── config.py           # placeholder for cloud client config (C-6)
├── io/
│   ├── granules/
│   │   ├── __init__.py     # Granule factory, to_s3, to_local, reconstitute
│   │   ├── _timestamps.py  # NEW (§C10 #2) — derive raw_collected_time
│   │   │                   # from filename (GMI/SSMIS/ATMS/AMSR2/MODIS)
│   │   ├── gmi.py          # GMI instrument reader
│   │   ├── amsr2.py        # AMSR2 instrument reader
│   │   ├── ssmis.py        # SSMIS instrument reader
│   │   ├── atms.py         # ATMS instrument reader (updated for 2025 format)
│   │   └── utils.py        # Granule utilities
│   └── s3.py               # S3 Parquet storage functions
├── tools/
│   ├── __init__.py
│   ├── stare_join.py       # STARE-based spatial joins
│   ├── intersections.py    # STARE intersection operations
│   └── ...                 # Other spatial tools
├── tests/                  # pytest test suite (+test_metadata_store,
│                           #  test_cloud_ticket_sizing, test_s3_layout,
│                           #  test_granule_timestamps, test_ingest_module)
└── examples/               # Example notebooks and scripts
```

### Unified Parquet layout (post task-12, 2026-05-25)

Both `to_local` and `to_s3` now produce the same layout — HTM tree first,
then granule basename, then dataset leaf. Spatial queries walk **one** tree
regardless of provenance; multiple granules in the same trixel coexist as
sibling subdirectories.

```
LOCAL: <root>/Q00_X/Q01_Y/.../QN_M/<granule_basename>/<dataset>.parquet
S3:    s3://zarrpods/storage/Q00_X/Q01_Y/.../QN_M/<granule_basename>/<dataset>.parquet
                            └─ default_s3_prefix (new .config field) ─┘
```

### Key Components

#### STAREDataFrame (`staredataframe.py`)
- **Core Class**: Extends pandas DataFrame with STARE capabilities
- **Key Methods**:
  - `set_sids()`: Set STARE indices for spatial operations
  - `make_trixels()`: Convert SIDs to geometric trixels
  - STARE-based spatial queries and intersections

#### Granule Readers (`io/granules/`)
- **Factory Pattern**: `granule_factory()` auto-detects instrument type
- **Supported Instruments**:
  - **GMI**: GPM Microwave Imager (13 channels)
  - **AMSR2**: Advanced Microwave Scanning Radiometer 2 (12 channels) 
  - **SSMIS**: Special Sensor Microwave Imager/Sounder (24 channels)
  - **ATMS**: Advanced Technology Microwave Sounder (updated 2025 format)

#### STARE-PODS Demo (`demo_lib.py`)
- **High-level API**: Complete workflow demonstration. As of task 7
  (2026-05-25) the ingest + clean methods are thin shims over the
  module-level callables in `starepandas/ingest.py` — keeps existing
  notebooks working while letting the cloud worker (C-2) import the
  functions directly.
- **Key Methods**:
  - `get_sids_for_bbox()`: Convert bounding box to STARE SIDs
  - `ingest_granules()` (shim → `starepandas.ingest.ingest_granules_s3`)
  - `clean_s3_prefix()` (shim → `starepandas.ingest.clean_s3_prefix`)
  - `find_intersecting_data()`: Find intersecting data across instruments
  - `download_and_analyze()`: Selective chunk download and analysis
  - `reconstitute_hdf5()`: Build HDF5 from intersecting partitions
  - `plot_comparison()`: Multi-instrument visualization

#### Top-level convenience imports (`starepandas/__init__.py`)
The cloud worker (and any script that doesn't need the demo classes)
imports these directly:
```python
import starepandas
starepandas.ingest_granules_s3(...)     # → ingest.ingest_granules_s3
starepandas.ingest_granules_local(...)  # → ingest.ingest_granules_local
starepandas.clean_s3_prefix(...)        # → ingest.clean_s3_prefix
```

---

## 🔧 pystare Library Structure

```
pystare/
├── __init__.py              # Package initialization
├── spatial.py                # Core spatial functions
├── temporal.py               # Temporal indexing functions  
├── core.py                  # Low-level C extension interface
├── exceptions.py             # Custom exception classes
└── tests/                    # Test suite
```

### Available Functions (from spatial.py)

#### **Spatial Conversion Functions**
```python
# Point/Coordinate Conversion
pystare.from_latlon(lat, lon, level)           # Convert lat/lon arrays to SIDs
pystare.from_lonlat(lon, lat, level)           # Same as above (reversed args)
pystare.from_latlon_2d(lat, lon, level, ...)     # 2D array conversion

# Polygon/Cover Functions  
pystare.cover_from_hull(lat, lon, level)          # Convert hull vertices to STARE SIDs ✅
pystare.cover_from_ring(lat, lon, level)          # Convert ring vertices to STARE SIDs
pystare.latlon2circular_cover(lat, lon, radius, level)  # Circular area cover

# Reverse Conversion
pystare.to_latlon(sids)                          # Convert SIDs back to lat/lon
pystare.to_latlonlevel(sids)                      # Convert SIDs to lat/lon/level
pystare.to_vertices_latlon(sids)                  # Get trixel vertices

# Spatial Operations
pystare.intersects(cover, sids, method)          # Test intersection between cover and SIDs
pystare.intersection(sids1, sids2, ...)           # Get intersection of two SID sets
pystare.cmp_spatial(sids1, sids2, ...)          # N-way containment test

# Resolution/Level Functions
pystare.spatial_resolution(sids)                   # Get STARE level of SIDs
pystare.spatial_increment_from_level(level)           # Level to increment conversion
pystare.spatial_scale_km(level)                    # Rough km scale at level
```

### **Important Notes**
- ❌ **No `sid_from_polygon()` function** - This doesn't exist
- ✅ **Use `cover_from_hull()`** instead for polygon to SID conversion
- **Function Location**: All spatial functions are in `pystare.spatial` module

---

## 🔗 STARE-PODS Workflow Integration

### Complete Pipeline
```python
# 1. Location Definition
demo = StarePodsDemo()
location_sids = demo.get_sids_for_bbox(-125, 32, -115, 42)

# 2. Granule Ingestion  
for instrument in ['GMI', 'AMSR2', 'SSMIS', 'ATMS']:
    s3_paths = demo.ingest_granules(data_path, instrument, s3_prefix)

# 3. Spatial Intersection
intersecting_metadata = demo.find_intersecting_data(location_sids, instruments)

# 4. Selective Download
data_dict = demo.download_and_analyze(intersecting_metadata, instruments)

# 5. Multi-Instrument Analysis
demo.plot_comparison(data_dict, "California Coast", ['Tc1', 'Tc2', 'Tc3', 'Tc4'])
```

### Key Benefits
- **80-95% Data Reduction**: Only download intersecting chunks
- **Multi-Instrument Support**: GMI, AMSR2, SSMIS, ATMS
- **Scalable Architecture**: S3 + RDS + STARE indexing
- **Production Ready**: Complete workflow from granules to analysis

---

## 🚨 Development Insights

### Common Issues & Solutions

#### 1. **Function Availability**
```python
# ❌ Wrong (doesn't exist)
pystare.sid_from_polygon(polygon, level)

# ✅ Correct  
pystare.cover_from_hull(lat_coords, lon_coords, level)
```

#### 2. **Bounding Box Conversion**
```python
def bbox_to_hull_sids(lon_min, lat_min, lon_max, lat_max, level=10):
    # Convert bbox to counter-clockwise hull vertices
    # IMPORTANT: Must use bottom-left, bottom-right, top-right, top-left order
    # This creates a proper non-degenerate polygon
    lats = [lat_min, lat_min, lat_max, lat_max]  # [32, 32, 42, 42]
    lons = [lon_min, lon_max, lon_max, lon_min]   # [-125, -115, -115, -125]
    
    # Convert to STARE SIDs
    sids = pystare.cover_from_hull(lats, lons, level)
    return sids.tolist()
```

#### 3. **ATMS 2025 Format Updates**
```python
# Updated ATMS reader supports:
# - S1: Single channel temperature data
# - S2: Multiple channels (Tc1-Tc6)  
# - Improved timestamp handling
# - Better error handling
```

#### 4. **Development Testing**
- **Issue**: Numpy installation conflicts cause import errors
- **Solution**: Use development mode installation (`pip install -e .`)
- **Alternative**: Module reloading for rapid testing

---

## ✅ Verification Skills (MANDATORY)

All work in this project must follow this protocol:

### Before starting any implementation or review
Run the appropriate verification skill first to establish a baseline. Any failures must be understood before proceeding.

### After implementing or modifying any function
Re-run the relevant verification skill to confirm nothing is broken. If you added or changed functionality covered by the scripts, **update the verification script** to reflect the new behavior.

### Skills

| Skill | Command | Scope |
|---|---|---|
| Reinstall only | `/stare-pandas-reinstall` | Reinstalls starepandas in `starepandas_3.12_v3` and checks pip log |
| Basic verification | `/basic-verification-stare-pandas` | Reinstall + core STAREPandas API (import, STAREDataFrame, stare_join, sids_from_xy) |
| STARE-PODS verification | `/stare-pods-verification` | Reinstall + full S3/RDS pipeline (config, S3, Parquet write/read, metadata, intersections) |

All skills run exclusively inside the `starepandas_3.12_v3` conda environment.

### Verification scripts (permanent, do not rewrite)
- `~/.claude/scripts/starepandas_verify.py` — basic verification (6 checks)
- `~/.claude/scripts/starepods_verify.py` — STARE-PODS pipeline verification (10 checks)

### When to update the verification scripts
- New public function added → add a callable check and a functional test
- Function signature changed → update the corresponding test
- New S3/RDS behavior → add or update the pipeline test
- Bug fixed → add a regression test asserting the fix
- New pytest file under `tests/` → add it to the unit-test check's `test_files` list

### Verified checks (basic, 7/7 PASS as of 2026-05-30)
1. `import starepandas` (core package loads + headline symbols present)
2. `import pystare` (dependency, with `from_latlon` / `to_latlon`)
3. `STAREDataFrame` instantiable + `set_sids` + `make_trixels`
4. `sids_from_xy` → `to_latlon` round-trips within tolerance
5. `stare_join` on synthetic STAREDataFrames returns matches
6. Task-7 ingest module + task-1 cloud package reachable at top level
7. C-2 `cloud.worker` exposes `Worker` / `WorkerConfig` / `main` / `_is_rds_auth_error`; `WorkerConfig.from_env()` rejects missing `SQS_QUEUE_URL`

### Verified checks (STARE-PODS, 10/10 PASS as of 2026-06-07)
1. `import starepandas`
2. `MAX_PARTITION_LEVEL == 4` (locks in the post-ba3028d level-4 partitioning)
3. `to_local` writes Parquet leaves (no zarr artifacts)
4. Parquet partition carries every column + `__row_positions__` + kv-metadata
5. `reconstitute_hdf5_from_local` round-trips through Parquet
6. `reconstitute_hdf5_from_s3` (local path) walks HTM tree
7. `local_starepods_examples.py` end-to-end against the real GMI granule
8. `pods_unique` UNIQUE constraint exists on `PodsMetadata` (§C10 #1 gate)
9. `PodsMetadata` insert is idempotent (§C10 #1 live regression —
   double-insert keeps row count stable, DO UPDATE refreshes MetadataJson)
10. C-1..C-5 unit tests pass (cloud.ticket_sizing + metadata + granule_timestamps
    + s3_layout + ingest_module + config_env_secret + control_plane_lambdas
    + completion_watcher — currently 88 unit tests)

---

## 📊 Available Test Files

### STAREPandas Tests
- `tests/test_instantiation.py` - Basic STAREDataFrame creation
- `tests/test_parquet_io.py` - Parquet storage operations
- `test_atms_implementation.py` - ATMS instrument testing
- `starepods_complete_demo.ipynb` - Complete demonstration notebook

### pystare Tests
- `tests/test_spatial.py` - Spatial function validation
- `tests/test_intersections.py` - Intersection operations
- `test_bbox_fix.py` - Bounding box conversion validation

---

## 🛠️ Quick Reference

### Import Patterns
```python
# For development without full installation
import sys
sys.path.insert(0, '/path/to/STAREPandas')
import starepandas

# Direct pystare access
import sys
sys.path.append('/path/to/pystare')
import pystare.spatial
```

### Environment Setup
```bash
# Development mode (recommended)
cd /path/to/STAREPandas
pip install -e .

# Alternative: Module reloading
%load_ext autoreload
%autoreload 2
```

### Configuration Files
- **AWS**: `starepandas/.config` - S3 and RDS credentials
- **Package**: `setup.cfg` - Dependencies and metadata
- **Testing**: `pytest.ini` - Test configuration

---

*Last Updated: 2026-06-07 (Path C C-5 COMPLETE — completion watcher. The
`starepods-completion-watcher` Lambda (`infra/cdk/lambdas/watcher/handler.py`),
fired every minute by EventBridge rule `starepods-completion-tick`
(`rate(1 min)` — classic-Rules floor; built in `_build_watcher`), closes drained
jobs hands-off: a conditional `running→complete/failed` flip (idempotency gate)
+ guarded `active_jobs -1` + scale-to-0 when the last job drains + `completed_at`
/`expires_at` stamping. Callback POST → 3× backoff → `starepods-callbacks-dlq`,
watched by a new `starepods-callbacks-dlq-depth` alarm (custom metrics deferred
to Observability). Live DoD: `POST /ingest` ran the whole pipeline hands-off
(job `35293668-…`: `running→complete` one tick after `proc 1/1`, ECS `4→0`,
514 Parquet objects); synthetic bad-callback run put a payload on the DLQ. All
new resources `Project=starepods`-tagged (idle cost ≈ $0). Verification: basic
7/7, STARE-PODS 10/10 (88 unit tests, +13 `test_completion_watcher.py`). C-6
(client SDK) is next. See `docs/path_c_implementation.md` §C-5 for the record.

Prior: C-4 COMPLETE — Part A "first light" (NAT topology replaced the
no-NAT/5-interface-endpoint VPC, net idle ~$73→~$32/mo; `STAREPANDAS_WORKER_SECRET`
env-var loader; worker image `sha256:bc32e21d…`) + Part B control plane
(`starepods-scheduler`/`starepods-status` Lambdas behind REST API `starepods-api`
+ tag-filtered `starepods-monthly` Budgets alarm).)*
*Generated by Claude for STAREPandas/pystare development*