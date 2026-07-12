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
│   ├── client.py           # C-6 client SDK — ingest_granules() -> JobHandle
│   │                       #   (urllib POST, >4MB list → granule_uris_s3
│   │                       #   escape hatch, IngestError/JobNotFound)
│   ├── job_handle.py       # C-6 JobHandle.status/wait/failures/cancel
│   ├── _http.py            # C-6 stdlib-urllib request helper + error types
│   └── config.py           # C-6 get_cloud_config() — reads endpoint/api_key
│                           #   from .config (or STAREPANDAS_CLOUD_* env)
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
│                           #  test_granule_timestamps, test_ingest_module,
│                           #  test_podcode_layout, test_temporal_catalog —
│                           #  issue 01 local seam; test_temporal_query —
│                           #  issue 03 period filter)
└── examples/               # Example notebooks and scripts
```

### Quaternary pod-code layout (post 2026-06-14)

Chunk storage uses **quaternary pod codes** — **flat in S3, hierarchical on
local disk** — with self-describing filenames. This replaced the earlier
"unified HTM-tree" layout (task-12, 2026-05-25); S3 and local now **diverge**.
See `docs/quaternary_storage_plan.md` (local/uncommitted) for the full design.

A **pod code** is a compact, dynamic-length base-4 string for a trixel:
`q` + octant(0-7) + one quaternary digit(0-3) per level. Its length follows the
trixel's actual STARE level (level-2 → `q132`; level-4 → `q13211`). It encodes
the same address the old `Q00_1/Q01_3/Q02_2/Q03_1/Q04_1` chain did.

```
LOCAL (hierarchical — cumulative pod-code dir tree, self-describing leaf):
  <root>/q13/q132/q1321/q13211/q13211-<granule_basename>-<dataset>.parquet

S3 (FLAT — every chunk directly under the storage prefix; pod code IS the key prefix):
  s3://zarrpods/storage/q13211-<granule_basename>-<dataset>.parquet
                       └─ default_s3_prefix (.config field) ─┘
```

**Filename grammar** (`<podcode>-<granule>-<dataset>.parquet`): pod code = before
the first `-`; dataset = after the last `-` (datasets use `_`, never `-`); granule
basename = the middle (may itself contain `-`). The flat S3 key's pod-code prefix
doubles as a native spatial query — `list_objects_v2(Prefix="storage/q13")`
returns the `q13` subtree, no tree walk.

**Codec** (`starepandas/staredataframe.py`): `sid_to_podcode` / `podcode_to_sid` /
`podcode_to_local_dirs` / `chunk_filename` / `parse_chunk_filename`. The writers
(`STAREDataFrame.to_s3` flat, `to_local` hier) and readers
(`reconstitute_hdf5_from_*`, which are metadata-driven via the stored
`group_path` and fall back to a pod-code prefix list / dir walk) all flow through
it. Old `Q00_*/…` data is **kept as-is** and not read by the new path (migration
is a deferred TODO in the plan §8).

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

### Verified checks (STARE-PODS, 12/12 PASS as of 2026-07-11 — fully online, live-RDS checks included)
1. `import starepandas`
2. `MAX_PARTITION_LEVEL == 4` (locks in the post-ba3028d level-4 partitioning)
3. `to_local` writes Parquet leaves (no zarr artifacts)
4. Parquet partition carries every column + `__row_positions__` + kv-metadata
5. `to_local` catalogs temporal range + pod code (SQLite `t_start`/`t_end`/
   `podcode` per chunk; re-ingest with same `raw_collected_time` stays
   idempotent — temporal-stare-pods issue 01)
6. Period filter + thin temporal-catalog load (issue 03 —
   `load_local_metadata(period=…)` includes/excludes by `[t_start, t_end]`
   overlap; `load_local_temporal_catalog` projects exactly
   `podcode`/`Dataset`/`t_start`/`t_end`)
7. `reconstitute_hdf5_from_local` round-trips through Parquet
8. `reconstitute_hdf5_from_s3` (local path) walks pod-code tree
9. `local_starepods_examples.py` end-to-end against the real GMI granule
10. `pods_unique` UNIQUE constraint exists on `PodsMetadata` (§C10 #1 gate)
    + `t_start`/`t_end`/`podcode` columns present on the live catalog
11. `PodsMetadata` insert is idempotent (§C10 #1 live regression —
    double-insert keeps row count stable, DO UPDATE refreshes MetadataJson)
12. C-1..C-6 unit tests pass (cloud.ticket_sizing + metadata + granule_timestamps
    + s3_layout + ingest_module + config_env_secret + control_plane_lambdas
    + completion_watcher + cloud_client + podcode_layout + temporal_catalog
    + temporal_query — currently 161 unit tests)

### Verified checks (cloud SDK, env-gated — C-6)
`~/.claude/scripts/starepods_cloud_verify.py` (run with `STAREPANDAS_CLOUD_VERIFY=1`;
skips cleanly offline so CI never hits AWS):
1. `starepandas.cloud.get_cloud_config()` resolves `endpoint` + `api_key`
2. SDK reaches + authenticates against the live API — `GET /jobs/<bogus> → 404`

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

*Last Updated: 2026-07-11 (temporal-stare-pods issue 03 COMPLETE — temporal-aware
intersection. `load_s3_metadata` / `load_local_metadata` and both demo
`find_intersecting_data` methods accept `period=(start, end)` — a chunk matches
when `[t_start, t_end]` overlaps the period, ANDed with the spatial pod match;
no period → spatial-only behavior unchanged; null-range chunks never match.
Predicate built by the shared `_period_conditions` helper
(`starepandas/io/granules/__init__.py`) as the ADR-0002 Decision-3
index-friendly rewrite: `t_start BETWEEN period_start − D_MAX AND period_end`
+ residual `t_end ≥ period_start`, `D_MAX = timedelta(hours=2)` (documented
constant; live EXPLAIN confirms Bitmap Index Scan on `idx_pods_temporal`).
Granule-level `start_date`/`end_date` (filename-derived "RawData Collected
Time") documented as distinct from the data-level `period` everywhere. New
thin-projection loaders for the issue-05 analytics:
`load_s3_temporal_catalog` / `load_local_temporal_catalog` project exactly
`podcode`/`Dataset`/`t_start`/`t_end` (never `MetadataJson`), timestamps
parsed. Post-review: `_validate_period` normalizes tz-aware bounds to naive
UTC and rejects `None`/`NaT`/reversed periods, demos validate the period up
front (fail fast, not swallowed to empty), and `StarePodsDemo` now actually
forwards `start_date`/`end_date`/`time_range` (previously captured and
dropped). Tests: `tests/test_temporal_query.py` (26, local round-trip + pure
predicate seams). Live read-only smoke against the re-ingested catalog
(14,225 SSMIS rows) passed. Verification: basic 7/7, STARE-PODS 12/12 online
(new check 6).)*

*Prior: 2026-07-11 (temporal-stare-pods issue 02 COMPLETE — cloud
redeploy + demo re-ingest. Worker image rebuilt from temporal HEAD `c282315`
(wheel `0.6.8+74.gc282315.dirty` — the `.dirty` is uncommitted docs/notebook
edits only, zero source diff vs `c282315`) and repushed to ECR `starepods/worker:dev`
(`sha256:dc8bbab0…` → `sha256:8736fd06…`, 431 MB); no ECS action needed — fresh
Fargate tasks re-resolve the tag (all 4 confirmed on the new digest). Demo data
re-ingested: cloud job `e5d79f75-…` (10/10 SSMIS, 0 failed, 5m6s, 14,225 Parquet
objects via re-run `examples/cloud_ingest_demo.ipynb`) + host GMI re-ingest
(re-run `starepandas/s3_starepods_examples.ipynb`, 514 rows). First
temporal-enabled connect applied the issue-01 idempotent schema upgrade to live
RDS; catalog now has 0 of 14,739 rows missing `t_start`/`t_end`/`podcode`.
Spot-check: chunk `[t_start,t_end]` == parquet timestamp min/max, podcode
consistent (catalog == filename == `sid_to_podcode(grouped_id)`),
`__row_positions__` strictly increasing. Verification: basic 7/7, STARE-PODS
11/11 online (live-RDS checks 9–10 pass against the migrated catalog for the
first time). No code change — operational only. See
`docs/path_c_implementation.md` §2026-07-11 for the record.)*

*Prior: 2026-06-14 — Path C C-6 COMPLETE — client SDK. New
`starepandas/cloud/{client,job_handle,_http,config}.py` give a pure client-side
wrapper over the live REST API: `sp.cloud.ingest_granules(granule_uris,
instrument, …) -> JobHandle` (stdlib-`urllib` POST — no new dependency; >4 MiB
serialized list → `granule_uris_s3` S3 escape hatch; typed `IngestError`/
`JobNotFound`) + `JobHandle.status()/wait()/failures()/cancel()` (501 →
`NotImplementedError`). `endpoint`+`api_key` read via the existing `.config`
loader and added to `_RESERVED_CONFIG_KEYS` so they don't leak into s3fs kwargs;
`STAREPANDAS_CLOUD_{ENDPOINT,API_KEY}` env overrides. **Live smoke test** from
the host (job `3b089a62-…`): `POST /ingest → 202 running`; `wait()` polled to
`state=complete` in 137 s (`processed 1/1`); 514 Parquet objects written to
`s3://zarrpods/storage/…`; success-callback POST received at a webhook.site
receiver (closes C-5's never-exercised live success path). Verification: basic
7/7, STARE-PODS 10/10 (105 unit tests, +17 `test_cloud_client.py`), new
env-gated `starepods_cloud_verify.py` (2/2 live). C-7 (load test + tune +
runbook) is next. See `docs/path_c_implementation.md` §C-6 for the record.

Prior: C-5 COMPLETE (2026-06-07) — completion watcher. The
`starepods-completion-watcher` Lambda (`infra/cdk/lambdas/watcher/handler.py`),
fired every minute by EventBridge rule `starepods-completion-tick`
(`rate(1 min)` — classic-Rules floor; built in `_build_watcher`), closes drained
jobs hands-off: a conditional `running→complete/failed` flip (idempotency gate)
+ guarded `active_jobs -1` + scale-to-0 when the last job drains + `completed_at`
/`expires_at` stamping. Callback POST → 3× backoff → `starepods-callbacks-dlq`,
watched by a `starepods-callbacks-dlq-depth` alarm (custom metrics deferred
to Observability). Live DoD: `POST /ingest` ran the whole pipeline hands-off
(job `35293668-…`: `running→complete` one tick after `proc 1/1`, ECS `4→0`,
514 Parquet objects); synthetic bad-callback run put a payload on the DLQ.

Prior: C-4 COMPLETE — Part A "first light" (NAT topology replaced the
no-NAT/5-interface-endpoint VPC, net idle ~$73→~$32/mo; `STAREPANDAS_WORKER_SECRET`
env-var loader; worker image `sha256:bc32e21d…`) + Part B control plane
(`starepods-scheduler`/`starepods-status` Lambdas behind REST API `starepods-api`
+ tag-filtered `starepods-monthly` Budgets alarm).)*
*Generated by Claude for STAREPandas/pystare development*

---

## Agent skills

### Issue tracker

Issues and PRDs live as local markdown under `.scratch/<feature-slug>/` (no remote tracker). See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical roles, default strings (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.