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
├── demo_plots.py            # figures for the example demos (steps 11-12):
│                            # plot_pod_coverage (per-instrument pod map),
│                            # plot_rendezvous (swaths + pass-time panels),
│                            # pod_pixels (backend-agnostic chunk loader —
│                            # the pod code prefixes the chunk filename in
│                            # both the local and the flat S3 layout),
│                            # widest_rendezvous. Returns Figures; callers
│                            # savefig (scripts) or display (notebooks).
├── overlap.py               # temporal issue 05 — slide-8/9 overlap
│                            # analytics: rendezvous_events (Helly sweep
│                            # kernel) + overlap_matrix / overlap_pod_table
│                            # / pair_drilldown / pod_drilldown +
│                            # fold_instrument. Pure functions over a
│                            # loaded temporal-catalog frame (ADR-0001/2).
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
│   │   ├── atms.py         # ATMS instrument reader (2025 format; ingest-safe
│   │   │                   # since 2026-08-04 — read_timestamps no longer
│   │   │                   # touches self.lat before read_latlon, and
│   │   │                   # read_data reads self.dataset not self.netcdf)
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
│                           #  issue 03 period filter; test_vcf_rollup —
│                           #  issue 04 VCF roll-up; test_overlap_analytics —
│                           #  issue 05 pure seam; test_atms_reader —
│                           #  ATMS ingest regressions; _temporal_fixtures.py =
│                           #  shared local-seam helpers)
└── examples/               # Example notebooks and scripts
```

### Quaternary pod-code layout (post 2026-06-14)

Chunk storage uses **quaternary pod codes** — **flat in S3, hierarchical on
local disk** — with self-describing filenames. This replaced the earlier
"unified HTM-tree" layout (task-12, 2026-05-25); S3 and local now **diverge**.
See `docs/quaternary_storage_plan.md` (local/uncommitted) for the full design.

A **pod code** is a compact, dynamic-length, **uniformly base-4** string for a
trixel: `q` + two root digits (d0 ∈ {0,1}, d1 ∈ {0-3}; `octant = d0·4 + d1`) +
one quaternary digit(0-3) per level — every digit is one 4-way step. Its length
follows the trixel's actual STARE level (`level + 3` chars: level-2 → `q1321`;
level-4 → `q132110`). It encodes the same address the old
`Q00_1/Q01_3/Q02_2/Q03_1/Q04_1` chain did. (Pre-2026-08-21 codes used a single
base-8 octant digit — `q03200` — one char shorter per level; the two formats
are indistinguishable by inspection when the old octant digit was 0-1, so the
cutover wiped + re-ingested every store, and old/new codes must never share a
prefix or catalog.)

```
LOCAL (hierarchical — cumulative pod-code dir tree, self-describing leaf):
  <root>/q13/q132/q1321/q13211/q132110/q132110-<granule_basename>-<dataset>.parquet

S3 (FLAT — every chunk directly under the storage prefix; pod code IS the key prefix):
  s3://zarrpods/storage/q132110-<granule_basename>-<dataset>.parquet
                       └─ default_s3_prefix (.config field) ─┘
```

**Filename grammar** (`<podcode>-<granule>-<dataset>.parquet`): pod code = before
the first `-`; dataset = after the last `-` (datasets use `_`, never `-`); granule
basename = the middle (may itself contain `-`). The flat S3 key's pod-code prefix
doubles as a native spatial query — `list_objects_v2(Prefix="storage/q13")`
returns the `q13` subtree, no tree walk.

**Codec** (`starepandas/staredataframe.py`): `sid_to_podcode` / `podcode_to_sid` /
`podcode_to_local_dirs` / `chunk_filename` / `parse_chunk_filename` /
`podcode_prefix_length` (level → prefix length, the VCF roll-up's grouping
key — kept beside the codec so it can't drift from the grammar). The writers
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

### Verified checks (basic, 8/8 PASS as of 2026-07-12)
1. `import starepandas` (core package loads + headline symbols present)
2. `import pystare` (dependency, with `from_latlon` / `to_latlon`)
3. `STAREDataFrame` instantiable + `set_sids` + `make_trixels`
4. `sids_from_xy` → `to_latlon` round-trips within tolerance
5. `stare_join` on synthetic STAREDataFrames returns matches
6. Task-7 ingest module + task-1 cloud package reachable at top level
7. Issue-05 overlap analytics reachable at top level (`rendezvous_events` /
   `overlap_matrix` / `overlap_pod_table` / `pair_drilldown` /
   `pod_drilldown` / `fold_instrument`) + functional micro-check: genuine
   trio counted at n=3, fake triangle rejected
8. C-2 `cloud.worker` exposes `Worker` / `WorkerConfig` / `main` / `_is_rds_auth_error`; `WorkerConfig.from_env()` rejects missing `SQS_QUEUE_URL`

### Verified checks (STARE-PODS, 14/14 PASS as of 2026-07-12 — fully online, live-RDS checks included)
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
7. VCF temporal roll-up (issue 04 — `load_local_vcf` level-1 union range ==
   manual leaf aggregation, child counts, subtree prefix scoping,
   `podcode_prefix_length` grouping key)
8. Overlap analytics over the local catalog (issue 05 — pod two instruments,
   thin-load, DELETE the SQLite db, then sweep at two Δt values + matrix +
   pod table + both drill-downs on the loaded frame; headline views agree
   for the pair)
9. `reconstitute_hdf5_from_local` round-trips through Parquet
10. `reconstitute_hdf5_from_s3` (local path) walks pod-code tree
11. `local_starepods_examples.py` end-to-end against the real granules
    (6 granules across 4 instruments — the GMI+SSMIS pair plus the
    4-instrument rendezvous set; ~7156 Parquet partitions at level 4)
12. `pods_unique` UNIQUE constraint exists on `PodsMetadata` (§C10 #1 gate)
    + `t_start`/`t_end`/`podcode` columns present on the live catalog
    + issue-06 index set present (`idx_pods_podcode`, `idx_pods_temporal`,
    `idx_pods_temporal_covering` with its `INCLUDE (podcode, "Dataset")`)
13. `PodsMetadata` insert is idempotent (§C10 #1 live regression —
    double-insert keeps row count stable, DO UPDATE refreshes MetadataJson)
14. C-1..C-6 unit tests pass (cloud.ticket_sizing + metadata + granule_timestamps
    + s3_layout + ingest_module + config_env_secret + control_plane_lambdas
    + completion_watcher + cloud_client + podcode_layout + temporal_catalog
    + temporal_query + vcf_rollup + overlap_analytics + atms_reader —
    currently 216 unit tests)

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

*Last Updated: 2026-08-21 (**uniform quaternary pod codes — 2-digit root**.
The pod-code grammar's one base-8 digit (the level-0 octant) became two
quaternary digits: `q` + `d0∈{0,1}` + `d1∈{0-3}` with `octant = d0·4 + d1`, so
*every* digit is now one 4-way step. `podcode_prefix_length(level)` = `level+3`
(was `+2`); level-4 codes are 7 chars (`q03200 → q003200`, `q63333 → q123333`);
`d0∈{2,3}` invalid. Old codes with leading 2-7 are rejected with an explicit
pre-2026-08-21 hint; old codes with leading 0/1 are *indistinguishable* from
new codes (an old level-4 parses as a new level-3), so this was a **hard
cutover**: every store we own was wiped + re-ingested (local
`/tmp/stare_pods_local`; S3+RDS `gmi-demo-parquet` 7,156 chunks re-ingested
from the host, `loadtest-jan/storage` 14,225 via cloud job `971b946c` — 10/10,
0 failed, on the redeployed worker; 2,000 keys sampled, all 7-char
grammar-valid). Codec changes are confined to `sid_to_podcode` /
`podcode_to_sid` / `podcode_prefix_length`; `podcode_to_local_dirs` rode
through unchanged and now naturally emits the level-0 root dir (`q13/…`), and
the only two call sites with old-grammar arithmetic were
`_podcode_query_prefixes` (ancestor loop now starts at the 2-digit root — the
old start emitted an impossible 1-digit-body `q0-` prefix) and an `overlap.py`
error message (`len-2 → len-3`). **Two operational traps recorded:** (1) the
worker Dockerfile installs a pre-built wheel from `infra/worker/dist/`, so a
rebuild without `python setup.py bdist_wheel` first silently cache-hits the
old image byte-for-byte (same digest) — the push only went out after the wheel
rebuild (digest `8736fd06… → 2a40d17e…`); (2) SSMIS granule
`…S112813-E131004` is in **both** the example-demo set and the 10-granule
loadtest set, and `pods_unique(Dataset, "RawData Collected Time", grouped_id)`
rows carry a single `group_path` — running the two ingests concurrently let
the loadtest's DO UPDATE steal the demo's 1,469 SSMIS rows (scoped catalog
5,687, sweep 403 pods instead of 442) until a re-ingest of that one granule
re-claimed them. Whichever job ingests a shared granule *last* owns its
catalog rows; the loadtest root's 12,756-rows-vs-14,225-objects gap is this
same overlap and is the historical steady state. All analytics verified
identical to pre-cutover modulo spelling: 442 pods at Δt=45 min
({2: 442, 3: 101, 4: 2}), AMSR2–ATMS 359, 4-way pods `q003200`/`q003203`,
pixel counts 139/578/146/9573, local == S3 row for row. Tests: all literal
codes updated across 10 test files; +2 new (all-8-roots round-trip `q00…q13`,
old-format rejection hint). Docs: grammar section above, CONTEXT.md,
`docs/quaternary_storage_plan.md` amendment banner (its 2026-06-14 examples
stay historical), both video-script docs incl. spoken forms
("q-zero-zero-three-two-zero-zero"); MP4s not regenerated (scripts-only per
user). Basic verify script's fixture codes updated; the STARE-PODS script
needed nothing (derives every code from the codec). Suite 376 green, basic
8/8, STARE-PODS 14/14 online.)*

*Prior: 2026-08-07f (**steps 12/13/14 build up 2- → 3- → 4-way, with
several examples each**. The three rendezvous steps ran widest-first and showed
**one** pod apiece, so the narrowest case came last and every width rested on a
single example. Reordered to 2 → 3 → 4 (a demo should build up, not down) and
widened to **3 / 3 / 2** examples — 2 being all the 4-way pods that exist in
this data. New `rendezvous_examples(events, n, count=, metadata=, window=)` in
`demo_plots.py`; `rendezvous_of_size` is now a one-line wrapper over it, so the
selection rules live in one place. Two changes make the extra examples worth
having rather than repetitive: (1) **combination round-robin** — the picks
spread across *different* instrument combinations before taking a second pod
from one already shown, so the three 2-way panels are GMI+SSMIS, AMSR2+ATMS and
ATMS+GMI instead of three GMI+SSMIS pods telling one story three times;
(2) **window-scoped, prorated footprints** — the legibility score
(`min` participant pixels) previously summed `num_rows` over the whole day,
which promoted a pod whose ATMS contribution to *that* meeting was 21 px. The
cause is that `[t_start, t_end]` is an *envelope*: an ATMS chunk in q33320
spans 20:17→21:56 because the granule crosses that pod **twice**, so a
whole-chunk count credits both passes to one rendezvous. `_pod_footprints` now
takes `(meetings, window)` and weights each chunk by the share of its span
inside `[meeting ± Δt]`; bursty two-pass chunks are down-weighted, which is
exactly right. Slivers (< `LEGIBLE_PIXELS` = 250) are held back but still used
when a width would otherwise show fewer examples — which is what keeps
q03203 (SSMIS 139 px) as the second 4-way. Meeting times are now taken from the
same combination a pod was *scored* on (a pod can hold several distinct pairs
hours apart). Pandas gotcha: `Series.clip` fills NaT bounds with `inf`, so the
non-candidate rows must be dropped before clipping or the comparison goes
object-dtype and raises. Header lists in both notebooks: the `12-14.` bullet
was not a valid list marker and rendered as a stray paragraph — now separate
`12.` / `13.` / `14.` bullets, and `**Core workflow**` gained the blank line
its list needed (the other two sections already had one, so that list alone was
not rendering). Notebook cells display each figure inline via `display(fig)` +
`plt.close(fig)` so text and picture stay adjacent. Verified: all 8 pods draw
exactly the labelled number of swaths, smallest participant ≥ 564 px for every
2-/3-way; both notebooks 9 figures, 0 errors, and local vs S3 agree on all 8
pods, all meeting times and all 23 pixel counts. Notebooks grew 1.7 → 3.7 MB
(9 embedded figures at dpi 110). Tests: +9 in `tests/test_demo_plots.py` (26).
Suite 373 green, basic 8/8, STARE-PODS 14/14 online.)*

*Prior: 2026-08-07e (**`SKIP_INGEST` + S3 notebook outputs restored**.
The committed `s3_starepods_examples.ipynb` had **no cell outputs at all** — a
step-12→12/13/14 rework script ended with a blanket `cell.outputs = []` so the
re-run would be honest, the *local* notebook was then re-executed but the S3
one was not (its re-run implied the ~16 min ingest), and it was committed
stripped. Fixed properly rather than by monkeypatching: new `SKIP_INGEST` flag
on the S3 demo (`.py` + `.ipynb`, default **True**) reuses whatever is already
under `S3_PREFIX`. The skip branch is guarded — it loads the catalog and
raises with instructions if the prefix is empty, so a first-time reader gets a
clear error instead of a demo silently running on nothing — and it prints what
it reused, so the notebook's code and its outputs agree (unlike an external
stub, which would have left "stored 0 dataset path(s)" in a worked example).
Full notebook execution drops **20 min → 2.5 min**. The notebook's step 4 also
still lacked the `granule_name=` scoping fixed in the `.py` the round before,
so its reconstitution was still doubled (5966 vs 2983) — now corrected; both
notebooks carry 4 figures, no errors, and the S3 analytics match local
exactly. Timing summary now shows the step-3 partition download (243 s) as the
dominant cost once ingest is skipped. Suite 364 green, basic 8/8, STARE-PODS
14/14.)*

*Prior: 2026-08-07d (**S3 reconstitution scoped to one granule**.
Running both demos with ingest stubbed out (data already on disk/S3) surfaced
a regression from the 4-instrument change: the S3 step-5 structure comparison
showed the reconstituted granule at **(5966, 221) against the original's
(2983, 221)** — exactly double — while local was correct. Cause: the S3
`reconstitute_hdf5` had no `granule_name` parameter at all, and step 4 relied
on a comment that had gone stale ("the bucket only holds this granule's
data"). With two GMI granules under one prefix both were reconstituted merged.
`s3_prefix` cannot express the scope either: the flat layout is
`<prefix>/<podcode>-<granule>-<dataset>.parquet`, so the granule name sits in
the *middle* of the key and a startswith filter can never isolate it. Added
`granule_name=` to `reconstitute_hdf5_from_s3` (matching the `-<granule>-`
span, bracketed so a granule that merely contains another's name cannot
match), threaded it through `StarePodsDemo.reconstitute_hdf5`, and passed it
in the demo — S3 now reconstitutes (2983, 221), matching the original and the
local demo. Both demos verified end to end without re-ingesting; steps 10-14
are byte-identical across backends. Suite 364 green, basic 8/8, STARE-PODS
14/14.)*

*Prior: 2026-08-07c (**`path_prefix` catalog filter** + **2-/3-/4-way
rendezvous steps**. (1) The shared RDS catalog leaked other jobs into the demo:
`Dataset`/`period` cannot separate two ingests of the same instrument over the
same hours, so the S3 panels counted a `loadtest-jan` job's SSMIS chunks (814
pods vs the local 672 — two of its granules straddle any window's edges). New
`path_prefix=` on `load_s3_metadata` / `load_s3_temporal_catalog` /
`load_local_metadata` / `load_local_temporal_catalog` filters on the chunk's
storage root. The path is not a column — it lives in `MetadataJson` — so
`_path_prefix_condition` emits a per-backend accessor (`"MetadataJson"->>
'group_path'` on Postgres, `json_extract(...)` on SQLite), with LIKE wildcards
escaped (`_` is common in prefixes) and an `ESCAPE` clause. No index serves a
JSON field, so it is documented as "combine with period/dataset"; measured at
0.89 s for the whole demo prefix. The S3 demo now scopes with it instead of
`DEMO_WINDOWS` — one call, 7156 rows, **identical to local row for row** (the
window approach was also *lossy*: it clipped SSMIS's 19:57 pass start).
(2) Steps **12/13/14** now show one rendezvous at each width — 4-way, 3-way,
2-way — via `rendezvous_of_size(events, n, metadata=)`, which considers only
pods whose widest rendezvous is *exactly* n (else a "2-way" is drawn with four
swaths) and, given metadata, picks the pod whose *least*-covered participant
has the most pixels — without that it chose a pod where ATMS contributed a
21-pixel sliver. Plot tuning: gentler marker sizes/alphas by density (a
2.5k-pixel sliver was hiding a 35k-pixel swath) and `set_aspect('auto')` (a
polar pod spans tens of degrees of longitude but few of latitude and rendered
as an unreadable sliver). Tests: `tests/test_path_prefix_filter.py` (13) +
6 more in `tests/test_demo_plots.py`. Suite 362 green, basic 8/8, STARE-PODS
14/14.)*

*Prior: 2026-08-07b (demo plots: **antimeridian fix**. Step 11's
coverage map drew full-width horizontal bands across the globe — 22 of GMI's
509 pods rendered with a ~357° longitude span. Cause: `make_trixels`' default
`wrap_lon=True` normalises every longitude into [-180, 180], so a trixel with
corners at 178° and -179° comes back as a polygon going the *long* way round,
filling a band at that latitude. Fix uses the library's own facility rather
than hand-rolled geometry — `make_trixels(wrap_lon=False)` (longitudes stay
continuous) + `STAREDataFrame.split_antimeridian()` → MultiPolygon halves, as
`split_antimeridian`'s own docstring prescribes. Widest single part is now
27.6°; GMI's panel shows its true 65°-inclination sinusoid. (The polar bands
on the sun-synchronous panels are **genuine** — their swaths really do cover
all longitudes near the poles.) Regression tests:
`tests/test_demo_plots.py` (11 — split/both-edges/interior-pod/order, a
whole-octant sweep of 256 pods, plus `widest_rendezvous`); verified meaningful
by confirming the pre-fix path yields 356–357° spans for exactly those pods.
Notebook figures were refreshed **in place** (regenerate the image, swap the
base64) rather than by re-executing — only `pod_trixels` changed and the
figure is a pure function of the unchanged catalog, so no re-ingest was
needed. **Open finding (not fixed):** the S3 step-7/11 panels bleed in other
ingests — SSMIS shows 814 pods vs the local 672, because two `loadtest-jan`
granules (`…S094621-E112812`, `…S131005-E145155`) straddle the edges of
`DEMO_WINDOWS`. The slide-8/9 matrix is unaffected (those passes never go
co-active with another instrument), but the coverage panel overstates what
this demo wrote; `DEMO_WINDOWS` scoping is not by itself enough to isolate a
demo's rows from the shared RDS catalog. Suite 343 green, basic 8/8,
STARE-PODS 14/14.)*

*Prior: 2026-08-07 (demo notebooks: **two plot steps**. The analytics
said *which* pods hold a rendezvous but showed none of the geometry that
produces it, so all four demo files gained **Step 11** (pod coverage map — one
world-map panel per instrument shading the level-4 pods its chunks occupy,
4-way pods outlined) and **Step 12** (the widest rendezvous up close — the
swaths inside the pod's trixel beside their pass windows on a time axis, since
a rendezvous needs *both* halves and one map cannot show them). Step 11 makes
the orbital geometry legible: GMI's 65° inclination confines it to a ±67°
band while the sun-synchronous instruments run pole to pole, which is exactly
why AMSR2–ATMS share 359 pods and anything involving GMI is scarce. New
`starepandas/demo_plots.py` holds all of it (`plot_pod_coverage`,
`plot_rendezvous`, `pod_pixels`, `widest_rendezvous`) so the four demo files
stay thin; `pod_pixels` is backend-agnostic because the pod code prefixes the
chunk *filename* in both layouts, so one `group_path` substring test selects a
pod's chunks on local disk and on flat S3 alike — verified to return identical
pixel counts (SSMIS 139 / AMSR2 578 / ATMS 146 / GMI 9573 in pod q03203) from
both backends. Both figures scope pixels to `[meeting − Δt, meeting + Δt]`,
which matters because SSMIS crosses q03200 in *both* demo windows and the
11:36 pass would otherwise be drawn as if it were part of the 21:29
rendezvous. Demo text states the granularity caveat explicitly: co-location
means the same level-4 pod (~500 km), not the same pixel. cartopy was already
a declared dependency. Both notebooks also gained **per-cell timing**: one hook
cell registers `pre_run_cell`/`post_run_cell` (unregistering first, so
re-running it cannot stack duplicate hooks) so every later cell self-reports
`⏱ cell [n] took X.XXs` into `CELL_TIMINGS`, with a slowest-first summary cell
at the end; the pre-existing `%%time` magics were stripped so nothing
double-reports. Chosen over per-cell `%%time` because it cannot drift as cells
are added or reordered. The numbers make the architecture's point: **local
89.6 s total of which ingest is 84.4 s (94%)** — every query and analytic
(temporal catalog, period filter, VCF roll-up, the whole slide-8/9 sweep) is
≤0.07 s; **S3 20.3 min of which ingest is 16 min and the step-3 partition
download 3.9 min**, the analytics again negligible. Verification: basic 8/8,
STARE-PODS 14/14 online, full suite 332 green; both notebooks re-executed
end-to-end with identical analytics output (matrix, pod table, and the
139/578/146/9573 pixel counts agree local vs S3).)*

*Prior: 2026-08-04 (demo notebooks: **four instruments** in the
slide-8/9 overlap analytics. The example demos previously ingested a GMI+SSMIS
pair, so the slide-9 table only ever had an n=2 column. All 56 healthy granules
of 2025-01-01 (GMI/SSMIS/AMSR2/ATMS, fetched from the Bayesics EC2) were
screened for a genuine **4-way** rendezvous by building the ingest's
`(podcode, Dataset, t_start, t_end)` catalog straight from each granule's
Latitude/Longitude/ScanTime — subsampled, so conservative — and sweeping it
with the shipped `rendezvous_events`. Exactly three 4-way events exist that
day; the chosen set (SSMIS `…S195732`, AMSR2 `…S201914`, ATMS-NOAA-21
`…S201707`, GMI `…S204910`) rendezvouses over pods **q03200/q03203**, the
passes arriving SSMIS 21:29 → AMSR2 21:46 → ATMS 21:47 → GMI 22:13 — so a
4-way needs **Δt≥45 min**. Confirmed by full-resolution ingest, not just the
probe. All four demo files (`local_`/`s3_starepods_examples`, `.py` +
`.ipynb`) now ingest **6 granules across 4 instruments** — the original
co-located pair is *kept* (it is the tightest rendezvous, ≤3 min, and keeps
the GMI–SSMIS matrix cell at 47 rather than 5) plus the quadruple; the two
windows are ~9 h apart so they never cross-contaminate. Step 10 gained the Δt
progression (15/30/45 min, showing where n=3 and then n=4 appear), the widest-
first pod table and a `pod_drilldown` of a 4-way pod (all 6 pairs + all 4
triples + the 4-way). Step 8 now separates the **two GMI passes** by their
data-level range — a sharper period-filter demo than the old bracket/miss pair
— and steps 2–5 stay scoped to the reconstituted granule via the granule-level
`end_date`. Headline (Δt=45 min): 442 pods, 101 with a 3-way, 2 with a 4-way;
matrix AMSR2–ATMS 359 (near-co-orbiting), ATMS–SSMIS 81, ATMS–GMI 68,
AMSR2–SSMIS 58, GMI–SSMIS 47, AMSR2–GMI 43. **Bug fixed:** ATMS could never be
ingested at all — `ATMS.read_timestamps` indexed `self.lat` although callers
run it *before* `read_latlon` (`read_granule` ordering) → `TypeError: argument
of type 'NoneType' is not iterable`, and it emitted one timestamp per scan
*line* instead of per pixel, which cannot align with the flattened grid
`to_df` builds; `ATMS.read_data` read a `self.netcdf` attribute this reader
never sets. Now reads the pixel width from the file (`scan_width` /
`scan_variable`, shape only) and takes up to 6 channels per scan from
`self.dataset`. Regression tests: `tests/test_atms_reader.py` (5). Data:
4 new granules committed to Git LFS (~188 MB; `git check-attr filter`
verified). Verification: basic 8/8, STARE-PODS 14/14 online (check 11 now
6 granules / ~7156 partitions; check 14 = 216 unit tests), full suite 332
green.)*

*Prior: 2026-07-12 (temporal-stare-pods issue 06 COMPLETE — measured
index upgrades. The ADR-0002 "measure before building" gate opened (issues
03 + 05 shipped), so the deferred index menu was profiled at realistic scale:
live 14,739-row RDS catalog + a 2M-row RDS scratch table (server-side
`generate_series`, real column shapes, ingest≈time heap order; dropped after
the run) + a 1M-row SQLite catalog. **Adopted item 1** — covering index
`idx_pods_temporal_covering` (`(t_start, t_end) INCLUDE (podcode, "Dataset")`
on RDS; trailing key columns `(t_start, t_end, podcode, "Dataset")` on
SQLite): the issue-05 analytics thin fetch becomes an index-only scan — RDS
@2M, 7 d period: 201.6 ms / ~25.7k buffers → 67.6 ms / 238 buffers, Heap
Fetches: 0; live catalog EXPLAIN confirms Index Only Scan (3.0 ms) after
`VACUUM (ANALYZE)` (visibility-map note in runbook §6h). DDL applied
idempotently in both initializers — probe-gated on RDS
(`_ensure_rds_db_and_table`, issue-01 pattern); plain per-open
`CREATE INDEX IF NOT EXISTS` on SQLite (`_ensure_sqlite_db_and_table`,
matching its existing index block). `idx_pods_temporal` kept (additive-only
mandate; the narrow index stays the cheaper key-only arm). **Rejected item 2** (`(podcode,
t_start)` composite — only wins exact-pod+period SQL, a shape no shipped
path emits; the `podcode_prefix=` LIKE path is answered index-only by the
covering index) and **item 3** (CLUSTER-by-podcode — zero heap fetches left
to optimize and measured harmful: 7 d fetch 67.6 → 189.1 ms, it destroys
ingest-order temporal locality). Full numbers in the issue-06 profiling note
(`.scratch/temporal-stare-pods/issues/06-covering-index-upgrades.md`);
ADR-0002 "Deferred" annotated with the verdicts. Tests: 3 new in
`tests/test_temporal_catalog.py` (SQLite covering-index shape, plan-level
covering-scan assertion, RDS DDL probe-gating) — 210 unit tests in the gate,
full suite 326 green. Verification: basic 8/8, STARE-PODS 14/14 online
(check 12 now asserts the index set + INCLUDE columns).)*

*Prior: 2026-07-12 (temporal-stare-pods issue 05 COMPLETE —
multi-instrument overlap analytics (slides 8/9). New `starepandas/overlap.py`,
all pure functions over a loaded temporal-catalog frame (no DB/object-store
access — ADR-0002 Decision 2): `rendezvous_events(catalog, dt, period=None,
include_passes=False)` is the one kernel — the ADR-0002 Decision-1 Helly
sweep (`+instrument` at `t_start`, `−instrument` at `t_end + Δt`, per-pod
vectorized segmented cumsum, '+' before '−' at ties so closed bounds hold),
emitting the canonical events frame (`EVENT_COLUMNS`: podcode, time = the
arriving pass's t_start, added, instruments = full active mask as sorted
tuple; optional `passes` = participating catalog index labels, requires a
unique index). The four views aggregate it: `overlap_matrix(events, pods=,
instruments=)` (slide 8; per-pair pod bitmaps → popcount cells; `pods=`
region re-scope = bitmap AND, prefix-cover semantics, no re-sweep),
`overlap_pod_table` (slide 9; distinct n-combos per pod via subset expansion
— a fake triangle never records a 3-mask so n=3 stays 0), `pair_drilldown` /
`pod_drilldown` (frequency = events where `added ∈ combo ⊆ mask`, plus
times; pod_drilldown rolls up a coarser code's subtree). Scan-group folding:
`fold_instrument` strips `_S<n>` (labeling only). Shared contracts extracted
to io/granules: `_require_podcodes` (used by vcf_rollup too) and
`_period_mask` (client-side closed-overlap predicate beside
`_period_conditions`). Post-review hardening: numpy scalars rejected as dt;
tz-aware t_start/t_end normalized to naive UTC (loader parity); pod-code
arguments finer than `MAX_PARTITION_LEVEL` raise (the sliver trap — they'd
silently cover nothing); unfolded instrument args (`'GMI_S1'`) raise instead
of silently matching nothing; typed empty results (pod table always carries
the n=2 int column). Perf: ~135 ms sweep+aggregate on a 100k-row catalog
(1 s CI guard vs combinatorial regression). Tests:
`tests/test_overlap_analytics.py` (26, pure seam + delete-the-db round-trip
proving no catalog access). Live read-only smoke vs the real RDS catalog
(14,739 rows): Δt=30 min → 0 events (verified genuine: min GMI–SSMIS gap in
any shared pod is 36.8 min); Δt=60 min → matrix cell 55 == brute-force
pairwise pod count == pair_drilldown pods. Verification: basic 8/8 (new
check 7), STARE-PODS 14/14 online (new check 8), full suite 323 green.
Issue 06 (covering-index upgrades) stays blocked on profiling.)*

*Prior: 2026-07-11 (temporal-stare-pods issue 04 COMPLETE — VCF temporal
roll-up. The temporal hierarchy ("Virtual Collection File") is queryable
on-the-fly: `vcf_rollup(catalog, level, subtree=None)` — pure function over a
temporal-catalog frame — plus `load_s3_vcf` / `load_local_vcf` return, per pod
at the requested level, the union range `[min(t_start), max(t_end)]` of all
chunks beneath it + `n_chunks` / `n_without_range` (null-range chunks counted
but never in the union; a pod with only range-less chunks gets a null range),
grouped on the pod-code prefix. Level → prefix-length centralized beside the
codec as `podcode_prefix_length(level)` = `level + 2` (can't-drift
requirement). Loaders push subtree/dataset/period into SQL — new
`podcode_prefix=` on the issue-03 thin loaders emits `podcode LIKE '<prefix>%'`
(grammar-validated via `podcode_to_sid`, no SQL wildcards possible; rides
`idx_pods_podcode`); period reuses `_period_conditions`; both backends share
the single pure groupby (no second GROUP-BY dialect). No materialized index
(deferred, issue 06). Post-review hardening: thin loaders exclude NULL-podcode
legacy rows in SQL and `vcf_rollup` raises on them (groupby silently dropped
them); `podcode_to_sid` digits tightened to ASCII (Unicode digits passed the
gate but matched nothing); subtree deeper than the roll-up level raises via
shared `_validate_vcf_args` (mislabeled sliver-envelope trap); half-null
ranges contribute neither end to the union; empty-catalog special case
deleted (dtype-divergent duplicate schema) so `VCF_COLUMNS` is load-bearing;
subtree LIKE deduped into `_podcode_prefix_condition`. Tests:
`tests/test_vcf_rollup.py` (23, pure-rollup + local round-trip +
cloud-parity-by-inspection seams); shared local-seam helpers lifted into
`tests/_temporal_fixtures.py` (issue-03 review flag). Live read-only smoke vs
the real RDS catalog (14,739 chunks): `load_s3_vcf` exactly equals
client-side `vcf_rollup` of the full thin load; subtree + period scoping
match manual aggregation; deep-subtree guard fires live. Verification: basic
7/7, STARE-PODS 13/13 online (new check 7), full suite 299 green.)*

*Prior: 2026-07-11 (temporal-stare-pods issue 03 COMPLETE — temporal-aware
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