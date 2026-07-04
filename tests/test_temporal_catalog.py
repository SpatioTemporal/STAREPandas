"""Temporal-range capture + catalog schema (temporal-stare-pods issue 01).

Verifies, at the local write → local SQLite round-trip seam, that podding a
granule catalogs each chunk with a real temporal range (``t_start``/``t_end`` =
min/max of the chunk's per-point scan times) and its pod code, that the schema
upgrade applies idempotently, and that re-ingest stays idempotent (the
on-conflict path refreshes the temporal range and pod code).

No live cloud dependency — the RDS side shares the same ``PartitionRow`` /
INSERT-SQL seam covered in ``test_metadata_store.py``.
"""

import datetime
import json
import os
import sqlite3

import numpy as np
import pandas as pd
import pystare
import pytest

from starepandas import STAREDataFrame
from starepandas.staredataframe import (
    _chunk_temporal_range,
    _ensure_sqlite_db_and_table,
    sid_to_podcode,
)


T0 = pd.Timestamp('2020-06-15 12:00:00')


def _make_sdf(n_scans=4, pixel_width=5, with_timestamp=True):
    """Synthetic multi-pod STAREDataFrame; scan s is stamped T0 + s seconds."""
    N = n_scans * pixel_width
    rng = np.random.default_rng(42)
    lats = rng.uniform(33, 41, N)
    lons = rng.uniform(-124, -116, N)
    data = {
        'lat': lats,
        'lon': lons,
        'sids': pystare.from_latlon(lats, lons, level=6),
    }
    if with_timestamp:
        timestamps = []
        for s in range(n_scans):
            timestamps.extend([T0 + pd.Timedelta(seconds=s)] * pixel_width)
        data['timestamp'] = timestamps
    sdf = STAREDataFrame(data)
    sdf._sid_column_name = 'sids'
    return sdf


def _catalog_rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            'SELECT "Dataset", grouped_id, t_start, t_end, podcode, "MetadataJson" '
            'FROM "PodsMetadata"'
        )
        return cur.fetchall()
    finally:
        conn.close()


def _write(tmp_path, sdf, **kwargs):
    root = os.path.join(str(tmp_path), 'local_root')
    db_path = os.path.join(str(tmp_path), 'metadata.db')
    kwargs.setdefault('dataset', 'GMI_S1')
    kwargs.setdefault('granule_name', 'G1')
    kwargs.setdefault('raw_collected_time', datetime.datetime(2020, 6, 15))
    sdf.to_local(root, level=6, db_path=db_path, **kwargs)
    return root, db_path


# ----- _chunk_temporal_range (pure helper) ---------------------------------


def test_chunk_temporal_range_is_min_max_of_points():
    df = pd.DataFrame({'timestamp': [T0 + pd.Timedelta(seconds=s) for s in (3, 0, 7)]})
    t_start, t_end = _chunk_temporal_range(df)
    assert t_start == T0.to_pydatetime()
    assert t_end == (T0 + pd.Timedelta(seconds=7)).to_pydatetime()
    assert t_start <= t_end


def test_chunk_temporal_range_drops_missing_times():
    df = pd.DataFrame({'timestamp': [pd.NaT, T0, None, T0 + pd.Timedelta(seconds=5)]})
    t_start, t_end = _chunk_temporal_range(df)
    assert t_start == T0.to_pydatetime()
    assert t_end == (T0 + pd.Timedelta(seconds=5)).to_pydatetime()


def test_chunk_temporal_range_no_column_is_null():
    assert _chunk_temporal_range(pd.DataFrame({'lat': [1.0]})) == (None, None)


def test_chunk_temporal_range_all_missing_is_null():
    df = pd.DataFrame({'timestamp': [pd.NaT, pd.NaT]})
    assert _chunk_temporal_range(df) == (None, None)


# ----- local write → SQLite catalog round-trip ------------------------------


def test_to_local_catalogs_temporal_range_and_podcode(tmp_path):
    sdf = _make_sdf()
    _root, db_path = _write(tmp_path, sdf)

    rows = _catalog_rows(db_path)
    assert rows, "no catalog rows written"

    # Recompute each chunk's expected range from the source points, keyed by
    # the same pod grouping the writer uses (level capped at 4 → the pod's
    # SID is the grouped_id in the catalog).
    from starepandas.staredataframe import MAX_PARTITION_LEVEL
    coerced = pystare.spatial_coerce_resolution(
        sdf['sids'].to_numpy().astype(np.int64), MAX_PARTITION_LEVEL)
    coerced = pystare.spatial_clear_to_resolution(coerced)
    expected = {
        int(gid): (g['timestamp'].min(), g['timestamp'].max())
        for gid, g in sdf.groupby(coerced)
    }

    assert len(rows) == len(expected)
    for dataset, gid, t_start, t_end, podcode, meta in rows:
        assert dataset == 'GMI_S1'
        assert t_start is not None and t_end is not None
        got_start, got_end = pd.Timestamp(t_start), pd.Timestamp(t_end)
        exp_start, exp_end = expected[int(gid)]
        assert got_start == exp_start, f"t_start mismatch for chunk {gid}"
        assert got_end == exp_end, f"t_end mismatch for chunk {gid}"
        assert got_start <= got_end
        assert podcode == sid_to_podcode(int(gid))
        # Pod code in the catalog matches the chunk file the row points at.
        group_path = json.loads(meta)['group_path']
        assert os.path.basename(group_path).startswith(podcode + '-')


def test_to_local_without_timestamp_column_writes_null_range(tmp_path):
    sdf = _make_sdf(with_timestamp=False)
    _root, db_path = _write(tmp_path, sdf)

    rows = _catalog_rows(db_path)
    assert rows, "timestamp-less write must still catalog its chunks"
    for _ds, gid, t_start, t_end, podcode, _meta in rows:
        assert t_start is None and t_end is None
        assert podcode == sid_to_podcode(int(gid))


def test_to_local_reingest_is_idempotent_and_refreshes(tmp_path):
    """Same granule, same raw_collected_time → same rows, refreshed fields."""
    sdf = _make_sdf()
    root, db_path = _write(tmp_path, sdf)
    n1 = len(_catalog_rows(db_path))

    # Re-ingest verbatim: row count must not grow.
    sdf.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                 granule_name='G1', raw_collected_time=datetime.datetime(2020, 6, 15))
    rows = _catalog_rows(db_path)
    assert len(rows) == n1, "re-ingest duplicated catalog rows"

    # Re-ingest with shifted point times: on-conflict refreshes the range.
    shifted = _make_sdf()
    shifted['timestamp'] = shifted['timestamp'] + pd.Timedelta(hours=1)
    shifted.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                     granule_name='G1', raw_collected_time=datetime.datetime(2020, 6, 15))
    rows = _catalog_rows(db_path)
    assert len(rows) == n1
    for _ds, _gid, t_start, _t_end, _pc, _meta in rows:
        assert pd.Timestamp(t_start) >= T0 + pd.Timedelta(hours=1), \
            "on-conflict path did not refresh t_start"


def test_to_local_without_dataset_is_still_idempotent(tmp_path):
    """dataset=None must not defeat the upsert: SQLite treats NULLs as
    distinct in unique indexes, so the catalog stores the same 'data'
    fallback the chunk filename uses."""
    sdf = _make_sdf()
    root = os.path.join(str(tmp_path), 'local_root')
    db_path = os.path.join(str(tmp_path), 'metadata.db')
    for _ in range(2):
        sdf.to_local(root, level=6, db_path=db_path, granule_name='G1',
                     raw_collected_time=datetime.datetime(2020, 6, 15))
    rows = _catalog_rows(db_path)
    counts = {}
    for ds, gid, *_rest in rows:
        assert ds == 'data', f"expected 'data' fallback, got {ds!r}"
        counts[gid] = counts.get(gid, 0) + 1
    assert all(n == 1 for n in counts.values()), \
        f"dataset=None re-ingest duplicated rows: {counts}"


# ----- schema idempotency ---------------------------------------------------


def test_sqlite_schema_upgrade_is_idempotent(tmp_path):
    db_path = os.path.join(str(tmp_path), 'meta.db')
    for _ in range(2):  # fresh create, then re-open — must not error
        conn = _ensure_sqlite_db_and_table(db_path)
        conn.close()
    conn = sqlite3.connect(db_path)
    try:
        cols = [r[1] for r in conn.execute('PRAGMA table_info("PodsMetadata")')]
    finally:
        conn.close()
    for col in ('t_start', 't_end', 'podcode'):
        assert cols.count(col) == 1, f"column {col} missing or duplicated: {cols}"


def test_sqlite_legacy_catalog_gains_temporal_columns(tmp_path):
    """Opening a pre-temporal catalog adds the three columns in place."""
    db_path = os.path.join(str(tmp_path), 'legacy.db')
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE "PodsMetadata" (
            "Dataset"               TEXT,
            "DataLevel"             TEXT,
            "RawData Collected Time" TEXT,
            grouped_id              INTEGER,
            "LocalPath"             TEXT,
            "Resolution level"      INTEGER,
            "MetadataJson"          TEXT
        )
    """)
    conn.execute(
        'INSERT INTO "PodsMetadata" VALUES (?, ?, ?, ?, ?, ?, ?)',
        ('GMI_S1', 'L1C', '2020-06-15T00:00:00', 42, '/x', 4, '{}'),
    )
    conn.commit()
    conn.close()

    conn = _ensure_sqlite_db_and_table(db_path)
    try:
        cols = [r[1] for r in conn.execute('PRAGMA table_info("PodsMetadata")')]
        for col in ('t_start', 't_end', 'podcode'):
            assert cols.count(col) == 1, f"upgrade missed column {col}: {cols}"
        # Legacy row survives with null temporal fields.
        row = conn.execute(
            'SELECT t_start, t_end, podcode FROM "PodsMetadata"').fetchone()
        assert row == (None, None, None)
    finally:
        conn.close()


def test_sqlite_schema_has_temporal_indexes(tmp_path):
    db_path = os.path.join(str(tmp_path), 'meta.db')
    conn = _ensure_sqlite_db_and_table(db_path)
    try:
        indexes = {r[1]: r for r in conn.execute('PRAGMA index_list("PodsMetadata")')}
        assert 'idx_pods_podcode' in indexes
        assert 'idx_pods_temporal' in indexes
        assert 'pods_unique' in indexes
        # pods_unique must actually be UNIQUE (r[2] == 1) to back the upsert.
        assert indexes['pods_unique'][2] == 1
        temporal_cols = [r[2] for r in conn.execute('PRAGMA index_info("idx_pods_temporal")')]
        assert temporal_cols == ['t_start', 't_end']
    finally:
        conn.close()


# ----- RDS DDL shape (no live database) -------------------------------------


def test_rds_ddl_adds_temporal_columns_idempotently():
    """The RDS initializer must carry the same three columns and indexes,
    gated behind catalog probes (information_schema / pg_indexes) so
    re-opening an existing catalog neither errors nor takes DDL locks."""
    import inspect
    import starepandas.staredataframe as m
    src = inspect.getsource(m._ensure_rds_db_and_table)
    assert 'ADD COLUMN IF NOT EXISTS' in src
    for col in ('t_start', 't_end', 'podcode'):
        assert f"'{col}'" in src, f"RDS initializer does not handle {col}"
    assert 'information_schema.columns' in src, \
        "column DDL must be probe-gated (no per-connect ALTER lock)"
    assert 'pg_indexes' in src, \
        "index DDL must be probe-gated (no per-connect CREATE INDEX)"
    assert 'idx_pods_podcode' in src
    assert 'idx_pods_temporal' in src
