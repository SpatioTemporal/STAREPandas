"""Server-side catalog aggregation loaders (Q1-2025 step 7, 2026-09-07).

``load_*_catalog_summary`` (one row per storage root × dataset × platform)
and ``load_*_pod_occupancy`` (chunks per dataset × pod). The local/SQLite
twins are exercised end to end through a real ``to_local`` write; the RDS
variants are checked at the SQL seam (same builder, Postgres dialect) with
the connection stubbed, and the two path-derivation helpers — the contract
both backends implement — are pinned against the Postgres semantics.
"""

import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

import starepandas.io.granules as gr
from starepandas.io.granules import (
    CATALOG_SUMMARY_COLUMNS,
    POD_OCCUPANCY_COLUMNS,
    _platform_of,
    _storage_root_of,
    load_local_catalog_summary,
    load_local_pod_occupancy,
    load_s3_catalog_summary,
    load_s3_pod_occupancy,
)

from tests._temporal_fixtures import T0, single_pod_sdf, write_local

GRANULE = '1C.F18.SSMIS.XCAL2021-V.20250101-S112813-E131004.078441.V07B'


# ── path-derivation contract ───────────────────────────────────────────────


def test_storage_root_is_the_chunk_directory():
    assert _storage_root_of(
        's3://zarrpods/storage/q003200-%s-SSMIS_S1.parquet' % GRANULE
    ) == 's3://zarrpods/storage'
    assert _storage_root_of('/tmp/root/q13/q132/q1321-G1-GMI_S1.parquet') == \
        '/tmp/root/q13/q132'
    assert _storage_root_of('no-slash.parquet') == 'no-slash.parquet'
    assert _storage_root_of(None) is None


def test_platform_is_second_dot_field_of_the_granule_basename():
    assert _platform_of('s3://b/p/q003200-%s-SSMIS_S1.parquet' % GRANULE) == 'F18'
    assert _platform_of(
        's3://b/p/q003200-1C.NOAA21.ATMS.XCAL2019-V.20250101-S201707-E215836.008617.V07A-ATMS_S4.parquet'
    ) == 'NOAA21'
    # A granule name without dots (test fixtures) has no second field —
    # Postgres's split_part gives '' there; the loaders turn it into None.
    assert _platform_of('/tmp/root/q1321-G1-GMI_S1.parquet') == ''
    # The dataset leaf and the pod code are stripped before splitting, so a
    # granule whose *own* name carries '-' still resolves.
    assert _platform_of('/r/q13-1C.GPM.GMI.X-V.2025-S1-E2.1.V07B-GMI_S1.parquet') == 'GPM'
    assert _platform_of(None) is None


# ── local seam ─────────────────────────────────────────────────────────────


def _two_granule_store(tmp_path):
    """Two granules of one dataset in two pods, one of them platform-named."""
    root, db_path = write_local(tmp_path, single_pod_sdf(lat=37.0, lon=-120.0),
                                dataset='GMI_S1', granule_name=GRANULE,
                                raw_collected_time=datetime.datetime(2025, 1, 1, 11))
    au = single_pod_sdf(lat=-25.0, lon=134.0,
                        t_start=T0 + pd.Timedelta(days=1),
                        t_end=T0 + pd.Timedelta(days=1, minutes=30))
    au.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                granule_name='G_AU',
                raw_collected_time=datetime.datetime(2025, 1, 2, 11))
    return root, db_path


def test_local_summary_counts_granules_chunks_pods_and_span(tmp_path):
    root, db_path = _two_granule_store(tmp_path)
    summary = load_local_catalog_summary(db_path)

    assert list(summary.columns) == CATALOG_SUMMARY_COLUMNS
    # Two granules land in two different pod directories (hierarchical
    # local layout), so the storage_root — the chunk's directory — differs;
    # one carries the F18 platform, the other has no dot-field.
    assert len(summary) == 2
    assert set(summary['Dataset']) == {'GMI_S1'}
    assert set(summary['platform']) == {'F18', None}
    assert summary['granules'].tolist() == [1, 1]
    assert summary['chunks'].tolist() == [1, 1]
    assert summary['pods'].tolist() == [1, 1]
    assert all(r.startswith(root) for r in summary['storage_root'])
    assert summary['t_start'].min() == T0
    assert summary['t_end'].max() == T0 + pd.Timedelta(days=1, minutes=30)
    assert str(summary['t_start'].dtype).startswith('datetime64')


def test_local_summary_filters_period_dataset_and_path_prefix(tmp_path):
    root, db_path = _two_granule_store(tmp_path)

    day2 = (T0 + pd.Timedelta(days=1), T0 + pd.Timedelta(days=2))
    only_au = load_local_catalog_summary(db_path, period=day2)
    assert len(only_au) == 1 and only_au['platform'].iloc[0] is None

    assert load_local_catalog_summary(db_path, dataset='SSMIS_S1').empty
    assert len(load_local_catalog_summary(db_path, dataset_prefix='GMI')) == 2

    ca_root = load_local_catalog_summary(db_path, period=(T0, T0 + pd.Timedelta(hours=1)))
    scoped = load_local_catalog_summary(db_path, path_prefix=ca_root['storage_root'].iloc[0])
    assert len(scoped) == 1 and scoped['platform'].iloc[0] == 'F18'


def test_local_occupancy_is_chunks_per_dataset_and_pod(tmp_path):
    _, db_path = _two_granule_store(tmp_path)
    occ = load_local_pod_occupancy(db_path)
    assert list(occ.columns) == POD_OCCUPANCY_COLUMNS
    assert len(occ) == 2 and occ['chunks'].tolist() == [1, 1]
    assert occ['podcode'].is_unique
    # Re-ingesting a granule upserts (idempotent catalog): still one chunk
    # per pod, and the summary's granule count is unchanged.
    write_local(tmp_path, single_pod_sdf(lat=37.0, lon=-120.0),
                dataset='GMI_S1', granule_name=GRANULE,
                raw_collected_time=datetime.datetime(2025, 1, 1, 11))
    occ2 = load_local_pod_occupancy(db_path)
    assert occ2['chunks'].tolist() == [1, 1]
    assert load_local_catalog_summary(db_path)['granules'].sum() == 2


def test_local_summary_and_occupancy_agree_on_totals(tmp_path):
    _, db_path = _two_granule_store(tmp_path)
    summary = load_local_catalog_summary(db_path)
    occ = load_local_pod_occupancy(db_path)
    assert summary['chunks'].sum() == occ['chunks'].sum()
    assert occ['podcode'].nunique() == 2


def test_empty_catalog_returns_typed_empty_frames(tmp_path):
    from starepandas.staredataframe import _ensure_sqlite_db_and_table
    db_path = str(tmp_path / 'empty.db')
    _ensure_sqlite_db_and_table(db_path).close()
    summary = load_local_catalog_summary(db_path)
    occ = load_local_pod_occupancy(db_path)
    assert summary.empty and list(summary.columns) == CATALOG_SUMMARY_COLUMNS
    assert occ.empty and list(occ.columns) == POD_OCCUPANCY_COLUMNS
    assert str(summary['t_start'].dtype).startswith('datetime64')


# ── RDS seam: same builder, Postgres dialect, connection stubbed ───────────


def _stub_rds(monkeypatch, rows):
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.__enter__.return_value = cur
    conn = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr('starepandas.staredataframe._ensure_rds_db_and_table',
                        lambda name='StarePodsMetadata': conn)
    return conn, cur


def test_s3_summary_sql_aggregates_server_side(monkeypatch):
    conn, cur = _stub_rds(monkeypatch, [
        ('s3://zarrpods/storage', 'GMI_S1', 'GPM', 1385, 388202, 1980,
         datetime.datetime(2025, 1, 1, 0, 37), datetime.datetime(2025, 4, 1, 0, 33)),
        ('s3://zarrpods/gmi-demo-parquet', 'GMI_S1', '', 2, 529, 509,
         datetime.datetime(2025, 1, 1, 11, 29), datetime.datetime(2025, 1, 1, 22, 22)),
    ])
    df = load_s3_catalog_summary(path_prefix='s3://zarrpods/storage',
                                 period=(pd.Timestamp('2025-02-05'),
                                         pd.Timestamp('2025-02-12')))
    sql, params = cur.execute.call_args.args
    assert 'GROUP BY 1, 2, 3' in sql
    assert 'COUNT(DISTINCT "RawData Collected Time") AS granules' in sql
    assert 'COUNT(DISTINCT podcode) AS pods' in sql
    assert "regexp_replace(\"MetadataJson\"->>'group_path', '/[^/]*$', '')" in sql
    assert "split_part(regexp_replace(regexp_replace(split_part(" in sql
    assert 'podcode IS NOT NULL' in sql
    assert "\"MetadataJson\"->>'group_path' LIKE %s ESCAPE" in sql
    assert params[0] == 's3://zarrpods/storage%'
    assert len(params) == 4          # path + t_start BETWEEN (2) + t_end >=
    assert conn.close.called
    assert df['platform'].tolist() == ['GPM', None]
    assert df['chunks'].sum() == 388731 and df['granules'].dtype == 'int64'


def test_s3_occupancy_sql_groups_by_dataset_and_pod(monkeypatch):
    _, cur = _stub_rds(monkeypatch, [('GMI_S1', 'q003200', 12)])
    df = load_s3_pod_occupancy(dataset_prefix='GMI')
    sql, params = cur.execute.call_args.args
    assert sql.startswith('SELECT "Dataset", podcode, COUNT(*) AS chunks')
    assert 'GROUP BY 1, 2' in sql and 'MetadataJson' not in sql
    assert params == ['GMI_%']
    assert df.iloc[0].tolist() == ['GMI_S1', 'q003200', 12]


def test_loaders_reject_bad_period_and_podcode_before_connecting(monkeypatch):
    monkeypatch.setattr('starepandas.staredataframe._ensure_rds_db_and_table',
                        lambda name='StarePodsMetadata': pytest.fail('connected'))
    with pytest.raises(ValueError):
        load_s3_catalog_summary(period=(pd.Timestamp('2025-02-12'),
                                        pd.Timestamp('2025-02-05')))
    with pytest.raises(ValueError):
        load_s3_pod_occupancy(podcode_prefix='Q00')
