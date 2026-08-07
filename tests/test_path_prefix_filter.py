"""Tests for the storage-root (``path_prefix``) catalog filter.

The RDS catalog is shared by every ingest, and ``Dataset``/``period`` cannot
separate two jobs that cover the same instrument over the same hours — the
demo's SSMIS panel was silently counting a load-test job's chunks alongside
its own. The chunk's ``group_path`` can separate them, but it lives inside
``MetadataJson`` rather than in a column of its own, so each backend needs its
own JSON accessor.
"""

import json
import sqlite3

import pandas as pd
import pytest

from starepandas.io.granules import (
    _GROUP_PATH_SQL, _path_prefix_condition, load_local_metadata,
    load_local_temporal_catalog,
)


# ----- the predicate itself --------------------------------------------------

def test_postgres_uses_the_jsonb_accessor():
    conditions, params = _path_prefix_condition('s3://b/demo', 'postgres')
    assert conditions == [_GROUP_PATH_SQL['postgres'] + " LIKE %s ESCAPE '\\'"]
    assert params == ['s3://b/demo%']


def test_sqlite_uses_json_extract_and_its_own_placeholder():
    conditions, params = _path_prefix_condition('/tmp/root', 'sqlite', placeholder='?')
    assert conditions == [_GROUP_PATH_SQL['sqlite'] + " LIKE ? ESCAPE '\\'"]
    assert params == ['/tmp/root%']


def test_like_wildcards_in_the_prefix_are_escaped():
    """'_' is a single-char wildcard and is common in bucket/prefix names."""
    _, params = _path_prefix_condition('s3://b/load_test%x', 'postgres')
    assert params == [r's3://b/load\_test\%x%']


@pytest.mark.parametrize('bad', ['', None, 123])
def test_rejects_a_non_prefix(bad):
    with pytest.raises(ValueError, match='path_prefix'):
        _path_prefix_condition(bad, 'postgres')


def test_rejects_an_unknown_backend():
    with pytest.raises(ValueError, match='backend'):
        _path_prefix_condition('s3://b/demo', 'mysql')


def test_escaping_actually_works_in_sqlite():
    """The ESCAPE clause must make '_' literal, not a single-char wildcard."""
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE t (p TEXT)')
    conn.executemany('INSERT INTO t VALUES (?)', [('load_test/a',), ('loadXtest/b',)])

    conditions, params = _path_prefix_condition('load_test', 'sqlite', placeholder='?')
    # Point the condition at this table's plain column instead of the JSON blob.
    predicate = conditions[0].replace(_GROUP_PATH_SQL['sqlite'], 'p')
    rows = [r[0] for r in conn.execute(f'SELECT p FROM t WHERE {predicate}', params)]

    assert rows == ['load_test/a']          # 'loadXtest/b' must NOT match


# ----- end to end through the local loaders ---------------------------------

def _seed_two_roots(tmp_path):
    """A catalog holding two ingests of the same dataset over the same times,
    distinguishable only by where their chunks were written.

    Built through the library's own initializer so the schema (and its UNIQUE
    constraint) is exactly the real one.
    """
    from starepandas.staredataframe import _ensure_sqlite_db_and_table

    db = str(tmp_path / 'metadata.db')
    conn = _ensure_sqlite_db_and_table(db)
    for grouped_id, (root, pod) in enumerate(
            (('/data/mine', 'q03200'), ('/data/other', 'q03201')), start=1):
        conn.execute(
            'INSERT INTO "PodsMetadata" '
            '("Dataset", "DataLevel", "RawData Collected Time", grouped_id, '
            ' "Resolution level", "MetadataJson", t_start, t_end, podcode) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            ('SSMIS_S1', 'L1C', '2025-01-01T00:00:00', grouped_id, 4,
             json.dumps({'group_path': f'{root}/{pod}-granule-SSMIS_S1.parquet'}),
             '2025-01-01T00:00:00', '2025-01-01T00:01:00', pod),
        )
    conn.commit()
    conn.close()
    return db


def test_temporal_catalog_selects_only_the_named_root(tmp_path):
    db = _seed_two_roots(tmp_path)
    assert len(load_local_temporal_catalog(db)) == 2
    mine = load_local_temporal_catalog(db, path_prefix='/data/mine')
    assert list(mine['podcode']) == ['q03200']


def test_metadata_loader_selects_only_the_named_root(tmp_path):
    db = _seed_two_roots(tmp_path)
    mine = load_local_metadata(db, path_prefix='/data/other')
    assert list(mine['podcode']) == ['q03201']


def test_a_root_matching_nothing_yields_an_empty_frame(tmp_path):
    db = _seed_two_roots(tmp_path)
    empty = load_local_temporal_catalog(db, path_prefix='/data/nonexistent')
    assert empty.empty
    # ... and still carries the documented projection.
    assert list(empty.columns) == ['podcode', 'Dataset', 't_start', 't_end']


def test_prefix_must_match_from_the_start_not_anywhere(tmp_path):
    db = _seed_two_roots(tmp_path)
    assert load_local_temporal_catalog(db, path_prefix='mine').empty


def test_path_prefix_composes_with_period(tmp_path):
    db = _seed_two_roots(tmp_path)
    hit = load_local_temporal_catalog(
        db, path_prefix='/data/mine',
        period=(pd.Timestamp('2025-01-01 00:00'), pd.Timestamp('2025-01-01 00:05')))
    assert list(hit['podcode']) == ['q03200']
    miss = load_local_temporal_catalog(
        db, path_prefix='/data/mine',
        period=(pd.Timestamp('2025-02-01 00:00'), pd.Timestamp('2025-02-01 00:05')))
    assert miss.empty


# ----- granule scoping for the S3 reconstitute path --------------------------

def test_reconstitute_from_s3_accepts_a_granule_name():
    """A prefix cannot isolate a granule in the flat layout.

    S3 keys are ``<prefix>/<podcode>-<granule>-<dataset>.parquet``, so the
    granule sits in the middle and ``s3_prefix`` (a startswith filter) cannot
    select it. Without ``granule_name``, a prefix holding two granules of the
    same dataset reconstitutes both merged — silently doubling the scan lines.
    """
    import inspect

    from starepandas.demo_lib import StarePodsDemo
    from starepandas.io.granules import reconstitute_hdf5_from_s3

    assert 'granule_name' in inspect.signature(reconstitute_hdf5_from_s3).parameters
    # ... and the demo wrapper must forward it, or the demo cannot use it.
    assert 'granule_name' in inspect.signature(
        StarePodsDemo.reconstitute_hdf5).parameters


def test_granule_marker_is_bracketed_by_separators():
    """The filter must not match a granule that merely *contains* the name."""
    frame = pd.DataFrame({'group_path': [
        's3://b/p/q03200-1C.GPM.GMI.20250101-S112952.061572.V07B-GMI_S1.parquet',
        's3://b/p/q03200-1C.GPM.GMI.20250101-S204910.061578.V07B-GMI_S1.parquet',
    ]})
    wanted = '1C.GPM.GMI.20250101-S112952.061572.V07B'
    hits = frame[frame['group_path'].str.contains(f'-{wanted}-', regex=False)]
    assert len(hits) == 1
    assert 'S112952' in hits.iloc[0]['group_path']
