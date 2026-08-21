"""Temporal-aware intersection — period filter (temporal-stare-pods issue 03).

Verifies, at the local write → local SQLite round-trip seam, that the metadata
loaders and the demo intersection accept an optional time period and include a
chunk exactly when its temporal range ``[t_start, t_end]`` overlaps the period
(``t_start ≤ period_end AND t_end ≥ period_start``), ANDed with the spatial pod
match. Omitting the period preserves spatial-only behavior exactly.

The period predicate must follow the ADR-0002 Decision-3 index-friendly
rewrite: a ``t_start`` range (``t_start BETWEEN period_start − D_MAX AND
period_end``) plus the residual exact ``t_end ≥ period_start`` filter — a bare
``t_end ≥ …`` half cannot use the ``(t_start, t_end)`` index. The cloud (RDS)
loader shares the predicate-builder seam, so it is covered here without a live
database (same pattern as the RDS DDL test in ``test_temporal_catalog.py``).
"""

import datetime
import inspect

import numpy as np
import pandas as pd
import pystare
import pytest

import starepandas.io.granules as granules
from starepandas.demo_lib import StarePodsDemo
from starepandas.io.granules import (
    D_MAX,
    _finish_temporal_catalog,
    _period_conditions,
    load_local_metadata,
    load_local_temporal_catalog,
    load_s3_metadata,
    load_s3_temporal_catalog,
)
from tests._temporal_fixtures import (
    CHUNK_END,
    CHUNK_START,
    T0,
    single_pod_sdf as _single_pod_sdf,
    two_pod_demo as _two_pod_demo,
    write_local as _write,
)


# ----- the pure predicate-builder seam (shared by SQLite and RDS loaders) ---


def test_dmax_is_a_named_generous_chunk_duration_bound():
    """D_MAX bounds one granule pass (≈ one orbit ≈ 100 min): must be at
    least that, and small enough to keep the t_start range selective."""
    assert isinstance(D_MAX, datetime.timedelta)
    assert D_MAX >= datetime.timedelta(minutes=100)
    assert D_MAX <= datetime.timedelta(hours=6)


def test_period_conditions_use_tstart_range_plus_residual_tend():
    ps = datetime.datetime(2020, 6, 15, 12, 0, 0)
    pe = datetime.datetime(2020, 6, 15, 18, 0, 0)
    conditions, params = _period_conditions((ps, pe), placeholder='%s')

    assert conditions == ['t_start >= %s', 't_start <= %s', 't_end >= %s']
    assert params == [ps - D_MAX, pe, ps]


def test_period_conditions_sqlite_uses_iso_text():
    """SQLite stores t_start/t_end as ISO-8601 TEXT, so the params must be
    ISO strings for the lexicographic comparison to be correct."""
    ps, pe = T0.to_pydatetime(), (T0 + pd.Timedelta(hours=6)).to_pydatetime()
    conditions, params = _period_conditions((ps, pe), placeholder='?', as_iso=True)

    assert conditions == ['t_start >= ?', 't_start <= ?', 't_end >= ?']
    assert params == [(ps - D_MAX).isoformat(), pe.isoformat(), ps.isoformat()]


def test_period_conditions_accept_strings_and_timestamps():
    _conds, params = _period_conditions(('2020-06-15', pd.Timestamp('2020-06-16')))
    assert params[1] == datetime.datetime(2020, 6, 16)


def test_period_conditions_reject_inverted_period():
    with pytest.raises(ValueError):
        _period_conditions((T0 + pd.Timedelta(days=1), T0))


def test_period_conditions_reject_open_ended_period():
    """None/NaT bounds must raise — on SQLite they would otherwise serialize
    to the literal string 'NaT' and silently match nothing."""
    with pytest.raises(ValueError):
        _period_conditions((None, T0))
    with pytest.raises(ValueError):
        _period_conditions((T0, pd.NaT))


def test_period_conditions_normalize_tz_aware_to_naive_utc():
    """The catalog stores naive UTC; tz-aware bounds must be converted, not
    isoformatted with an offset suffix (which breaks the lexicographic TEXT
    comparison on SQLite)."""
    ps = pd.Timestamp('2020-06-15 05:00:00+05:00')   # == 2020-06-15 00:00 UTC
    pe = pd.Timestamp('2020-06-15 12:00:00Z')
    _conds, params = _period_conditions((ps, pe), placeholder='?', as_iso=True)
    assert params[1] == '2020-06-15T12:00:00'
    assert params[2] == '2020-06-15T00:00:00'
    assert all('+' not in p for p in params)


def test_tz_aware_period_matches_naive_catalog(tmp_path):
    """Round-trip: a UTC-aware period equivalent to a bracketing naive one
    must match the same chunk."""
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    df = load_local_metadata(
        db_path,
        period=((CHUNK_START - pd.Timedelta(hours=1)).tz_localize('UTC'),
                (CHUNK_END + pd.Timedelta(hours=1)).tz_localize('UTC')))
    assert len(df) == 1


def test_s3_loader_routes_period_through_shared_predicate():
    """No live RDS in unit tests — pin the cloud loader to the same
    index-friendly predicate builder the local loader uses."""
    for fn in (load_s3_metadata, load_s3_temporal_catalog):
        src = inspect.getsource(fn)
        assert '_period_conditions' in src, \
            f"{fn.__name__} must build its period predicate via _period_conditions"


# ----- local round-trip: ingest, then query with a period --------------------


def test_period_bracketing_chunk_includes_it(tmp_path):
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    df = load_local_metadata(
        db_path, period=(CHUNK_START - pd.Timedelta(hours=1),
                         CHUNK_END + pd.Timedelta(hours=1)))
    assert len(df) == 1


def test_period_outside_chunk_excludes_it(tmp_path):
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    after = load_local_metadata(
        db_path, period=(CHUNK_END + pd.Timedelta(days=2),
                         CHUNK_END + pd.Timedelta(days=3)))
    before = load_local_metadata(
        db_path, period=(CHUNK_START - pd.Timedelta(days=3),
                         CHUNK_START - pd.Timedelta(days=2)))
    assert after.empty
    assert before.empty


def test_period_straddling_boundary_includes_chunk(tmp_path):
    """Overlap, not containment: a chunk straddling either period boundary
    matches."""
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    # Period starts inside the chunk's range.
    tail = load_local_metadata(
        db_path, period=(CHUNK_START + pd.Timedelta(minutes=15),
                         CHUNK_END + pd.Timedelta(hours=1)))
    # Period ends inside the chunk's range.
    head = load_local_metadata(
        db_path, period=(CHUNK_START - pd.Timedelta(hours=1),
                         CHUNK_START + pd.Timedelta(minutes=15)))
    assert len(tail) == 1
    assert len(head) == 1


def test_dmax_rewrite_keeps_recent_starter_visible(tmp_path):
    """A chunk whose t_start precedes period_start by less than D_MAX but
    whose range overlaps the period must be found (the t_start range prunes
    with slack D_MAX; the residual t_end filter does the exact test)."""
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    period_start = CHUNK_START + pd.Timedelta(minutes=20)   # < D_MAX after t_start
    assert period_start - CHUNK_START < D_MAX
    df = load_local_metadata(
        db_path, period=(period_start, period_start + pd.Timedelta(hours=1)))
    assert len(df) == 1


def test_no_period_is_identical_to_spatial_only(tmp_path):
    _root, db_path = _write(tmp_path, _single_pod_sdf())
    without = load_local_metadata(db_path)
    explicit_none = load_local_metadata(db_path, period=None)
    pd.testing.assert_frame_equal(without, explicit_none)
    assert len(without) == 1


def test_null_temporal_range_never_matches_a_period(tmp_path):
    """A chunk ingested without per-point timestamps has a null range: it
    stays reachable spatially but cannot match any period."""
    sdf = _single_pod_sdf(with_timestamp=False)
    _root, db_path = _write(tmp_path, sdf)
    assert len(load_local_metadata(db_path)) == 1
    assert load_local_metadata(
        db_path, period=(CHUNK_START, CHUNK_END)).empty


def test_granule_date_filter_is_distinct_from_period(tmp_path):
    """start_date/end_date filter the granule-level 'RawData Collected Time'
    (filename-derived); period filters the data-level [t_start, t_end]. A
    period bracketing the data must match even when the same bounds, used as
    granule-level dates, do not."""
    # Granule stamped June 1; its data points collected June 15.
    _root, db_path = _write(tmp_path, _single_pod_sdf(),
                            raw_collected_time=datetime.datetime(2020, 6, 1))
    by_period = load_local_metadata(db_path, period=('2020-06-15', '2020-06-16'))
    by_granule_date = load_local_metadata(
        db_path, start_date='2020-06-15', end_date='2020-06-16')
    assert len(by_period) == 1
    assert by_granule_date.empty


# ----- demo intersection: temporal AND spatial -------------------------------


def test_demo_intersection_ands_spatial_and_temporal(tmp_path):
    demo, ca_sids = _two_pod_demo(tmp_path)

    # Spatial-only: the California pod's chunk.
    spatial_only = demo.find_intersecting_data(ca_sids, instruments=['GMI'])
    assert len(spatial_only) == 1
    assert spatial_only.iloc[0]['granule_name'] == 'G_CA'

    # Right place, right time → still the California chunk.
    both = demo.find_intersecting_data(
        ca_sids, instruments=['GMI'],
        period=(CHUNK_START - pd.Timedelta(hours=1),
                CHUNK_END + pd.Timedelta(hours=1)))
    assert len(both) == 1
    assert both.iloc[0]['granule_name'] == 'G_CA'

    # Right place, wrong time (the Australian chunk's temporal range) → nothing.
    wrong_time = demo.find_intersecting_data(
        ca_sids, instruments=['GMI'],
        period=(T0 + pd.Timedelta(days=1), T0 + pd.Timedelta(days=2)))
    assert wrong_time.empty


def test_demo_intersection_no_period_unchanged(tmp_path):
    demo, ca_sids = _two_pod_demo(tmp_path)
    without = demo.find_intersecting_data(ca_sids, instruments=['GMI'])
    explicit_none = demo.find_intersecting_data(ca_sids, instruments=['GMI'],
                                                period=None)
    assert list(without['granule_name']) == list(explicit_none['granule_name'])


def test_demo_intersection_period_with_no_spatial_filter(tmp_path):
    """location_sids=None (no spatial filter) still honors the period."""
    demo, _ca_sids = _two_pod_demo(tmp_path)
    df = demo.find_intersecting_data(
        None, instruments=['GMI'],
        period=(T0 + pd.Timedelta(days=1), T0 + pd.Timedelta(days=2)))
    assert len(df) == 1
    assert df.iloc[0]['granule_name'] == 'G_AU'


def test_cloud_demo_accepts_period_parameter():
    """The cloud demo's signature carries the same period parameter (its
    loader path is RDS-backed, covered by the shared predicate seam)."""
    sig = inspect.signature(StarePodsDemo.find_intersecting_data)
    assert 'period' in sig.parameters
    assert sig.parameters['period'].default is None


def test_demo_intersection_rejects_malformed_period(tmp_path):
    """A reversed period must raise, not be swallowed by the per-instrument
    error handler into an empty (indistinguishable-from-no-data) result."""
    demo, ca_sids = _two_pod_demo(tmp_path)
    with pytest.raises(ValueError):
        demo.find_intersecting_data(
            ca_sids, instruments=['GMI'],
            period=(T0 + pd.Timedelta(days=1), T0))


def test_cloud_demo_forwards_granule_dates_and_period(monkeypatch):
    """StarePodsDemo must pass start_date/end_date (granule-level) and
    period (data-level) through to load_s3_metadata — historically the date
    pair was captured into an unused time_range and silently dropped."""
    calls = []

    def fake_load_s3_metadata(**kw):
        calls.append(kw)
        return pd.DataFrame()

    monkeypatch.setattr(granules, 'load_s3_metadata', fake_load_s3_metadata)
    demo = StarePodsDemo()
    sids = pystare.from_latlon(np.array([37.0]), np.array([-120.0]), level=6)
    demo.find_intersecting_data(
        list(sids), instruments=['GMI'],
        start_date='2020-06-15', end_date='2020-06-16',
        period=(CHUNK_START, CHUNK_END))
    assert calls, "loader was never invoked"
    for kw in calls:
        assert kw['start_date'] == '2020-06-15'
        assert kw['end_date'] == '2020-06-16'
        assert kw['period'] == (CHUNK_START, CHUNK_END)

    # Legacy time_range shorthand maps onto the granule-level date pair.
    calls.clear()
    demo.find_intersecting_data(
        list(sids), instruments=['GMI'], time_range=('2020-06-15', '2020-06-16'))
    for kw in calls:
        assert kw['start_date'] == '2020-06-15'
        assert kw['end_date'] == '2020-06-16'


# ----- thin projection for the overlap analytics (ADR-0002 Decision 3) ------


def test_local_temporal_catalog_is_thin_and_parsed(tmp_path):
    demo, _ = _two_pod_demo(tmp_path)
    df = load_local_temporal_catalog(demo.db_path)

    assert list(df.columns) == ['podcode', 'Dataset', 't_start', 't_end']
    assert len(df) == 2
    assert df['podcode'].str.match(r'^q[01][0-3]+$').all()
    assert (df['Dataset'] == 'GMI_S1').all()
    # Parsed to timestamps, ready for the sweep kernel.
    assert pd.api.types.is_datetime64_any_dtype(df['t_start'])
    assert pd.api.types.is_datetime64_any_dtype(df['t_end'])
    assert (df['t_start'] <= df['t_end']).all()


def test_local_temporal_catalog_period_filter(tmp_path):
    demo, _ = _two_pod_demo(tmp_path)
    df = load_local_temporal_catalog(
        demo.db_path, period=(T0 + pd.Timedelta(days=1), T0 + pd.Timedelta(days=2)))
    assert len(df) == 1


def test_local_temporal_catalog_dataset_filters(tmp_path):
    demo, _ = _two_pod_demo(tmp_path)
    assert len(load_local_temporal_catalog(demo.db_path, dataset='GMI_S1')) == 2
    assert len(load_local_temporal_catalog(demo.db_path, dataset_prefix='GMI')) == 2
    assert load_local_temporal_catalog(demo.db_path, dataset='ATMS_S1').empty


def test_s3_temporal_catalog_never_touches_metadatajson():
    src = inspect.getsource(load_s3_temporal_catalog)
    assert 'MetadataJson' not in src
    assert 'SELECT podcode, "Dataset", t_start, t_end' in src


def test_finish_temporal_catalog_parses_mixed_precision_and_null():
    """Regression: a catalog mixing whole-second stamps (no fraction) with
    fractional ones — as real GMI/SSMIS ingests produce — must parse. A fixed
    ``.%f`` format inferred from the first row rejected the rest; ISO8601 does
    not. ``None`` (null-range chunks) parses to ``NaT``."""
    rows = [
        ('q013011', 'GMI_S1', '2025-01-01T04:36:55', '2025-01-01T04:36:57.123456'),
        ('q013012', 'GMI_S1', '2025-01-01T04:36:55.500000', '2025-01-01T04:37:00'),
        ('q013013', 'GMI_S1', None, None),
    ]
    df = _finish_temporal_catalog(rows)

    assert pd.api.types.is_datetime64_any_dtype(df['t_start'])
    assert pd.api.types.is_datetime64_any_dtype(df['t_end'])
    assert df['t_start'].iloc[0] == pd.Timestamp('2025-01-01T04:36:55')
    assert df['t_end'].iloc[0] == pd.Timestamp('2025-01-01T04:36:57.123456')
    assert pd.isna(df['t_start'].iloc[2]) and pd.isna(df['t_end'].iloc[2])
