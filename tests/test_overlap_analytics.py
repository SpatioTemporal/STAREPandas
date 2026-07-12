"""Multi-instrument overlap analytics (temporal-stare-pods issue 05).

Verifies the slide-8/9 analytics: given an already-loaded, period-filtered
temporal-catalog frame and a Δt window, which instruments achieved a
**simultaneous rendezvous** (ADR-0001) in which pods.

Seams:
- the **pure** functions over a fixture temporal-catalog DataFrame (highest
  seam, no database) — the frame shape mirrors what
  ``load_s3_temporal_catalog`` / ``load_local_temporal_catalog`` return;
- the **local write → SQLite round-trip** only to prove the analytics never
  touch the catalog again once the frame is loaded (the database is deleted
  before the analytics run).

Semantics pinned here (ADR-0001 + ADR-0002):
- a *pass* is one chunk's ``[t_start, t_end]``; the Δt test is pass-by-pass;
- an n-instrument overlap needs all n mutually within Δt **at once**
  (Helly sweep: ``max(starts) <= min(ends) + Δt``) — a "fake triangle"
  (each pair coincided at different times) is NOT a 3-way overlap;
- headline cells count distinct instrument-combinations per pod (deduped);
  frequency and timestamps live in the drill-downs;
- scan-group datasets (``GMI_S1``/``GMI_S2``) fold to one instrument.
"""

import os
import time

import numpy as np
import pandas as pd
import pytest

from starepandas.overlap import (
    EVENT_COLUMNS,
    fold_instrument,
    overlap_matrix,
    overlap_pod_table,
    pair_drilldown,
    pod_drilldown,
    rendezvous_events,
)
from tests._temporal_fixtures import T0

ZERO = pd.Timedelta(0)


def _t(hours):
    return T0 + pd.Timedelta(hours=hours)


def _catalog(rows):
    """rows = (podcode, dataset, start_hours, end_hours) — temporal-catalog
    shape as returned by the issue-03 thin loaders."""
    return pd.DataFrame(
        [{'podcode': p, 'Dataset': d,
          't_start': _t(h0) if h0 is not None else pd.NaT,
          't_end': _t(h1) if h1 is not None else pd.NaT}
         for p, d, h0, h1 in rows],
        columns=['podcode', 'Dataset', 't_start', 't_end'])


# Pod q13211: a genuine 3-way rendezvous — G, S, A all active at hour 2.
# Pod q20123: a fake triangle — each pair touches, the trio never coincides.
GENUINE_AND_FAKE = _catalog([
    ('q13211', 'GMI_S1',   0, 2),
    ('q13211', 'SSMIS_S1', 1, 3),
    ('q13211', 'ATMS_S1',  2, 4),
    ('q20123', 'GMI_S1',   10, 11),
    ('q20123', 'SSMIS_S1', 11, 12),
    ('q20123', 'SSMIS_S1', 20, 21),
    ('q20123', 'ATMS_S1',  21, 22),
    ('q20123', 'ATMS_S1',  30, 31),
    ('q20123', 'GMI_S1',   31, 32),
])


# ----- instrument folding -----------------------------------------------------


def test_fold_instrument_strips_scan_group_suffix():
    assert fold_instrument('GMI_S1') == 'GMI'
    assert fold_instrument('GMI_S2') == 'GMI'
    assert fold_instrument('SSMIS_S12') == 'SSMIS'
    assert fold_instrument('AMSR2') == 'AMSR2'         # no suffix → unchanged
    assert fold_instrument('data_S1') == 'data'


def test_scan_groups_of_one_instrument_never_rendezvous_with_themselves():
    """Folding is labeling only, but a rendezvous needs >= 2 *distinct*
    instruments — two co-active scan groups of GMI are not an overlap."""
    catalog = _catalog([
        ('q13211', 'GMI_S1', 0, 2),
        ('q13211', 'GMI_S2', 1, 3),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    assert events.empty

    catalog = _catalog([
        ('q13211', 'GMI_S1', 0, 2),
        ('q13211', 'GMI_S2', 1, 3),
        ('q13211', 'SSMIS_S3', 1, 2),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    assert set(events['instruments']) == {('GMI', 'SSMIS')}
    table = overlap_pod_table(events)
    assert table.loc['q13211', 2] == 1                  # one pair, deduped
    assert 3 not in table.columns                       # never a fake trio


# ----- the sweep kernel -------------------------------------------------------


def test_events_schema_and_rendezvous_moments():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    assert list(events.columns) == EVENT_COLUMNS
    assert pd.api.types.is_datetime64_any_dtype(events['time'])

    genuine = events[events['podcode'] == 'q13211']
    # +SSMIS at hour 1 (mask {G,S}), +ATMS at hour 2 (mask {G,S,A}).
    assert list(genuine['added']) == ['SSMIS', 'ATMS']
    assert list(genuine['time']) == [_t(1), _t(2)]
    assert list(genuine['instruments']) == [
        ('GMI', 'SSMIS'), ('ATMS', 'GMI', 'SSMIS')]


def test_fake_triangle_never_reaches_three_instruments():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    fake = events[events['podcode'] == 'q20123']
    assert len(fake) == 3                               # three pair touches
    assert all(len(mask) == 2 for mask in fake['instruments'])


def test_pod_table_counts_genuine_trio_but_not_fake_triangle():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    table = overlap_pod_table(events)

    # Genuine pod: 3 distinct pairs + 1 distinct trio (subset expansion).
    assert table.loc['q13211', 2] == 3
    assert table.loc['q13211', 3] == 1
    # Fake triangle: all 3 pairs, but NO trio — the decisive case.
    assert table.loc['q20123', 2] == 3
    assert table.loc['q20123', 3] == 0


def test_multiple_passes_and_actives_decrement_correctly():
    """A chain G–S–A where G is gone before A arrives must yield the two
    chain pairs only — no (G, A), no trio."""
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 2),
        ('q13211', 'SSMIS_S1', 1, 5),
        ('q13211', 'ATMS_S1',  4, 6),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    assert list(events['instruments']) == [
        ('GMI', 'SSMIS'), ('ATMS', 'SSMIS')]

    # Same-instrument passes keep the instrument active until the LAST one
    # ends: A2 bridges over A1's end, so +B at 2.5 still sees GMI active.
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 2),
        ('q13211', 'GMI_S2',   1, 3),
        ('q13211', 'SSMIS_S1', 2.5, 4),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    assert list(events['instruments']) == [('GMI', 'SSMIS')]


def test_dt_is_a_query_parameter():
    """A 1 h gap between passes: invisible at Δt=0, a rendezvous at Δt=1 h
    (closed boundary), computed from the same loaded frame."""
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 1),
        ('q13211', 'SSMIS_S1', 2, 3),
    ])
    assert rendezvous_events(catalog, dt=ZERO).empty
    assert rendezvous_events(catalog, dt=pd.Timedelta(minutes=59)).empty
    events = rendezvous_events(catalog, dt=pd.Timedelta(hours=1))
    assert list(events['instruments']) == [('GMI', 'SSMIS')]

    with pytest.raises(ValueError):
        rendezvous_events(catalog, dt=pd.Timedelta(hours=-1))
    # Bare numbers are ambiguous (pd.Timedelta would read them as
    # nanoseconds) — Python and numpy scalars alike must be rejected.
    for ambiguous in (1800, 1800.0, np.int64(1800), np.float64(1800.0)):
        with pytest.raises(ValueError):
            rendezvous_events(catalog, dt=ambiguous)


def test_period_is_a_query_parameter():
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 2),
        ('q13211', 'SSMIS_S1', 1, 3),
        ('q13211', 'ATMS_S1',  2, 4),
    ])
    # Excluding the ATMS pass leaves only the G–S pair.
    events = rendezvous_events(catalog, dt=ZERO, period=(_t(0), _t(1.5)))
    assert set(events['instruments']) == {('GMI', 'SSMIS')}
    # A pass overlapping the period boundary still counts (closed overlap).
    events = rendezvous_events(catalog, dt=ZERO, period=(_t(3.5), _t(9)))
    assert events.empty
    with pytest.raises(ValueError):
        rendezvous_events(catalog, dt=ZERO, period=(_t(2), _t(1)))


def test_null_range_chunks_are_not_passes():
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 2),
        ('q13211', 'SSMIS_S1', None, None),
        ('q13211', 'ATMS_S1',  1, 3),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    assert set(events['instruments']) == {('ATMS', 'GMI')}


def test_null_podcode_raises():
    catalog = GENUINE_AND_FAKE.copy()
    catalog.loc[0, 'podcode'] = None
    with pytest.raises(ValueError, match='podcode'):
        rendezvous_events(catalog, dt=ZERO)


def test_tz_aware_catalog_normalizes_to_naive_utc():
    """A catalog parsed with utc=True must behave exactly like the loaders'
    naive-UTC frames — including under a period filter."""
    catalog = GENUINE_AND_FAKE.copy()
    for col in ('t_start', 't_end'):
        catalog[col] = catalog[col].dt.tz_localize('UTC')
    naive = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    aware = rendezvous_events(catalog, dt=ZERO)
    pd.testing.assert_frame_equal(aware, naive)

    period = (_t(0), _t(1.5))
    pd.testing.assert_frame_equal(
        rendezvous_events(catalog, dt=ZERO, period=period),
        rendezvous_events(GENUINE_AND_FAKE, dt=ZERO, period=period))


def test_empty_catalog_yields_empty_but_typed_outputs():
    """Empty results carry the same schema and dtypes as populated ones, so
    downstream concat/arithmetic never sees an object-dtype surprise."""
    empty = GENUINE_AND_FAKE.iloc[0:0]
    events = rendezvous_events(empty, dt=ZERO)
    assert events.empty
    assert list(events.columns) == EVENT_COLUMNS
    assert pd.api.types.is_datetime64_any_dtype(events['time'])

    table = overlap_pod_table(events)
    assert table.empty
    assert table[2].sum() == 0                          # n=2 column always there
    assert table[2].dtype == 'int64'
    matrix = overlap_matrix(events, instruments=['GMI', 'SSMIS'])
    assert (matrix.to_numpy() == 0).all()
    assert overlap_matrix(events).empty                 # no observed universe
    for drill in (pair_drilldown(events, 'GMI', 'SSMIS'),
                  pod_drilldown(events, 'q13211')):
        assert drill.empty
        assert drill['frequency'].dtype == 'int64'


# ----- slide 8: the instrument×instrument matrix ------------------------------


def test_matrix_counts_pods_per_pair():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    matrix = overlap_matrix(events)

    assert list(matrix.index) == list(matrix.columns) == \
        ['ATMS', 'GMI', 'SSMIS']
    # Every pair rendezvous in both pods (genuine trio + fake triangle edges).
    for a, b in (('GMI', 'SSMIS'), ('ATMS', 'GMI'), ('ATMS', 'SSMIS')):
        assert matrix.loc[a, b] == 2
        assert matrix.loc[b, a] == 2                     # symmetric
    assert (matrix.to_numpy().diagonal() == 0).all()


def test_matrix_and_pod_table_agree_for_pairs():
    """Acceptance: for n=2 the two headline views count the same
    (pod, pair) incidences."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    matrix = overlap_matrix(events)
    table = overlap_pod_table(events)

    total_incidences = matrix.to_numpy().sum() // 2      # symmetric halves
    assert total_incidences == table[2].sum()

    # And per pod: the drill-down's pairs are the pod table's n=2 count.
    for pod in table.index:
        drill = pod_drilldown(events, pod)
        assert table.loc[pod, 2] == (drill['n_instruments'] == 2).sum()


def test_matrix_universe_can_include_silent_instruments():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    matrix = overlap_matrix(events,
                            instruments=['AMSR2', 'ATMS', 'GMI', 'SSMIS'])
    assert list(matrix.index) == ['AMSR2', 'ATMS', 'GMI', 'SSMIS']
    assert (matrix.loc['AMSR2'] == 0).all()


def test_matrix_region_rescope_is_a_pod_subset_no_resweep():
    """Acceptance: restricting to a pod subset of the loaded frame works on
    the events already computed (bitmap AND — never a re-sweep/re-query)."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)

    scoped = overlap_matrix(events, pods=['q13211'])
    assert scoped.loc['GMI', 'SSMIS'] == 1
    # A coarser pod code covers its whole subtree (prefix semantics).
    assert overlap_matrix(events, pods=['q1']).loc['GMI', 'SSMIS'] == 1
    assert overlap_matrix(events, pods=['q2']).loc['GMI', 'SSMIS'] == 1
    both = overlap_matrix(events, pods=['q1', 'q2'])
    assert both.loc['GMI', 'SSMIS'] == 2
    # Outside the loaded pods → all-zero cells, not an error.
    assert (overlap_matrix(events, pods=['q3']).to_numpy() == 0).all()

    with pytest.raises(ValueError):
        overlap_matrix(events, pods=['not-a-podcode'])
    # A cover finer than the partition level could never match a cataloged
    # pod — it must raise, not silently report an all-zero region.
    with pytest.raises(ValueError, match='finer'):
        overlap_matrix(events, pods=['q1321101'])


def test_instrument_arguments_must_be_folded_names():
    """A raw dataset name would silently match nothing in the events — the
    analytics label by instrument, so scan-group names raise."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    with pytest.raises(ValueError, match='fold'):
        pair_drilldown(events, 'GMI_S1', 'SSMIS')
    with pytest.raises(ValueError, match='fold'):
        overlap_matrix(events, instruments=['GMI_S1', 'SSMIS'])


# ----- drill-downs ------------------------------------------------------------


def test_pair_drilldown_reports_per_pod_frequency_and_times():
    """One long GMI pass rendezvousing with two successive SSMIS passes →
    frequency 2 with the two rendezvous moments."""
    catalog = _catalog([
        ('q13211', 'GMI_S1',   0, 10),
        ('q13211', 'SSMIS_S1', 1, 2),
        ('q13211', 'SSMIS_S1', 5, 6),
        ('q20123', 'GMI_S1',   0, 1),
        ('q20123', 'SSMIS_S1', 0.5, 2),
    ])
    events = rendezvous_events(catalog, dt=ZERO)
    drill = pair_drilldown(events, 'SSMIS', 'GMI')       # order-insensitive

    assert list(drill.columns) == ['podcode', 'frequency', 'times']
    assert list(drill['podcode']) == ['q13211', 'q20123']
    q13211 = drill.set_index('podcode').loc['q13211']
    assert q13211['frequency'] == 2
    assert q13211['times'] == [_t(1), _t(5)]

    with pytest.raises(ValueError):
        pair_drilldown(events, 'GMI', 'GMI')


def test_pair_drilldown_counts_pair_inside_larger_rendezvous():
    """A pair co-active only inside a 3-way rendezvous still shows up in the
    pair's drill-down (subsets of a recorded mask rendezvoused too)."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    drill = pair_drilldown(events, 'ATMS', 'GMI').set_index('podcode')
    # In q13211, (ATMS, GMI) only ever coincide within the trio at hour 2.
    assert drill.loc['q13211', 'frequency'] == 1
    assert drill.loc['q13211', 'times'] == [_t(2)]


def test_pod_drilldown_reports_combinations_frequency_and_times():
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    drill = pod_drilldown(events, 'q13211')

    assert list(drill.columns) == [
        'instruments', 'n_instruments', 'frequency', 'times']
    by_combo = {row.instruments: row for row in drill.itertuples(index=False)}
    assert set(by_combo) == {
        ('GMI', 'SSMIS'), ('ATMS', 'GMI'), ('ATMS', 'SSMIS'),
        ('ATMS', 'GMI', 'SSMIS')}
    pair = by_combo[('GMI', 'SSMIS')]
    assert pair.frequency == 1
    assert pair.times == [_t(1)]
    trio = by_combo[('ATMS', 'GMI', 'SSMIS')]
    assert trio.n_instruments == 3
    assert trio.frequency == 1
    assert trio.times == [_t(2)]

    fake = pod_drilldown(events, 'q20123')
    assert (fake['n_instruments'] == 2).all()            # no trio, ever

    with pytest.raises(ValueError):
        pod_drilldown(events, 'not-a-podcode')
    with pytest.raises(ValueError, match='finer'):
        pod_drilldown(events, 'q1321101')                # finer than partition


def test_pod_drilldown_coarser_code_rolls_up_the_subtree():
    """Same cover semantics as the matrix's pods=: a coarser pod code
    aggregates every loaded pod beneath it."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO)
    subtree = pod_drilldown(events, 'q1')
    pd.testing.assert_frame_equal(subtree, pod_drilldown(events, 'q13211'))
    assert pod_drilldown(events, 'q3').empty


def test_include_passes_lists_the_participating_passes():
    """ADR-0002's canonical events table carries the participating passes:
    per event, the catalog index labels of every pass active at that moment."""
    events = rendezvous_events(GENUINE_AND_FAKE, dt=ZERO, include_passes=True)
    assert list(events.columns) == EVENT_COLUMNS + ['passes']

    genuine = events[events['podcode'] == 'q13211']
    # Rows 0–2 of the fixture are the genuine trio's passes (G, S, A).
    assert list(genuine['passes']) == [(0, 1), (0, 1, 2)]
    fake = events[events['podcode'] == 'q20123']
    assert all(len(p) == 2 for p in fake['passes'])

    # Labels are the caller's catalog index, not positions.
    relabeled = GENUINE_AND_FAKE.set_axis(GENUINE_AND_FAKE.index + 100)
    events = rendezvous_events(relabeled, dt=ZERO, include_passes=True)
    assert list(events[events['podcode'] == 'q13211']['passes']) == \
        [(100, 101), (100, 101, 102)]

    # Duplicate index labels cannot identify passes — refuse them rather
    # than emit ambiguous participants like (0, 0).
    doubled = pd.concat([GENUINE_AND_FAKE, GENUINE_AND_FAKE])
    with pytest.raises(ValueError, match='index'):
        rendezvous_events(doubled, dt=ZERO, include_passes=True)
    # Without include_passes the index is irrelevant and duplicates are fine.
    assert not rendezvous_events(doubled, dt=ZERO).empty


# ----- no catalog access after the load (local round-trip) --------------------


def test_analytics_run_on_the_loaded_frame_after_the_catalog_is_gone(tmp_path):
    """ADR-0002 Decision 2 as behavior: load the thin frame once, DELETE the
    database, then re-run with two different Δt and take both drill-downs —
    everything must still work (pure functions over the loaded frame)."""
    from starepandas.io.granules import load_local_temporal_catalog
    from tests._temporal_fixtures import CHUNK_END, single_pod_sdf, write_local

    sdf = single_pod_sdf()                              # [T0, T0 + 30m]
    root, db_path = write_local(tmp_path, sdf, dataset='GMI_S1')
    # Second instrument passes 15 minutes after the first one ends.
    late = single_pod_sdf(t_start=CHUNK_END + pd.Timedelta(minutes=15),
                          t_end=CHUNK_END + pd.Timedelta(minutes=30))
    late.to_local(root, level=6, db_path=db_path, dataset='SSMIS_S1',
                  granule_name='G2',
                  raw_collected_time=pd.Timestamp('2020-06-15').to_pydatetime())

    catalog = load_local_temporal_catalog(db_path)
    os.remove(db_path)                                  # no catalog left

    # Δt below the 15-minute gap: no rendezvous; above it: one pair.
    assert rendezvous_events(catalog, dt=pd.Timedelta(minutes=5)).empty
    events = rendezvous_events(catalog, dt=pd.Timedelta(minutes=20))
    assert list(events['instruments']) == [('GMI', 'SSMIS')]

    pod = events['podcode'].iloc[0]
    assert not pair_drilldown(events, 'GMI', 'SSMIS').empty
    assert not pod_drilldown(events, pod).empty
    assert overlap_matrix(events).loc['GMI', 'SSMIS'] == 1


# ----- performance smoke (guards a combinatorial regression) ------------------


def test_performance_smoke_100k_rows():
    """~100k passes through the sweep + both headline views. The budget is a
    generous CI-stable bound (the target is tens of milliseconds); a
    combinatorial subset search or quadratic sweep blows it by orders of
    magnitude."""
    pods = [f'q{o}{a}{b}{c}0'
            for o in range(8) for a in range(4) for b in range(4)
            for c in range(2)]                          # 8*4*4*2 = 256 pods
    instruments = ['GMI_S1', 'SSMIS_S1', 'ATMS_S1', 'AMSR2_S1']
    n_passes = 98                                       # 256*4*98 ≈ 100k rows
    frames = []
    for k, inst in enumerate(instruments):
        start = (np.arange(n_passes * len(pods)) % n_passes) * 3600.0 * 3 \
            + k * 600.0
        frames.append(pd.DataFrame({
            'podcode': np.repeat(pods, n_passes),
            'Dataset': inst,
            't_start': T0 + pd.to_timedelta(start, unit='s'),
            't_end': T0 + pd.to_timedelta(start + 3600.0, unit='s'),
        }))
    catalog = pd.concat(frames, ignore_index=True)
    assert len(catalog) > 100_000 * 0.99

    tic = time.perf_counter()
    events = rendezvous_events(catalog, dt=pd.Timedelta(minutes=30))
    matrix = overlap_matrix(events)
    table = overlap_pod_table(events)
    elapsed = time.perf_counter() - tic

    assert not events.empty
    assert matrix.loc['GMI', 'SSMIS'] == len(pods)      # everywhere, by design
    assert table[4].gt(0).all()                          # 4-way in every pod
    assert elapsed < 1.0, f"analytics took {elapsed:.3f}s on 100k rows"
