"""Regression tests for the demo plot helpers.

The antimeridian case is the one that silently produced a *wrong picture*
rather than an error: ``make_trixels``' default ``wrap_lon=True`` normalises
every longitude into [-180, 180], so a trixel with corners at 178° and -179°
came back as a polygon spanning ~357° the wrong way round — which painted a
band clean across the world map at that trixel's latitude.
"""

import pandas as pd
import pytest

from starepandas.demo_plots import (
    pod_trixels, rendezvous_of_size, widest_rendezvous,
)
from starepandas.overlap import rendezvous_events

# Level-4 pods whose trixels straddle the antimeridian (their corners sit on
# both sides of ±180°), and one that does not.
ANTIMERIDIAN_PODS = ['q23233', 'q20123', 'q21133']
INTERIOR_POD = 'q03203'


def parts(geometry):
    return list(geometry.geoms) if geometry.geom_type == 'MultiPolygon' else [geometry]


def lon_span(geometry):
    """Widest longitude extent of any single part of a geometry."""
    return max(part.bounds[2] - part.bounds[0] for part in parts(geometry))


@pytest.mark.parametrize('podcode', ANTIMERIDIAN_PODS)
def test_antimeridian_pod_is_split_not_smeared(podcode):
    geometry = pod_trixels([podcode]).iloc[0]
    assert geometry.geom_type == 'MultiPolygon', "expected a split geometry"
    assert len(parts(geometry)) >= 2
    # Each half hugs one edge; neither may stretch across the map.
    assert lon_span(geometry) < 180


@pytest.mark.parametrize('podcode', ANTIMERIDIAN_PODS)
def test_antimeridian_pod_reaches_both_edges(podcode):
    """The two halves belong on opposite sides of the map, not in the middle."""
    bounds = [part.bounds for part in parts(pod_trixels([podcode]).iloc[0])]
    assert min(b[0] for b in bounds) <= -179.9
    assert max(b[2] for b in bounds) >= 179.9


def test_interior_pod_is_a_single_compact_polygon():
    geometry = pod_trixels([INTERIOR_POD]).iloc[0]
    assert lon_span(geometry) < 45
    assert geometry.area > 0


def test_pod_trixels_preserves_order_and_length():
    podcodes = [INTERIOR_POD] + ANTIMERIDIAN_PODS
    geometries = pod_trixels(podcodes)
    assert len(geometries) == len(podcodes)
    # Position 0 is the interior pod, so it must still be the compact one.
    assert lon_span(geometries.iloc[0]) < 45


def test_no_pod_of_a_realistic_set_spans_the_globe():
    """A whole octant's worth of level-4 pods, none of them smeared."""
    podcodes = [f'q2{a}{b}{c}{d}' for a in range(4) for b in range(4)
                for c in range(4) for d in range(4)]
    assert max(lon_span(g) for g in pod_trixels(podcodes)) < 180


def test_widest_rendezvous_picks_the_widest_and_its_completion():
    catalog = pd.DataFrame({
        'podcode': ['q03200'] * 3 + ['q03201'] * 2,
        'Dataset': ['GMI_S1', 'SSMIS_S1', 'ATMS_S1', 'GMI_S1', 'SSMIS_S1'],
        't_start': pd.to_datetime([
            '2025-01-01 00:00', '2025-01-01 00:10', '2025-01-01 00:20',
            '2025-01-01 02:00', '2025-01-01 02:10',
        ]),
        't_end': pd.to_datetime([
            '2025-01-01 00:01', '2025-01-01 00:11', '2025-01-01 00:21',
            '2025-01-01 02:01', '2025-01-01 02:11',
        ]),
    })
    events = rendezvous_events(catalog, pd.Timedelta(minutes=45))
    podcode, meeting, n_way = widest_rendezvous(events)
    assert (podcode, n_way) == ('q03200', 3)
    # The rendezvous completes when the last of the three arrives.
    assert meeting == pd.Timestamp('2025-01-01 00:20')


def test_widest_rendezvous_rejects_an_empty_events_frame():
    empty = rendezvous_events(
        pd.DataFrame({'podcode': [], 'Dataset': [],
                      't_start': pd.to_datetime([]), 't_end': pd.to_datetime([])}),
        pd.Timedelta(minutes=45),
    )
    with pytest.raises(ValueError, match='no rendezvous events'):
        widest_rendezvous(empty)


# ----- picking a representative pod of a given width -------------------------

def _mixed_catalog():
    """One pod with a genuine 3-way, two pods with only pairs."""
    rows = [
        # q00000 — three instruments, all within the window.
        ('q00000', 'GMI_S1', '00:00'), ('q00000', 'SSMIS_S1', '00:05'),
        ('q00000', 'ATMS_S1', '00:10'),
        # q00001 / q00002 — pairs only.
        ('q00001', 'GMI_S1', '00:00'), ('q00001', 'SSMIS_S1', '00:05'),
        ('q00002', 'GMI_S1', '00:00'), ('q00002', 'AMSR2_S1', '00:05'),
    ]
    return pd.DataFrame({
        'podcode': [r[0] for r in rows],
        'Dataset': [r[1] for r in rows],
        't_start': pd.to_datetime([f'2025-01-01 {r[2]}' for r in rows]),
        't_end': pd.to_datetime([f'2025-01-01 {r[2]}' for r in rows])
                 + pd.Timedelta(seconds=30),
    })


def _events():
    return rendezvous_events(_mixed_catalog(), pd.Timedelta(minutes=45))


def test_size_two_never_returns_a_pod_that_also_holds_a_trio():
    """The whole point of 'exactly n' — else a 2-way is drawn with 3 swaths."""
    podcode, _meeting, n_way = rendezvous_of_size(_events(), 2)
    assert n_way == 2
    assert podcode in {'q00001', 'q00002'}


def test_size_three_finds_the_trio_pod():
    podcode, meeting, n_way = rendezvous_of_size(_events(), 3)
    assert (podcode, n_way) == ('q00000', 3)
    assert meeting == pd.Timestamp('2025-01-01 00:10')


def test_widest_agrees_with_an_explicit_size_request():
    assert widest_rendezvous(_events()) == rendezvous_of_size(_events(), 3)


def test_absent_width_raises():
    with pytest.raises(ValueError, match='exactly 4'):
        rendezvous_of_size(_events(), 4)


def test_selection_is_deterministic():
    """A demo re-run must tell the same story, so the tie-break is stable."""
    picks = {rendezvous_of_size(_events(), 2) for _ in range(5)}
    assert len(picks) == 1


def test_metadata_breaks_the_tie_towards_the_larger_footprint():
    """Without footprints the pair pods tie; num_rows must decide."""
    metadata = pd.DataFrame({
        'podcode': ['q00001', 'q00001', 'q00002', 'q00002'],
        'Dataset': ['GMI_S1', 'SSMIS_S1', 'GMI_S1', 'AMSR2_S1'],
        # q00002's smallest participant (900) beats q00001's (10).
        'num_rows': [5000, 10, 5000, 900],
    })
    podcode, _meeting, n_way = rendezvous_of_size(_events(), 2, metadata=metadata)
    assert (podcode, n_way) == ('q00002', 2)
