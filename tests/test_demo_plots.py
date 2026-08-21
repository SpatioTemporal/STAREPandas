"""Regression tests for the demo plot helpers.

The antimeridian case is the one that silently produced a *wrong picture*
rather than an error: ``make_trixels``' default ``wrap_lon=True`` normalises
every longitude into [-180, 180], so a trixel with corners at 178° and -179°
came back as a polygon spanning ~357° the wrong way round — which painted a
band clean across the world map at that trixel's latitude.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import pytest

from starepandas.demo_plots import (
    plot_pod_coverage, pod_trixels, rendezvous_examples, rendezvous_of_size,
    widest_rendezvous,
)
from starepandas.overlap import rendezvous_events

# Level-4 pods whose trixels straddle the antimeridian (their corners sit on
# both sides of ±180°), and one that does not.
ANTIMERIDIAN_PODS = ['q023233', 'q020123', 'q021133']
INTERIOR_POD = 'q003203'


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
    podcodes = [f'q02{a}{b}{c}{d}' for a in range(4) for b in range(4)
                for c in range(4) for d in range(4)]
    assert max(lon_span(g) for g in pod_trixels(podcodes)) < 180


def test_widest_rendezvous_picks_the_widest_and_its_completion():
    catalog = pd.DataFrame({
        'podcode': ['q003200'] * 3 + ['q003201'] * 2,
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
    assert (podcode, n_way) == ('q003200', 3)
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

def _catalog(rows):
    """Temporal catalog from ``(podcode, Dataset, 'HH:MM')`` triples."""
    return pd.DataFrame({
        'podcode': [r[0] for r in rows],
        'Dataset': [r[1] for r in rows],
        't_start': pd.to_datetime([f'2025-01-01 {r[2]}' for r in rows]),
        't_end': pd.to_datetime([f'2025-01-01 {r[2]}' for r in rows])
                 + pd.Timedelta(seconds=30),
    })


def _mixed_catalog():
    """One pod with a genuine 3-way, two pods with only pairs."""
    return _catalog([
        # q000000 — three instruments, all within the window.
        ('q000000', 'GMI_S1', '00:00'), ('q000000', 'SSMIS_S1', '00:05'),
        ('q000000', 'ATMS_S1', '00:10'),
        # q000001 / q000002 — pairs only.
        ('q000001', 'GMI_S1', '00:00'), ('q000001', 'SSMIS_S1', '00:05'),
        ('q000002', 'GMI_S1', '00:00'), ('q000002', 'AMSR2_S1', '00:05'),
    ])


def _events():
    return rendezvous_events(_mixed_catalog(), pd.Timedelta(minutes=45))


def test_size_two_never_returns_a_pod_that_also_holds_a_trio():
    """The whole point of 'exactly n' — else a 2-way is drawn with 3 swaths."""
    podcode, _meeting, n_way = rendezvous_of_size(_events(), 2)
    assert n_way == 2
    assert podcode in {'q000001', 'q000002'}


def test_size_three_finds_the_trio_pod():
    podcode, meeting, n_way = rendezvous_of_size(_events(), 3)
    assert (podcode, n_way) == ('q000000', 3)
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
        'podcode': ['q000001', 'q000001', 'q000002', 'q000002'],
        'Dataset': ['GMI_S1', 'SSMIS_S1', 'GMI_S1', 'AMSR2_S1'],
        # q000002's smallest participant (900) beats q000001's (10).
        'num_rows': [5000, 10, 5000, 900],
    })
    podcode, _meeting, n_way = rendezvous_of_size(_events(), 2, metadata=metadata)
    assert (podcode, n_way) == ('q000002', 2)


# ----- several examples of one width -----------------------------------------

def _repeated_combination_events():
    """Two pods hold GMI+SSMIS, a third holds GMI+AMSR2."""
    return rendezvous_events(_catalog([
        ('q000001', 'GMI_S1', '00:00'), ('q000001', 'SSMIS_S1', '00:05'),
        ('q000003', 'GMI_S1', '00:00'), ('q000003', 'SSMIS_S1', '00:05'),
        ('q000002', 'GMI_S1', '00:00'), ('q000002', 'AMSR2_S1', '00:05'),
    ]), pd.Timedelta(minutes=45))


def _footprints(amsr2_pixels=1000):
    """q000003 is the most legible GMI+SSMIS pod; q000002 the only GMI+AMSR2."""
    return pd.DataFrame({
        'podcode': ['q000001', 'q000001', 'q000003', 'q000003', 'q000002', 'q000002'],
        'Dataset': ['GMI_S1', 'SSMIS_S1', 'GMI_S1', 'SSMIS_S1',
                    'GMI_S1', 'AMSR2_S1'],
        'num_rows': [5000, 3000, 5000, 4000, 5000, amsr2_pixels],
    })


def test_examples_prefers_a_second_combination_over_a_second_pod():
    """Three pods of the same pair tell one story three times."""
    picked = rendezvous_examples(_repeated_combination_events(), 2, count=2,
                                 metadata=_footprints())
    # Ranked by legibility alone this would be q000003 then q000001 — both
    # GMI+SSMIS. The lower-scoring q000002 wins its place by being a *different*
    # pair.
    assert [p for p, _, _ in picked] == ['q000003', 'q000002']


def test_examples_returns_every_pod_when_count_exceeds_the_combinations():
    picked = rendezvous_examples(_repeated_combination_events(), 2, count=3,
                                 metadata=_footprints())
    assert [p for p, _, _ in picked] == ['q000003', 'q000002', 'q000001']


def test_examples_never_returns_more_than_the_catalog_holds():
    """A demo asking for three when two exist shows two, it does not break."""
    picked = rendezvous_examples(_repeated_combination_events(), 2, count=99,
                                 metadata=_footprints())
    assert len(picked) == 3


def test_a_sliver_pod_is_held_back_but_used_rather_than_returning_fewer():
    """The legibility floor ranks examples; it must not shrink the answer."""
    slivered = _footprints(amsr2_pixels=10)   # q000002's AMSR2 barely clips
    two = rendezvous_examples(_repeated_combination_events(), 2, count=2,
                              metadata=slivered)
    assert [p for p, _, _ in two] == ['q000003', 'q000001'], \
        "the sliver pod must lose to a repeat of a legible combination"
    three = rendezvous_examples(_repeated_combination_events(), 2, count=3,
                                metadata=slivered)
    assert [p for p, _, _ in three] == ['q000003', 'q000001', 'q000002'], \
        "but it is still better than showing only two examples"


def test_every_example_names_a_real_event_in_its_pod():
    """The meeting time must be an instant that width actually rendezvoused."""
    events = _events()
    for podcode, meeting, n_way in rendezvous_examples(events, 2, count=2):
        matching = events[(events['podcode'] == podcode)
                          & (events['time'] == meeting)
                          & (events['instruments'].map(len) == n_way)]
        assert not matching.empty


def test_window_counts_only_the_chunks_present_at_the_rendezvous():
    """``[t_start, t_end]`` is an envelope, not a pass.

    A granule crossing one pod twice stores both passes in a single chunk
    spanning the gap between them, so counting such a chunk in full credits
    an instrument with pixels it did not bring to *this* meeting. That is how
    a 21-pixel sliver used to be picked as a good example.
    """
    events = _repeated_combination_events()          # every meeting at 00:05
    metadata = _footprints()
    # q000002's AMSR2 chunk spans four hours around the meeting; only a
    # sliver of it is anywhere near. The others are tight on the meeting.
    metadata['t_start'] = ['2025-01-01 00:00'] * 4 + ['2025-01-01 00:00',
                                                      '2025-01-01 00:00']
    metadata['t_end'] = ['2025-01-01 00:06'] * 4 + ['2025-01-01 00:06',
                                                    '2025-01-01 04:00']
    window = pd.Timedelta(minutes=6)

    unscoped = rendezvous_examples(events, 2, count=1, metadata=metadata)
    scoped = rendezvous_examples(events, 2, count=1, metadata=metadata,
                                 window=window)
    # Unscoped, q000002 is a legible alternative pair and leads the round-robin
    # of its own combination; scoped, its 1000 rows shrink to a sliver.
    assert unscoped[0][0] == 'q000003'
    assert scoped[0][0] == 'q000003'
    two_scoped = [p for p, _, _ in rendezvous_examples(
        events, 2, count=2, metadata=metadata, window=window)]
    assert two_scoped == ['q000003', 'q000001'], \
        "the long-envelope pod must lose its place to a legible repeat"


def test_window_scoping_is_skipped_when_the_catalog_has_no_timestamps():
    """Metadata without t_start/t_end still ranks, it does not blow up."""
    picked = rendezvous_examples(_repeated_combination_events(), 2, count=2,
                                 metadata=_footprints(),
                                 window=pd.Timedelta(minutes=6))
    assert [p for p, _, _ in picked] == ['q000003', 'q000002']


def test_examples_and_the_single_pod_helper_agree():
    events = _repeated_combination_events()
    metadata = _footprints()
    assert (rendezvous_examples(events, 2, count=1, metadata=metadata)[0]
            == rendezvous_of_size(events, 2, metadata=metadata))


def test_examples_raises_for_an_absent_width():
    with pytest.raises(ValueError, match='exactly 4'):
        rendezvous_examples(_events(), 4, count=2)


# ── plot_pod_coverage highlight groups ───────────────────────────────────────

def _tiny_catalog():
    return pd.DataFrame({
        'podcode': ['q003200', 'q003203', 'q123333'],
        'Dataset': ['GMI_S1', 'GMI_S1', 'SSMIS_S1'],
    })


def test_coverage_highlight_accepts_a_plain_pod_list():
    """The pre-existing form — a list of pod codes — still outlines in black."""
    fig = plot_pod_coverage(_tiny_catalog(), highlight=['q003200'])
    assert 'black = q003200' in fig._suptitle.get_text()
    plt.close(fig)


def test_coverage_highlight_accepts_labelled_color_groups():
    fig = plot_pod_coverage(_tiny_catalog(), highlight=[
        (['q003200', 'q003203'], 'black', '4-way'),
        (['q123333'], 'gold', '3-way examples'),
        ([], 'darkgreen', '2-way examples'),   # empty group: skipped entirely
    ])
    text = fig._suptitle.get_text()
    assert 'black = 4-way: q003200, q003203' in text
    assert 'gold = 3-way examples: q123333' in text
    assert 'darkgreen' not in text
    plt.close(fig)
