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


class _StubDemo:
    """Stands in for StarePodsDemo: hands back canned per-dataset chunks."""

    def __init__(self, chunks):
        self._chunks = chunks

    def download_and_analyze(self, rows):
        return self._chunks


def _pod_pixels_fixture():
    stamps = pd.to_datetime(['2025-01-01 00:01', '2025-01-01 00:02',
                             '2025-01-01 03:00'])
    chunk = pd.DataFrame({'lat': [0.0, 1.0, 2.0], 'lon': [0.0, 1.0, 2.0],
                          'timestamp': stamps})
    metadata = pd.DataFrame({'group_path': ['store/q000001-granule-GMI_S1.parquet',
                                            'store/q000001-granule-GMI_S2.parquet']})
    demo = _StubDemo({'GMI_S1': chunk, 'GMI_S2': chunk.iloc[:2]})
    window = (pd.Timestamp('2025-01-01 00:00'), pd.Timestamp('2025-01-01 00:10'))
    return demo, metadata, window


def test_pod_pixels_folds_scan_groups_by_default():
    from starepandas.demo_plots import pod_pixels
    demo, metadata, window = _pod_pixels_fixture()
    folded = pod_pixels(demo, metadata, 'q000001', window)
    # one merged instrument entry; the 03:00 pixel is outside the window
    assert list(folded) == ['GMI']
    assert len(folded['GMI']) == 4


def test_pod_pixels_fold_false_keeps_the_per_swath_split():
    from starepandas.demo_plots import pod_pixels
    demo, metadata, window = _pod_pixels_fixture()
    by_swath = pod_pixels(demo, metadata, 'q000001', window, fold=False)
    assert sorted(by_swath) == ['GMI_S1', 'GMI_S2']
    assert len(by_swath['GMI_S1']) == 2 and len(by_swath['GMI_S2']) == 2


def _region_spatial_only():
    return pd.DataFrame({
        'Dataset': ['GMI_S1', 'SSMIS_S1', 'SSMIS_S2'],
        'podcode': ['q000001', 'q000001', 'q000002'],
        't_start': pd.to_datetime(['2025-01-01 21:00', '2025-01-01 21:10',
                                   '2025-01-01 11:00']),
        't_end': pd.to_datetime(['2025-01-01 21:05', '2025-01-01 21:15',
                                 '2025-01-01 11:05']),
    })


def test_region_cover_draws_bbox_cover_and_data_pods():
    from starepandas.demo_plots import plot_region_cover
    from starepandas.staredataframe import podcode_to_sid
    fig = plot_region_cover(
        bbox=(0.0, 0.0, 10.0, 10.0),
        cover_sids=[podcode_to_sid(p) for p in ('q000001', 'q000002', 'q000003')],
        spatial_only=_region_spatial_only(), highlight_pod='q000001')
    assert 'STARE cover' in fig.axes[0].get_title()
    labels = [t.get_text() for t in fig.axes[0].get_legend().get_texts()]
    assert any('3 QL4 trixels' in t for t in labels)
    assert any('holding data — 2' in t for t in labels)
    plt.close(fig)


def test_region_result_splits_kept_and_dropped():
    from starepandas.demo_plots import plot_region_result
    stamps = pd.to_datetime(['2025-01-01 21:01', '2025-01-01 21:02'])
    passes = {'GMI': pd.DataFrame({'lat': [1.0, 2.0], 'lon': [1.0, 2.0],
                                   'timestamp': stamps})}
    window = (pd.Timestamp('2025-01-01 20:30'), pd.Timestamp('2025-01-01 22:00'))
    fig = plot_region_result(passes, _region_spatial_only(), window,
                             bbox=(0.0, 0.0, 10.0, 10.0),
                             highlight_pod='q000001')
    # the morning SSMIS chunk is outside the window: kept 2, dropped 1
    assert 'keeps 2 and drops 1' in fig._suptitle.get_text()
    plt.close(fig)


# ── 2026-09-11: single-scan-group figures (video notebook v3) ────────────────


def test_color_folds_scan_group_to_instrument():
    from starepandas.demo_plots import INSTRUMENT_COLORS, _color, _DEFAULT_COLOR
    assert _color('GMI_S1') == _color('GMI') == INSTRUMENT_COLORS['GMI']
    assert _color('SSMIS_S4') == INSTRUMENT_COLORS['SSMIS']
    assert _color('NOPE') == _DEFAULT_COLOR


def test_plot_region_result_fold_false_labels_rows_by_scan_group():
    import matplotlib
    matplotlib.use('Agg')
    import pandas as pd
    from starepandas.demo_plots import plot_region_result
    t0 = pd.Timestamp('2025-01-01 21:30')
    spatial = pd.DataFrame({
        'Dataset': ['GMI_S1', 'GMI_S2', 'SSMIS_S1'],
        'podcode': ['q003200'] * 3,
        't_start': [t0, t0, t0 + pd.Timedelta(hours=10)],
        't_end': [t0 + pd.Timedelta(minutes=2)] * 2 + [t0 + pd.Timedelta(hours=10, minutes=2)],
    })
    px = pd.DataFrame({'lat': [-55.0], 'lon': [60.0], 'timestamp': [t0]})
    window = (t0 - pd.Timedelta(minutes=45), t0 + pd.Timedelta(minutes=45))
    fig = plot_region_result({'GMI_S1': px}, spatial, window, bbox=(55, -60, 65, -50), fold=False)
    ax_time = fig.axes[-1]
    assert [t.get_text() for t in ax_time.get_yticklabels()] == ['GMI_S1', 'GMI_S2', 'SSMIS_S1']
    fig2 = plot_region_result({'GMI': px}, spatial, window, bbox=(55, -60, 65, -50))
    assert [t.get_text() for t in fig2.axes[-1].get_yticklabels()] == ['GMI', 'SSMIS']
    import matplotlib.pyplot as plt
    plt.close(fig); plt.close(fig2)


# ── 2026-09-25: per-swath loader + region outlines (video notebook v5) ───────


def test_color_ignores_a_platform_suffix():
    from starepandas.demo_plots import INSTRUMENT_COLORS, _color, swath_label
    assert _color('ATMS_S1 (NOAA-21)') == INSTRUMENT_COLORS['ATMS']
    assert swath_label('ATMS_S1', '1C.NOAA21.ATMS.XCAL2023-V.20250208-S063124-E081253.011648.V07A') \
        == 'ATMS_S1 (NOAA-21)'
    assert swath_label('GMI_S1', 'oddname') == 'GMI_S1 (oddname)'


def test_swath_pixels_keeps_one_entry_per_granule():
    from starepandas.demo_plots import swath_pixels, chunk_pixels

    class _PerRowDemo:
        """Returns the chunk named by each row, keyed by its dataset."""
        def __init__(self, chunks):
            self._chunks = chunks

        def download_and_analyze(self, rows):
            out = {}
            for path, dataset in zip(rows['group_path'], rows['Dataset']):
                out.setdefault(dataset, []).append(self._chunks[path])
            return {k: pd.concat(v) for k, v in out.items()}

    t0 = pd.Timestamp('2025-02-08 07:30')
    late = pd.DataFrame({'lat': [38.0], 'lon': [-77.0], 'timestamp': [t0 + pd.Timedelta(minutes=30)]})
    early = pd.DataFrame({'lat': [38.5], 'lon': [-77.5], 'timestamp': [t0]})
    g_late = '1C.NPP.ATMS.XCAL2019-V.20250208-S065731-E083900.068836.V07A'
    g_early = '1C.NOAA21.ATMS.XCAL2023-V.20250208-S063124-E081253.011648.V07A'
    rows = pd.DataFrame({
        'group_path': [f'store/q111333-{g_late}-ATMS_S1.parquet',
                       f'store/q111333-{g_early}-ATMS_S1.parquet'],
        'Dataset': ['ATMS_S1', 'ATMS_S1'],
    })
    demo = _PerRowDemo({rows['group_path'][0]: late, rows['group_path'][1]: early})
    window = (t0 - pd.Timedelta(hours=1), t0 + pd.Timedelta(hours=1))
    # the scan-group loader merges the two platforms into one entry …
    assert list(chunk_pixels(demo, rows, window, fold=False)) == ['ATMS_S1']
    # … the swath loader keeps them apart, ordered by first pixel time
    by_swath = swath_pixels(demo, rows, window)
    assert list(by_swath) == ['ATMS_S1 (NOAA-21)', 'ATMS_S1 (Suomi NPP)']
    assert len(by_swath['ATMS_S1 (NOAA-21)']) == 1
    # a second granule of the same satellite (its next orbit) is not merged
    g_next = '1C.NOAA21.ATMS.XCAL2023-V.20250208-S081254-E095423.011649.V07A'
    rows2 = pd.concat([rows, pd.DataFrame({'group_path': [f'store/q111333-{g_next}-ATMS_S1.parquet'],
                                           'Dataset': ['ATMS_S1']})], ignore_index=True)
    demo2 = _PerRowDemo({**demo._chunks, rows2['group_path'][2]: late.assign(
        timestamp=late['timestamp'] + pd.Timedelta(minutes=10))})
    assert list(swath_pixels(demo2, rows2, window)) == [
        'ATMS_S1 (NOAA-21)', 'ATMS_S1 (Suomi NPP)', 'ATMS_S1 (NOAA-21) #011649']


def test_plot_rendezvous_title_override():
    from starepandas.demo_plots import plot_rendezvous
    t0 = pd.Timestamp('2025-02-08 07:35')
    passes = {'GMI_S1 (GPM)': pd.DataFrame({'lat': [38.0, 38.1], 'lon': [-77.0, -77.1],
                                            'timestamp': [t0, t0 + pd.Timedelta(minutes=1)]})}
    fig = plot_rendezvous('q111333', passes, t0, pd.Timedelta(minutes=5))
    assert fig._suptitle.get_text().startswith('A 1-way rendezvous')
    plt.close(fig)
    fig = plot_rendezvous('q111333', passes, t0, pd.Timedelta(minutes=5),
                          title='Six swaths over the pod')
    assert fig._suptitle.get_text() == 'Six swaths over the pod'
    assert 'passes over the pod' in fig.axes[-1].get_title()
    plt.close(fig)


def test_region_figures_accept_a_polygon_outline():
    from shapely.geometry import Polygon
    from starepandas.demo_plots import plot_region_cover, plot_region_result
    from starepandas.staredataframe import podcode_to_sid
    region = Polygon([(-83.7, 36.5), (-75.2, 36.5), (-75.2, 39.5), (-83.7, 39.5)])
    cover = [podcode_to_sid(p) for p in ('q111301', 'q111303', 'q111332', 'q111333')]
    spatial = pd.DataFrame({
        'Dataset': ['GMI_S1', 'ATMS_S1'], 'podcode': ['q111333', 'q111333'],
        't_start': pd.to_datetime(['2025-02-08 07:35', '2025-02-08 19:00']),
        't_end': pd.to_datetime(['2025-02-08 07:36', '2025-02-08 19:02']),
    })
    fig = plot_region_cover(None, cover, spatial, region=region, region_label='Virginia')
    labels = [t.get_text() for t in fig.axes[0].get_legend().get_texts()]
    assert 'Virginia' in labels and any('4 QL4 trixels' in t for t in labels)
    plt.close(fig)
    t0 = pd.Timestamp('2025-02-08 07:35')
    px = pd.DataFrame({'lat': [38.0], 'lon': [-77.0], 'timestamp': [t0]})
    window = (t0 - pd.Timedelta(hours=3), t0 + pd.Timedelta(hours=3))
    fig = plot_region_result({'GMI_S1': px}, spatial, window, bbox=None,
                             region=region, fold=False)
    assert 'keeps 1 and drops 1' in fig._suptitle.get_text()
    assert 'past the region' in fig.axes[0].get_title()
    plt.close(fig)


# ── 2026-09-26: per-swath panels, zoom extent, uniform legend markers ───────


def test_coverage_panels_by_swath_column_and_zoom():
    catalog = pd.DataFrame({
        'podcode': ['q111333', 'q111332', 'q111333', 'q111301'],
        'Dataset': ['ATMS_S1', 'ATMS_S1', 'GMI_S1', 'ATMS_S2'],
        'swath': ['ATMS — NOAA-21', 'ATMS — NOAA-21', 'GMI — GPM', 'ATMS — Suomi NPP'],
    })
    fig = plot_pod_coverage(catalog, group_by='swath')
    titles = [ax.get_title() for ax in fig.axes if ax.get_visible()]
    assert titles == ['ATMS — NOAA-21 — 2 QL4 pods', 'GMI — GPM — 1 QL4 pods',
                      'ATMS — Suomi NPP — 1 QL4 pods']
    plt.close(fig)
    fig = plot_pod_coverage(catalog, group_by='swath', extent=(-100, -60, 22, 52))
    lon_min, lon_max, lat_min, lat_max = fig.axes[0].get_extent()
    assert lon_min < -95 and lon_max > -65 and lat_min < 25 and lat_max > 50
    plt.close(fig)


def test_swath_legends_use_one_marker_size():
    from starepandas.demo_plots import (LEGEND_MARKER_SIZE, plot_rendezvous,
                                        plot_region_result)
    t0 = pd.Timestamp('2025-02-08 07:35')
    dense = pd.DataFrame({'lat': [38.0] * 50, 'lon': [-77.0] * 50, 'timestamp': [t0] * 50})
    sparse = pd.DataFrame({'lat': [38.5], 'lon': [-77.5], 'timestamp': [t0 + pd.Timedelta(minutes=1)]})
    passes = {'AMSR2_S1 (GCOM-W1)': dense, 'ATMS_S1 (NOAA-21)': sparse}
    fig = plot_rendezvous('q111333', passes, t0, pd.Timedelta(minutes=5))
    legend = fig.axes[0].get_legend()
    assert [t.get_text() for t in legend.get_texts()] == list(passes)     # arrival order
    assert {h.get_markersize() for h in legend.legend_handles} == {LEGEND_MARKER_SIZE}
    plt.close(fig)
    spatial = pd.DataFrame({'Dataset': ['AMSR2_S1'], 'podcode': ['q111333'],
                            't_start': [t0], 't_end': [t0 + pd.Timedelta(minutes=2)]})
    fig = plot_region_result(passes, spatial, (t0 - pd.Timedelta(hours=3), t0 + pd.Timedelta(hours=3)),
                             bbox=(-80, 36, -75, 40), fold=False)
    assert {h.get_markersize() for h in fig.axes[0].get_legend().legend_handles} == {LEGEND_MARKER_SIZE}
    plt.close(fig)


def test_plot_rendezvous_draws_a_region_outline_and_widens_to_it():
    from shapely.geometry import Polygon
    from starepandas.demo_plots import plot_rendezvous
    t0 = pd.Timestamp('2025-02-08 07:35')
    passes = {'GMI_S1 (GPM)': pd.DataFrame({'lat': [40.0, 40.1], 'lon': [-75.0, -75.1],
                                            'timestamp': [t0, t0 + pd.Timedelta(minutes=1)]})}
    virginia = Polygon([(-83.7, 36.5), (-75.2, 36.5), (-75.2, 39.5), (-83.7, 39.5)])
    fig = plot_rendezvous('q111333', passes, t0, pd.Timedelta(minutes=5),
                          region=virginia, region_label='Virginia')
    ax = fig.axes[0]
    assert [t.get_text() for t in ax.get_legend().get_texts()] == ['GMI_S1 (GPM)', 'Virginia']
    lon_min, lon_max, lat_min, lat_max = ax.get_extent()
    assert lon_min <= -83.7 and lat_min <= 36.5          # the state fits …
    assert lat_max >= 44.9 and lon_max >= -72.1          # … and so does the pod trixel
    plt.close(fig)


def test_plot_rendezvous_marks_a_point_in_the_legend_only():
    from shapely.geometry import Polygon
    from starepandas.demo_plots import INSTRUMENT_COLORS, plot_rendezvous, POINT_COLOR
    t0 = pd.Timestamp('2025-02-08 07:35')
    passes = {'GMI_S1 (GPM)': pd.DataFrame({'lat': [40.0, 40.1], 'lon': [-75.0, -75.1],
                                            'timestamp': [t0, t0 + pd.Timedelta(minutes=1)]})}
    virginia = Polygon([(-83.7, 36.5), (-75.2, 36.5), (-75.2, 39.5), (-83.7, 39.5)])
    fig = plot_rendezvous('q111332', passes, t0, pd.Timedelta(minutes=5),
                          region=virginia, region_label='Virginia',
                          point=(-77.04, 38.9), point_label='Washington D.C.')
    ax = fig.axes[0]
    legend = ax.get_legend()
    assert [t.get_text() for t in legend.get_texts()] == ['GMI_S1 (GPM)', 'Virginia', 'Washington D.C.']
    # the marker is a hollow ring in the point colour, distinct from every instrument colour
    dc = legend.legend_handles[-1]
    assert dc.get_markerfacecolor() == 'none'
    assert dc.get_markeredgecolor() == POINT_COLOR
    assert POINT_COLOR not in INSTRUMENT_COLORS.values()
    # no text label was written onto the map itself
    assert not [t for t in ax.texts if 'Washington' in t.get_text()]
    plt.close(fig)
