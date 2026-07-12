"""VCF temporal roll-up (temporal-stare-pods issue 04).

Verifies the on-the-fly temporal hierarchy: given a pod-code level (and
optionally a subtree and a period), the roll-up returns per pod at that level
the **union** temporal range ``[min(t_start), max(t_end)]`` of all chunks
beneath it, plus a child count — computed by grouping on a pod-code prefix (a
coarser pod code is a prefix of its descendants). No materialized index.

Seams:
- the **pure** :func:`vcf_rollup` over a fixture temporal-catalog DataFrame
  (highest seam, no database);
- the **local write → SQLite round-trip** via :func:`load_local_vcf`
  (same seam as ``test_temporal_query.py``);
- the cloud loader is pinned to the same shared machinery by source
  inspection (no live RDS in unit tests — same pattern as issue 03).

The level → prefix-length mapping is centralized next to the pod-code codec
(:func:`starepandas.staredataframe.podcode_prefix_length`) so it cannot drift
from the grammar: ``'q'`` + octant + one quaternary digit per level.
"""

import inspect

import pandas as pd
import pytest

from starepandas.io.granules import (
    load_local_temporal_catalog,
    load_local_vcf,
    load_s3_temporal_catalog,
    load_s3_vcf,
    vcf_rollup,
)
from starepandas.staredataframe import (
    MAX_PARTITION_LEVEL,
    podcode_prefix_length,
    podcode_to_sid,
    sid_to_podcode,
)
from tests._temporal_fixtures import (
    CHUNK_END,
    CHUNK_START,
    T0,
    single_pod_sdf,
    two_pod_demo,
    write_local,
)


VCF_COLUMNS = ['podcode', 't_start', 't_end', 'n_chunks', 'n_without_range']


def _hours(h):
    return T0 + pd.Timedelta(hours=h)


def _fixture_catalog():
    """Six level-4 chunks across two octant-1 subtrees and one octant-2 pod;
    one chunk (in q1320…) has a null temporal range."""
    return pd.DataFrame({
        'podcode': ['q13211', 'q13211', 'q13210', 'q13201', 'q13011', 'q20123'],
        'Dataset': ['GMI_S1'] * 6,
        't_start': [_hours(0), _hours(5), _hours(2), pd.NaT, _hours(1), _hours(7)],
        't_end':   [_hours(1), _hours(6), _hours(3), pd.NaT, _hours(2), _hours(8)],
    })


# ----- level → prefix length: centralized next to the codec -----------------


def test_prefix_length_matches_codec():
    """prefix length at a level == length of a pod code the codec emits at
    that level — the 'cannot drift' contract."""
    for podcode in ('q1', 'q13', 'q132', 'q1321', 'q13211'):
        level = len(podcode) - 2
        assert podcode_prefix_length(level) == len(podcode)
        assert len(sid_to_podcode(podcode_to_sid(podcode))) == \
            podcode_prefix_length(level)


def test_prefix_length_rejects_bad_levels():
    with pytest.raises(ValueError):
        podcode_prefix_length(-1)
    with pytest.raises(ValueError):
        podcode_prefix_length(28)          # codec caps at 27 refinement levels
    with pytest.raises((TypeError, ValueError)):
        podcode_prefix_length(1.5)


# ----- the pure roll-up seam -------------------------------------------------


def test_rollup_union_range_matches_manual_aggregation():
    catalog = _fixture_catalog()
    vcf = vcf_rollup(catalog, level=1)

    assert list(vcf.columns) == VCF_COLUMNS
    assert list(vcf['podcode']) == ['q13', 'q20']

    q13 = vcf[vcf['podcode'] == 'q13'].iloc[0]
    beneath = catalog[catalog['podcode'].str.startswith('q13')]
    assert q13['t_start'] == beneath['t_start'].min()   # NaT-skipping min
    assert q13['t_end'] == beneath['t_end'].max()
    assert q13['t_start'] == _hours(0)
    assert q13['t_end'] == _hours(6)

    q20 = vcf[vcf['podcode'] == 'q20'].iloc[0]
    assert q20['t_start'] == _hours(7)
    assert q20['t_end'] == _hours(8)


def test_rollup_child_count_counts_chunks_and_reports_missing_ranges():
    vcf = vcf_rollup(_fixture_catalog(), level=1).set_index('podcode')
    # Null-range chunks count as children but are noted as range-less.
    assert vcf.loc['q13', 'n_chunks'] == 5
    assert vcf.loc['q13', 'n_without_range'] == 1
    assert vcf.loc['q20', 'n_chunks'] == 1
    assert vcf.loc['q20', 'n_without_range'] == 0


def test_rollup_at_leaf_level_returns_per_chunk_pods():
    catalog = _fixture_catalog()
    vcf = vcf_rollup(catalog, level=MAX_PARTITION_LEVEL).set_index('podcode')

    assert len(vcf) == 5                    # q13211 holds two chunks
    assert vcf.loc['q13211', 'n_chunks'] == 2
    assert vcf.loc['q13211', 't_start'] == _hours(0)
    assert vcf.loc['q13211', 't_end'] == _hours(6)
    assert vcf.loc['q13011', 'n_chunks'] == 1


def test_rollup_intermediate_level_splits_subtrees():
    vcf = vcf_rollup(_fixture_catalog(), level=3).set_index('podcode')
    assert set(vcf.index) == {'q1321', 'q1320', 'q1301', 'q2012'}
    # q1320 holds only the null-range chunk → union range is null.
    assert pd.isna(vcf.loc['q1320', 't_start'])
    assert pd.isna(vcf.loc['q1320', 't_end'])
    assert vcf.loc['q1320', 'n_chunks'] == 1
    assert vcf.loc['q1320', 'n_without_range'] == 1


def test_rollup_subtree_scopes_to_prefix():
    vcf = vcf_rollup(_fixture_catalog(), level=4, subtree='q132')
    assert set(vcf['podcode']) == {'q13211', 'q13210', 'q13201'}
    vcf_deep = vcf_rollup(_fixture_catalog(), level=4, subtree='q13211')
    assert list(vcf_deep['podcode']) == ['q13211']
    assert vcf_rollup(_fixture_catalog(), level=4, subtree='q3').empty


def test_rollup_rejects_invalid_subtree():
    # 'q٣3' has an Arabic-Indic THREE: int('٣') == 3, but it can never match
    # the ASCII pod codes the writer emits — the grammar gate must reject it.
    for bad in ('x13', 'q8', 'q14', 'q1%', '', 'q٣3'):
        with pytest.raises(ValueError):
            vcf_rollup(_fixture_catalog(), level=4, subtree=bad)


def test_rollup_rejects_subtree_deeper_than_level():
    """A level-1 row labeled 'q13' but aggregating only q13211's sliver would
    be indistinguishable from the pod's true envelope — must raise, both in
    the pure roll-up and in the loaders' pre-query fail-fast."""
    with pytest.raises(ValueError, match='deeper'):
        vcf_rollup(_fixture_catalog(), level=1, subtree='q13211')
    with pytest.raises(ValueError, match='deeper'):
        load_local_vcf('/nonexistent/never-touched.db', level=0, subtree='q132')
    # Equal depth is the boundary and is fine (leaf-level identity).
    assert list(vcf_rollup(_fixture_catalog(), level=4,
                           subtree='q13211')['podcode']) == ['q13211']


def test_rollup_raises_on_null_podcode():
    """Rows without a pod code (pre-temporal upgrade remnants) must raise —
    pandas groupby would otherwise drop them silently and undercount."""
    catalog = _fixture_catalog()
    catalog.loc[2, 'podcode'] = None
    with pytest.raises(ValueError, match='null podcode'):
        vcf_rollup(catalog, level=1)


def test_rollup_half_null_range_never_joins_the_union():
    """A chunk with t_start but no t_end has no usable range: it must count
    in n_without_range AND contribute neither end to the union."""
    catalog = pd.DataFrame({
        'podcode': ['q13211', 'q13210'],
        'Dataset': ['GMI_S1'] * 2,
        't_start': [_hours(-240), _hours(2)],   # rogue early half-null start
        't_end':   [pd.NaT, _hours(3)],
    })
    vcf = vcf_rollup(catalog, level=1)
    assert len(vcf) == 1
    assert vcf.iloc[0]['n_chunks'] == 2
    assert vcf.iloc[0]['n_without_range'] == 1
    assert vcf.iloc[0]['t_start'] == _hours(2)    # not the half-null row's


def test_rollup_empty_catalog_returns_empty_frame_with_schema():
    empty = _fixture_catalog().iloc[0:0]
    vcf = vcf_rollup(empty, level=2)
    assert vcf.empty
    assert list(vcf.columns) == VCF_COLUMNS
    assert pd.api.types.is_datetime64_any_dtype(vcf['t_start'])
    assert pd.api.types.is_datetime64_any_dtype(vcf['t_end'])


# ----- local round-trip: pod granules, then roll up --------------------------


def test_local_vcf_matches_manual_leaf_aggregation(tmp_path):
    demo, _ = two_pod_demo(tmp_path)
    leaves = load_local_temporal_catalog(demo.db_path)
    vcf = load_local_vcf(demo.db_path, level=0)

    assert list(vcf.columns) == VCF_COLUMNS
    for _, pod in vcf.iterrows():
        beneath = leaves[leaves['podcode'].str.startswith(pod['podcode'])]
        assert pod['t_start'] == beneath['t_start'].min()
        assert pod['t_end'] == beneath['t_end'].max()
        assert pod['n_chunks'] == len(beneath)

    # CA chunk [T0, T0+30m] and AU chunk a day later live in different
    # octants → two level-0 pods, each carrying its own chunk's range.
    assert len(vcf) == 2
    assert vcf['n_chunks'].sum() == len(leaves) == 2
    assert vcf['t_start'].min() == CHUNK_START
    assert vcf['t_end'].max() == T0 + pd.Timedelta(days=1, minutes=30)


def test_local_vcf_coarse_level_unions_disjoint_leaf_ranges(tmp_path):
    """Two same-pod granules with disjoint ranges: the VCF at every coarser
    level reports the union envelope [min start, max end]."""
    sdf_early = single_pod_sdf()
    sdf_late = single_pod_sdf(t_start=T0 + pd.Timedelta(days=2),
                              t_end=T0 + pd.Timedelta(days=2, minutes=30))
    root, db_path = write_local(tmp_path, sdf_early, granule_name='G_EARLY')
    sdf_late.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                      granule_name='G_LATE',
                      raw_collected_time=pd.Timestamp('2020-06-17').to_pydatetime())

    for level in range(MAX_PARTITION_LEVEL + 1):
        vcf = load_local_vcf(db_path, level=level)
        assert len(vcf) == 1
        pod = vcf.iloc[0]
        assert len(pod['podcode']) == podcode_prefix_length(level)
        assert pod['t_start'] == CHUNK_START
        assert pod['t_end'] == T0 + pd.Timedelta(days=2, minutes=30)
        assert pod['n_chunks'] == 2


def test_local_vcf_subtree_scoping(tmp_path):
    demo, _ = two_pod_demo(tmp_path)
    leaves = load_local_temporal_catalog(demo.db_path)
    ca_leaf = leaves.loc[leaves['t_start'] == CHUNK_START, 'podcode'].iloc[0]
    subtree = ca_leaf[:podcode_prefix_length(1)]

    vcf = load_local_vcf(demo.db_path, level=MAX_PARTITION_LEVEL, subtree=subtree)
    assert list(vcf['podcode']) == [ca_leaf]
    assert vcf.iloc[0]['t_start'] == CHUNK_START
    assert vcf.iloc[0]['t_end'] == CHUNK_END


def test_local_vcf_period_filter_prunes_chunks(tmp_path):
    demo, _ = two_pod_demo(tmp_path)
    vcf = load_local_vcf(
        demo.db_path, level=0,
        period=(CHUNK_START - pd.Timedelta(hours=1),
                CHUNK_END + pd.Timedelta(hours=1)))
    assert len(vcf) == 1
    assert vcf.iloc[0]['t_start'] == CHUNK_START
    assert vcf.iloc[0]['t_end'] == CHUNK_END


def test_local_vcf_counts_null_range_chunks(tmp_path):
    _root, db_path = write_local(tmp_path, single_pod_sdf(with_timestamp=False))
    vcf = load_local_vcf(db_path, level=0)
    assert len(vcf) == 1
    assert vcf.iloc[0]['n_chunks'] == 1
    assert vcf.iloc[0]['n_without_range'] == 1
    assert pd.isna(vcf.iloc[0]['t_start'])


def test_local_temporal_catalog_podcode_prefix_filter(tmp_path):
    demo, _ = two_pod_demo(tmp_path)
    leaves = load_local_temporal_catalog(demo.db_path)
    ca_leaf = leaves.loc[leaves['t_start'] == CHUNK_START, 'podcode'].iloc[0]

    scoped = load_local_temporal_catalog(demo.db_path, podcode_prefix=ca_leaf[:3])
    assert list(scoped['podcode']) == [ca_leaf]
    with pytest.raises(ValueError):
        load_local_temporal_catalog(demo.db_path, podcode_prefix='q1%')


def test_local_temporal_catalog_excludes_null_podcode_rows(tmp_path):
    """A pre-temporal catalog upgraded in place keeps its legacy rows with a
    NULL podcode; the temporal-catalog projection must exclude them (they
    cannot participate in pod-keyed analytics) so the VCF stays computable."""
    import sqlite3

    demo, _ = two_pod_demo(tmp_path)
    conn = sqlite3.connect(demo.db_path)
    conn.execute(
        'INSERT INTO "PodsMetadata" ("Dataset", "RawData Collected Time", '
        'grouped_id) VALUES (?, ?, ?)', ('GMI_S1', '2019-01-01 00:00:00', 42))
    conn.commit()
    conn.close()

    leaves = load_local_temporal_catalog(demo.db_path)
    assert len(leaves) == 2                    # legacy row not in the projection
    assert leaves['podcode'].notna().all()
    vcf = load_local_vcf(demo.db_path, level=0)
    assert vcf['n_chunks'].sum() == 2          # and the roll-up still works


# ----- cloud parity (no live RDS in unit tests) -------------------------------


def test_s3_vcf_shares_the_local_rollup_machinery():
    """Both backends must roll up through the same pure vcf_rollup over the
    thin temporal-catalog projection — no second GROUP BY dialect to drift."""
    for fn, loader in ((load_s3_vcf, 'load_s3_temporal_catalog'),
                       (load_local_vcf, 'load_local_temporal_catalog')):
        src = inspect.getsource(fn)
        assert 'vcf_rollup' in src
        assert loader in src


def test_s3_temporal_catalog_supports_subtree_prefix():
    """Both thin loaders route the subtree filter through the shared
    _podcode_prefix_condition builder (same pattern as _period_conditions),
    which emits the index-riding LIKE prefix."""
    from starepandas.io.granules import (_podcode_prefix_condition,
                                         load_local_temporal_catalog)

    for fn in (load_s3_temporal_catalog, load_local_temporal_catalog):
        assert '_podcode_prefix_condition' in inspect.getsource(fn)
        assert 'podcode_prefix' in inspect.signature(fn).parameters
    conds, params = _podcode_prefix_condition('q13', placeholder='?')
    assert conds == ['podcode LIKE ?']
    assert params == ['q13%']


def test_vcf_loader_signatures_are_parallel():
    s3 = inspect.signature(load_s3_vcf)
    local = inspect.signature(load_local_vcf)
    for name in ('level', 'subtree', 'dataset', 'dataset_prefix', 'period'):
        assert name in s3.parameters
        assert name in local.parameters
    assert list(local.parameters)[0] == 'db_path'
