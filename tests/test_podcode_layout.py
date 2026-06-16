"""Tests for the quaternary pod-code storage layout.

Covers the codec (``sid_to_podcode`` / ``podcode_to_sid`` /
``podcode_to_local_dirs``), the self-describing chunk-filename grammar
(``chunk_filename`` / ``parse_chunk_filename``), and the S3 query-prefix helper
(``_podcode_query_prefixes``).  See ``docs/quaternary_storage_plan.md`` §2/§4.
"""

import numpy as np
import pystare
import pytest

from starepandas.staredataframe import (
    MAX_PARTITION_LEVEL,
    sid_to_podcode,
    podcode_to_sid,
    podcode_to_local_dirs,
    chunk_filename,
    parse_chunk_filename,
)
from starepandas.io.granules import _podcode_query_prefixes


# ----- codec round-trip ----------------------------------------------------


def _partition_sids(level, n=300, seed=0):
    rng = np.random.default_rng(seed)
    lats = rng.uniform(-80, 80, n)
    lons = rng.uniform(-180, 180, n)
    sids = pystare.from_latlon(lats, lons, level).astype(np.int64)
    sids = pystare.spatial_coerce_resolution(sids, level)
    sids = pystare.spatial_clear_to_resolution(sids)
    return np.unique(sids)


@pytest.mark.parametrize("level", [0, 1, 2, 3, 4])
def test_sid_podcode_round_trip(level):
    for s in _partition_sids(level):
        pc = sid_to_podcode(int(s))
        assert podcode_to_sid(pc) == int(s), (level, pc, int(s))


def test_podcode_length_follows_level():
    # Pod code length = q + octant + <level> quaternary digits = level + 2.
    s4 = int(_partition_sids(4)[0])
    s2 = int(_partition_sids(2)[0])
    assert len(sid_to_podcode(s4)) == MAX_PARTITION_LEVEL + 2 == 6
    assert len(sid_to_podcode(s2)) == 2 + 2 == 4


def test_octant_and_digit_ranges():
    # All 8 octants must be encodable and recoverable.
    seen_octants = {sid_to_podcode(int(s))[1] for s in _partition_sids(4, n=2000)}
    assert seen_octants, "no codes produced"
    assert seen_octants <= set("01234567")


def test_podcode_to_sid_rejects_bad_input():
    with pytest.raises(ValueError):
        podcode_to_sid("13211")        # missing 'q'
    with pytest.raises(ValueError):
        podcode_to_sid("q8")           # octant out of range
    with pytest.raises(ValueError):
        podcode_to_sid("q14")          # quaternary digit out of range
    with pytest.raises(ValueError):
        podcode_to_sid("qa1")          # non-numeric octant


# ----- local dir chain -----------------------------------------------------


def test_podcode_to_local_dirs():
    assert podcode_to_local_dirs("q13211") == ["q13", "q132", "q1321", "q13211"]
    assert podcode_to_local_dirs("q132") == ["q13", "q132"]
    assert podcode_to_local_dirs("q1") == ["q1"]          # octant-only (level 0)
    # Last element is always the full pod code (the leaf dir).
    assert podcode_to_local_dirs("q13211")[-1] == "q13211"


# ----- chunk filename grammar ----------------------------------------------


def test_chunk_filename_round_trip_with_dash_in_granule():
    granule = "1C.F18.SSMIS.XCAL2021-V.20250101-S011702-E025853.078435.V07B"
    name = chunk_filename("q13211", granule, "SSMIS_S1")
    assert name == f"q13211-{granule}-SSMIS_S1.parquet"
    pc, g, ds = parse_chunk_filename(name)
    assert (pc, g, ds) == ("q13211", granule, "SSMIS_S1")


def test_parse_chunk_filename_accepts_full_key():
    name = chunk_filename("q13211", "G1", "GMI_S1")
    pc, g, ds = parse_chunk_filename(f"s3://zarrpods/storage/{name}")
    assert (pc, g, ds) == ("q13211", "G1", "GMI_S1")


def test_chunk_filename_rejects_dash_in_dataset():
    with pytest.raises(ValueError, match="dataset name must not contain"):
        chunk_filename("q13211", "G1", "BAD-DATASET")


def test_parse_chunk_filename_rejects_non_chunk():
    with pytest.raises(ValueError):
        parse_chunk_filename("q13211.parquet")        # only the pod code, no granule/dataset


# ----- S3 query prefixes (mixed-level / ancestor probing) ------------------


def test_query_prefixes_include_self_and_ancestors():
    prefixes = _podcode_query_prefixes("q13211")
    # The query code itself (prefix-matches itself + finer descendants).
    assert prefixes[0] == "q13211"
    # Each coarser ancestor probed with a trailing '-' so it matches only
    # chunks stored AT that level, not the query's own subtree.
    assert set(prefixes[1:]) == {"q1-", "q13-", "q132-", "q1321-"}


def test_query_prefix_matches_exact_and_ancestor_keys():
    # A fine query's exact-level chunk: matched by the bare query prefix.
    exact = chunk_filename("q13211", "G1", "GMI_S1")
    # A coarser ancestor chunk (level 2): matched by 'q132-' but NOT by 'q13211'.
    coarse = chunk_filename("q132", "G1", "GMI_S1")
    prefixes = _podcode_query_prefixes("q13211")
    assert any(exact.startswith(p) for p in prefixes)
    assert any(coarse.startswith(p) for p in prefixes)
    # The bare query prefix must NOT swallow the coarse ancestor.
    assert not coarse.startswith("q13211")
