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
    # Pod code length = q + 2 root digits + <level> quaternary digits = level + 3.
    s4 = int(_partition_sids(4)[0])
    s2 = int(_partition_sids(2)[0])
    assert len(sid_to_podcode(s4)) == MAX_PARTITION_LEVEL + 3 == 7
    assert len(sid_to_podcode(s2)) == 2 + 3 == 5


def test_octant_and_digit_ranges():
    # All 8 octants must be encodable and recoverable via the 2-digit root.
    seen_roots = {sid_to_podcode(int(s))[1:3] for s in _partition_sids(4, n=2000)}
    assert seen_roots, "no codes produced"
    valid_roots = {f"{o // 4}{o % 4}" for o in range(8)}
    assert seen_roots <= valid_roots


def test_all_eight_roots_round_trip():
    # octant = d0*4 + d1; d0 in {0,1}, d1 in {0-3} -> q00..q13.
    for octant in range(8):
        sid = octant << 59                     # level-0 SID for this face
        code = sid_to_podcode(sid)
        assert code == "q" + str(octant // 4) + str(octant % 4)
        assert podcode_to_sid(code) == sid


def test_podcode_to_sid_rejects_bad_input():
    with pytest.raises(ValueError):
        podcode_to_sid("132110")       # missing 'q'
    with pytest.raises(ValueError):
        podcode_to_sid("q0")           # single root digit (old-length level 0)
    with pytest.raises(ValueError):
        podcode_to_sid("q04")          # root digit d1 out of range
    with pytest.raises(ValueError):
        podcode_to_sid("q004")         # refinement digit out of range (0-3)
    with pytest.raises(ValueError):
        podcode_to_sid("q003x")        # non-numeric refinement digit
    with pytest.raises(ValueError):
        podcode_to_sid("qa1")          # non-numeric root
    # d0 in {2..7} carries the old single-octant-digit format hint.
    with pytest.raises(ValueError, match="pre-2026-08-21"):
        podcode_to_sid("q63333")


# ----- local dir chain -----------------------------------------------------


def test_podcode_to_local_dirs():
    assert podcode_to_local_dirs("q132110") == [
        "q13", "q132", "q1321", "q13211", "q132110"]
    assert podcode_to_local_dirs("q1321") == ["q13", "q132", "q1321"]
    assert podcode_to_local_dirs("q13") == ["q13"]        # root-only (level 0)
    # Last element is always the full pod code (the leaf dir).
    assert podcode_to_local_dirs("q132110")[-1] == "q132110"


# ----- chunk filename grammar ----------------------------------------------


def test_chunk_filename_round_trip_with_dash_in_granule():
    granule = "1C.F18.SSMIS.XCAL2021-V.20250101-S011702-E025853.078435.V07B"
    name = chunk_filename("q132110", granule, "SSMIS_S1")
    assert name == f"q132110-{granule}-SSMIS_S1.parquet"
    pc, g, ds = parse_chunk_filename(name)
    assert (pc, g, ds) == ("q132110", granule, "SSMIS_S1")


def test_parse_chunk_filename_accepts_full_key():
    name = chunk_filename("q132110", "G1", "GMI_S1")
    pc, g, ds = parse_chunk_filename(f"s3://zarrpods/storage/{name}")
    assert (pc, g, ds) == ("q132110", "G1", "GMI_S1")


def test_chunk_filename_rejects_dash_in_dataset():
    with pytest.raises(ValueError, match="dataset name must not contain"):
        chunk_filename("q132110", "G1", "BAD-DATASET")


def test_parse_chunk_filename_rejects_non_chunk():
    with pytest.raises(ValueError):
        parse_chunk_filename("q132110.parquet")        # only the pod code, no granule/dataset


# ----- S3 query prefixes (mixed-level / ancestor probing) ------------------


def test_query_prefixes_include_self_and_ancestors():
    prefixes = _podcode_query_prefixes("q132110")
    # The query code itself (prefix-matches itself + finer descendants).
    assert prefixes[0] == "q132110"
    # Each coarser ancestor probed with a trailing '-' so it matches only
    # chunks stored AT that level, not the query's own subtree. The coarsest
    # ancestor is the level-0 root (2-digit body) — never a 1-digit body.
    assert set(prefixes[1:]) == {"q13-", "q132-", "q1321-", "q13211-"}


def test_query_prefix_matches_exact_and_ancestor_keys():
    # A fine query's exact-level chunk: matched by the bare query prefix.
    exact = chunk_filename("q132110", "G1", "GMI_S1")
    # A coarser ancestor chunk (level 2): matched by 'q1321-' but NOT by 'q132110'.
    coarse = chunk_filename("q1321", "G1", "GMI_S1")
    prefixes = _podcode_query_prefixes("q132110")
    assert any(exact.startswith(p) for p in prefixes)
    assert any(coarse.startswith(p) for p in prefixes)
    # The bare query prefix must NOT swallow the coarse ancestor.
    assert not coarse.startswith("q132110")
