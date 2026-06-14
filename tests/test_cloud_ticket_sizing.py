"""Tests for starepandas.cloud.ticket_sizing.

Covers the revised §C2 sizing rule ``ticket_size = max(MIN, min(MAX, ceil(N/W)))``
plus boundary and edge cases.
"""

import math

import pytest

from starepandas.cloud.ticket_sizing import (
    DEFAULT_MAX_TICKET_SIZE,
    DEFAULT_MIN_TICKET_SIZE,
    split_into_tickets,
)


def _uris(n):
    return [f"s3://bucket/granule-{i:05d}" for i in range(n)]


# ----- Small batches now parallelize across the workers -------------------
# (Revised 2026-06-14: the old rule put N<=MAX on a single ticket/worker,
#  running serially; it now balances across W like every other size.)


def test_small_batch_balances_across_workers():
    """N = 40, W = 4: 4 tickets of 10 (was 1 ticket of 40 — the cliff fix)."""
    tickets = split_into_tickets(_uris(40), workers=4)
    assert len(tickets) == 4
    assert [len(t) for t in tickets] == [10, 10, 10, 10]


def test_small_batch_fewer_granules_than_workers():
    """N = 2, W = 4: 2 tickets of 1 (only min(W, N) workers get work)."""
    tickets = split_into_tickets(_uris(2), workers=4)
    assert [len(t) for t in tickets] == [1, 1]


@pytest.mark.parametrize("n", [1])
def test_single_granule_single_ticket(n):
    """N = 1 → exactly one ticket of one granule, regardless of W."""
    tickets = split_into_tickets(_uris(n), workers=4)
    assert tickets == [_uris(n)]


def test_min_ticket_size_floor_keeps_tiny_jobs_compact():
    """min_ticket_size avoids over-fanning tiny jobs: N=8, W=4, MIN=5
    → ceil(8/4)=2 is floored to 5 → 2 tickets of [5, 3] (2 workers, not 4)."""
    tickets = split_into_tickets(_uris(8), workers=4, min_ticket_size=5)
    assert [len(t) for t in tickets] == [5, 3]


def test_min_ticket_size_does_not_exceed_max():
    """A min_ticket_size above max_ticket_size is rejected."""
    with pytest.raises(ValueError, match="min_ticket_size"):
        split_into_tickets(_uris(10), workers=4, max_ticket_size=4, min_ticket_size=5)


# ----- MAX < N <= W * MAX → W tickets, ceil(N/W) each ---------------------


def test_balanced_across_workers_lower_bound():
    """N = 41, W = 4: 4 tickets of 11, 11, 11, 8 (sum = 41)."""
    tickets = split_into_tickets(_uris(41), workers=4)
    assert len(tickets) == 4
    sizes = [len(t) for t in tickets]
    assert sizes == [11, 11, 11, 8]
    assert sum(sizes) == 41


def test_balanced_across_workers_at_boundary():
    """N = 160 = W * MAX, W = 4: 4 tickets of exactly 40 each."""
    tickets = split_into_tickets(_uris(160), workers=4)
    assert len(tickets) == 4
    assert all(len(t) == 40 for t in tickets)


def test_balanced_across_workers_mid_range():
    """N = 100, W = 4: 4 tickets of 25 each."""
    tickets = split_into_tickets(_uris(100), workers=4)
    assert len(tickets) == 4
    assert [len(t) for t in tickets] == [25, 25, 25, 25]


def test_boundary_shifts_with_worker_count():
    """W = 8 shifts the row-3/row-4 boundary from 160 to 320."""
    # N = 200, W = 8: still in row 3 (200 <= 320) → 8 tickets of ceil(200/8)=25.
    tickets = split_into_tickets(_uris(200), workers=8)
    assert len(tickets) == 8
    assert all(len(t) == 25 for t in tickets)


# ----- N > W * MAX → ceil(N/MAX) tickets of MAX, work-stolen --------------


def test_drain_mode_just_above_boundary():
    """N = 161, W = 4: 5 tickets — first 4 of 40, last of 1 (sum = 161)."""
    tickets = split_into_tickets(_uris(161), workers=4)
    assert len(tickets) == 5
    assert [len(t) for t in tickets] == [40, 40, 40, 40, 1]


def test_drain_mode_large_batch():
    """N = 1000, W = 4: 25 tickets of 40 each."""
    tickets = split_into_tickets(_uris(1000), workers=4)
    assert len(tickets) == 25
    assert all(len(t) == 40 for t in tickets)


def test_drain_mode_last_ticket_shorter():
    """N = 1037, W = 4: 26 tickets, last of size 37 (1000 + 37)."""
    tickets = split_into_tickets(_uris(1037), workers=4)
    assert len(tickets) == 26
    assert all(len(t) == 40 for t in tickets[:-1])
    assert len(tickets[-1]) == 37


# ----- Edge cases ---------------------------------------------------------


def test_empty_input_returns_empty_list():
    assert split_into_tickets([], workers=4) == []


def test_invalid_workers_raises():
    with pytest.raises(ValueError, match="workers"):
        split_into_tickets(_uris(10), workers=0)


def test_invalid_max_ticket_size_raises():
    with pytest.raises(ValueError, match="max_ticket_size"):
        split_into_tickets(_uris(10), workers=4, max_ticket_size=0)


def test_concatenation_preserves_order_and_completeness():
    """Flattening tickets back together must reproduce the input exactly."""
    uris = _uris(347)
    tickets = split_into_tickets(uris, workers=4)
    flat = [u for t in tickets for u in t]
    assert flat == uris


def test_default_max_constant_matches_spec():
    """§C2 specifies MAX = 40."""
    assert DEFAULT_MAX_TICKET_SIZE == 40


def test_default_min_constant_is_one():
    """Default MIN = 1 → no floor (full balancing across workers)."""
    assert DEFAULT_MIN_TICKET_SIZE == 1


def test_custom_max_ticket_size():
    """A caller can override MAX (e.g. for testing with smaller batches)."""
    tickets = split_into_tickets(_uris(15), workers=4, max_ticket_size=5)
    # N=15, MAX=5, boundary=20 → row 3: ticket_size=ceil(15/4)=4
    assert len(tickets) == 4
    assert [len(t) for t in tickets] == [4, 4, 4, 3]
