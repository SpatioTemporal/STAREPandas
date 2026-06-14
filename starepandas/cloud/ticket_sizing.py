"""Ticket sizing for the Path C scheduler.

Pure functions, no AWS imports — shared by the scheduler Lambda (C-4) and
local-host callers / tests.

The rule (Path C §C2, revised 2026-06-14):

    ticket_size = max(MIN, min(MAX, ceil(N / W)))

then the granules are chunked into back-to-back tickets of that size (the last
ticket may be shorter). This single expression replaces the original 3-row
table and removes its small-job latency cliff:

  ==========================  ==========================  ====================
  Range                        Tickets                     Per-ticket size
  ==========================  ==========================  ====================
  N <= W * MAX                 ceil(N / ticket_size)        ceil(N / W) (>= MIN)
  N >  W * MAX                 ceil(N / MAX)                MAX (last shorter)
  ==========================  ==========================  ====================

Why the revision: the old rule made ``N <= MAX`` a single ticket on a single
worker, so a 40-granule job ran fully serially (~8.7 min at ~13 s/granule)
while the other workers sat idle. Balancing across ``W`` for *all* job sizes
uses the parallelism we already pay to boot. For ``N > W * MAX`` the size caps
at ``MAX`` (a visibility-timeout safety bound) and the surplus tickets are
work-stolen by the ``W`` workers — unchanged from before.

``MAX`` defaults to 40 (§C2; conservative against the 3600 s SQS visibility
timeout — C-7 may raise it from measured per-granule time). ``MIN`` defaults to
1 (no floor); raising it keeps trivially small jobs on fewer workers so their
cold-start cost isn't wasted — a knob C-7 can calibrate from the measured
cold-start-vs-processing crossover.
"""

from __future__ import annotations

import math
from typing import List, Sequence

DEFAULT_MAX_TICKET_SIZE = 40
DEFAULT_MIN_TICKET_SIZE = 1


def split_into_tickets(
    granule_uris: Sequence[str],
    workers: int,
    max_ticket_size: int = DEFAULT_MAX_TICKET_SIZE,
    min_ticket_size: int = DEFAULT_MIN_TICKET_SIZE,
) -> List[List[str]]:
    """Split a list of granule URIs into tickets per the §C2 sizing rule.

    Parameters
    ----------
    granule_uris : sequence of str
        URIs to partition (s3://, https://, file://, etc.). Order is
        preserved within and across tickets.
    workers : int
        Worker count the caller requested. Must be >= 1. The §C9 cost cap
        (workers <= 4 for v1) is enforced by the scheduler, not here.
    max_ticket_size : int, optional
        Upper bound on granules per ticket (visibility-timeout safety). Default
        40 (§C2).
    min_ticket_size : int, optional
        Lower bound on granules per ticket. Default 1 (no floor — balance fully
        across the workers). Raise it to avoid over-fanning trivially small
        jobs across more workers than their cold-start cost justifies.

    Returns
    -------
    list of list of str
        Tickets, each a list of granule URIs. The flattened concatenation
        equals ``list(granule_uris)``.
    """
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")
    if max_ticket_size < 1:
        raise ValueError(f"max_ticket_size must be >= 1, got {max_ticket_size}")
    if min_ticket_size < 1:
        raise ValueError(f"min_ticket_size must be >= 1, got {min_ticket_size}")
    if min_ticket_size > max_ticket_size:
        raise ValueError(
            f"min_ticket_size ({min_ticket_size}) must be <= "
            f"max_ticket_size ({max_ticket_size})"
        )

    uris = list(granule_uris)
    n = len(uris)
    if n == 0:
        return []

    # Balance across the workers (ceil(N / W)), capped at MAX so no ticket can
    # outrun the visibility timeout, floored at MIN so tiny jobs don't fan out
    # across more workers than their cold-start cost justifies.
    ticket_size = max(min_ticket_size, min(max_ticket_size, math.ceil(n / workers)))
    return [uris[i : i + ticket_size] for i in range(0, n, ticket_size)]
