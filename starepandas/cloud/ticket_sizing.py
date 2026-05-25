"""Ticket sizing for the Path C scheduler.

Pure functions, no AWS imports — shared by the scheduler Lambda (C-4) and
local-host callers / tests.

The rule (Path C §C2):

  ============================  =========================  ===================
  Range                          Ticket count               Per-ticket size
  ============================  =========================  ===================
  N <= MAX                       1                          N
  MAX < N <= W * MAX             W                          ceil(N / W)
  N > W * MAX                    ceil(N / MAX)              MAX (last shorter)
  ============================  =========================  ===================

``MAX`` defaults to 40 (per §C2). ``W`` is the caller's requested worker
count; the boundary slides with ``W * MAX`` so passing ``workers=8`` moves
the switch point from N=160 to N=320 automatically.
"""

from __future__ import annotations

import math
from typing import List, Sequence

DEFAULT_MAX_TICKET_SIZE = 40


def split_into_tickets(
    granule_uris: Sequence[str],
    workers: int,
    max_ticket_size: int = DEFAULT_MAX_TICKET_SIZE,
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
        Upper bound on granules per ticket. Default 40 (§C2).

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

    uris = list(granule_uris)
    n = len(uris)

    if n == 0:
        return []

    if n <= max_ticket_size:
        # Rows 1 & 2 of the §C2 table: small batch, one ticket, one worker.
        return [uris]

    boundary = workers * max_ticket_size
    if n <= boundary:
        # Row 3: balance across W workers; ticket size in [ceil(MAX/W)+1, MAX].
        ticket_size = math.ceil(n / workers)
        return [uris[i : i + ticket_size] for i in range(0, n, ticket_size)]

    # Row 4: drain at MAX-sized tickets; last ticket may be shorter.
    return [uris[i : i + max_ticket_size] for i in range(0, n, max_ticket_size)]
