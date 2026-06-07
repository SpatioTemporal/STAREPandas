"""VENDORED copy of ``starepandas/cloud/ticket_sizing.py`` (C-4 Part B).

Why vendored: the Lambdas need only this pure §C2 sizing function, but
importing it from the installed package would pull in all of
``starepandas`` (and transitively ``pystare`` / ``rasterio``) via the
package ``__init__`` — bloating the deploy bundle and cold-start. This is
a verbatim copy of the source logic; keep the two in sync if either
changes. There is a parity unit test (``tests/test_scheduler_lambda.py``)
asserting this copy matches the library implementation.

Source of truth: ``starepandas/cloud/ticket_sizing.py``.
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

    N <= MAX            -> 1 ticket of N
    MAX < N <= W*MAX    -> W tickets of ceil(N/W)
    N > W*MAX           -> ceil(N/MAX) tickets of MAX (last shorter)
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
        return [uris]

    boundary = workers * max_ticket_size
    if n <= boundary:
        ticket_size = math.ceil(n / workers)
        return [uris[i : i + ticket_size] for i in range(0, n, ticket_size)]

    return [uris[i : i + max_ticket_size] for i in range(0, n, max_ticket_size)]
