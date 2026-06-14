"""VENDORED copy of ``starepandas/cloud/ticket_sizing.py`` (C-4 Part B).

Why vendored: the Lambdas need only this pure §C2 sizing function, but
importing it from the installed package would pull in all of
``starepandas`` (and transitively ``pystare`` / ``rasterio``) via the
package ``__init__`` — bloating the deploy bundle and cold-start. This is
a verbatim copy of the source logic; keep the two in sync if either
changes. There is a parity unit test (``tests/test_control_plane_lambdas.py``)
asserting this copy matches the library implementation.

Source of truth: ``starepandas/cloud/ticket_sizing.py``.
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

    ticket_size = max(MIN, min(MAX, ceil(N / W)))   # then chunk into tickets

    N <= W*MAX   -> ceil(N / ticket_size) tickets of ceil(N/W) (>= MIN)
    N >  W*MAX   -> ceil(N / MAX) tickets of MAX (last shorter), work-stolen
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

    ticket_size = max(min_ticket_size, min(max_ticket_size, math.ceil(n / workers)))
    return [uris[i : i + ticket_size] for i in range(0, n, ticket_size)]
