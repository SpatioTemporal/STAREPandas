"""VENDORED copy of ``starepandas/io/granules/_timestamps.py`` (C-4 Part B).

The scheduler stamps ``raw_collected_time`` per granule from its filename
(§C10 #2) so the worker's ON CONFLICT dedup key is deterministic across
SQS redeliveries. Vendored to avoid importing the full ``starepandas``
package into the Lambda. Verbatim copy — keep in sync with the source;
a parity unit test guards the two.

Source of truth: ``starepandas/io/granules/_timestamps.py``.
"""

from __future__ import annotations

import datetime
import os
import re


class CannotDeriveTimestampError(ValueError):
    """Filename did not match any known granule timestamp pattern."""


# (regex, strptime format, extract callable) — most-specific first.
_PATTERNS = [
    # GMI / SSMIS: ….YYYYMMDD-SHHMMSS-EHHMMSS.…
    (
        re.compile(r"\.(?P<date>\d{8})-S(?P<time>\d{6})-E\d{6}\."),
        "%Y%m%d%H%M%S",
        lambda m: m.group("date") + m.group("time"),
    ),
    # ATMS: …_dYYYYMMDD_tHHMMSS…
    (
        re.compile(r"_d(?P<date>\d{8})_t(?P<time>\d{6})"),
        "%Y%m%d%H%M%S",
        lambda m: m.group("date") + m.group("time"),
    ),
    # AMSR2: GW1AM2_YYYYMMDDhhmm_…
    (
        re.compile(r"GW1AM2_(?P<datetime>\d{12})_"),
        "%Y%m%d%H%M",
        lambda m: m.group("datetime"),
    ),
    # MODIS: …AYYYYDDD.HHMM… (DDD = day-of-year)
    (
        re.compile(r"\.A(?P<year>\d{4})(?P<doy>\d{3})\.(?P<time>\d{4})\."),
        "%Y%j%H%M",
        lambda m: m.group("year") + m.group("doy") + m.group("time"),
    ),
]


def derive_timestamp_from_path(path: str) -> datetime.datetime:
    """Parse a granule filename and return its collection timestamp (naive UTC).

    Raises CannotDeriveTimestampError if no known pattern matches.
    """
    basename = os.path.basename(path)
    for pattern, fmt, extract in _PATTERNS:
        m = pattern.search(basename)
        if m:
            return datetime.datetime.strptime(extract(m), fmt)
    raise CannotDeriveTimestampError(
        f"Cannot derive timestamp from filename '{basename}'. "
        f"Pass raw_collected_time explicitly or add a pattern."
    )
