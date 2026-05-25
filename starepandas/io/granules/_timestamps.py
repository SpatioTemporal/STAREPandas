"""Derive a single granule-collection timestamp from a filename.

Used by :func:`starepandas.io.granules.to_s3` as a per-granule
deterministic default for ``raw_collected_time`` (§C10 #2 fix).

Before this, ``STAREDataFrame.to_s3`` defaulted ``raw_collected_time``
to ``datetime.utcnow()`` on every call — meaning two ingest attempts of
the same granule (e.g. after an SQS visibility-timeout redelivery)
produced rows with *different* timestamps, defeating the §C10 #1 UNIQUE
constraint that's supposed to dedup them.

The fix: derive a deterministic timestamp from the granule's filename
*before* the call. The patterns below cover the instruments this
project targets (GMI, AMSR2, SSMIS, ATMS) plus MODIS.

If none match, :exc:`CannotDeriveTimestampError` is raised — the caller
must either pass ``raw_collected_time`` explicitly or add a new pattern
here.
"""

from __future__ import annotations

import datetime
import os
import re


class CannotDeriveTimestampError(ValueError):
    """Filename did not match any known granule timestamp pattern."""


# (regex, strptime format, extract callable)
# Ordered most-specific first. ``extract(m)`` concatenates the named groups
# into a string ``strptime`` can parse with the given format.
_PATTERNS = [
    # GMI / SSMIS (GES-DISC style):
    # 1A.GPM.GMI.COUNT2021.YYYYMMDD-SHHMMSS-EHHMMSS.NNNNNN.VVV.HDF5
    # 1C.F17.SSMIS.XCAL2021.YYYYMMDD-SHHMMSS-EHHMMSS.NNNNNN.VVV.HDF5
    (
        re.compile(r"\.(?P<date>\d{8})-S(?P<time>\d{6})-E\d{6}\."),
        "%Y%m%d%H%M%S",
        lambda m: m.group("date") + m.group("time"),
    ),
    # ATMS (NPP/JPSS style):
    # SATMS_npp_dYYYYMMDD_tHHMMSSS_eHHMMSSS_b…_c…_…ops.h5
    # (the trailing digit of tHHMMSSS is tenths-of-second; we keep HHMMSS.)
    (
        re.compile(r"_d(?P<date>\d{8})_t(?P<time>\d{6})"),
        "%Y%m%d%H%M%S",
        lambda m: m.group("date") + m.group("time"),
    ),
    # AMSR2 (JAXA style):
    # GW1AM2_YYYYMMDDhhmm_NNNNN_…
    (
        re.compile(r"GW1AM2_(?P<datetime>\d{12})_"),
        "%Y%m%d%H%M",
        lambda m: m.group("datetime"),
    ),
    # MODIS (NASA style):
    # MOD05_L2.AYYYYDDD.HHMM.061.YYYYDDDhhmmss.hdf  (DDD = day-of-year)
    (
        re.compile(r"\.A(?P<year>\d{4})(?P<doy>\d{3})\.(?P<time>\d{4})\."),
        "%Y%j%H%M",
        lambda m: m.group("year") + m.group("doy") + m.group("time"),
    ),
]


def derive_timestamp_from_path(path: str) -> datetime.datetime:
    """Parse a granule filename and return its collection timestamp (naive UTC).

    Recognised patterns:

    * **GMI / SSMIS:** ``…YYYYMMDD-SHHMMSS-EHHMMSS…`` (e.g.
      ``1A.GPM.GMI…20240115-S000000-E001234…``)
    * **ATMS:** ``…_dYYYYMMDD_tHHMMSS…`` (e.g.
      ``SATMS_npp_d20240115_t000000_…``)
    * **AMSR2:** ``GW1AM2_YYYYMMDDhhmm_…``
    * **MODIS:** ``…AYYYYDDD.HHMM…`` (Julian day-of-year)

    Parameters
    ----------
    path : str
        Granule file path or URI. Only the basename is examined.

    Returns
    -------
    datetime.datetime
        Naive UTC timestamp derived from the filename.

    Raises
    ------
    CannotDeriveTimestampError
        Filename did not match any known pattern. The caller should pass
        ``raw_collected_time`` explicitly or add a pattern here.
    """
    basename = os.path.basename(path)
    for pattern, fmt, extract in _PATTERNS:
        m = pattern.search(basename)
        if m:
            return datetime.datetime.strptime(extract(m), fmt)
    raise CannotDeriveTimestampError(
        f"Cannot derive timestamp from filename '{basename}'. "
        f"Pass raw_collected_time= explicitly or add a pattern to "
        f"starepandas/io/granules/_timestamps.py"
    )
