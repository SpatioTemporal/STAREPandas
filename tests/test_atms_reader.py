"""ATMS granule reader regression tests.

The ATMS reader is exercised by the demo ingest (it is one of the four
instruments in the overlap analytics), and both defects below made that
ingest fail outright:

* ``read_timestamps()`` indexed ``self.lat``, but callers run it *before*
  ``read_latlon()`` (see :func:`starepandas.io.granules.read_granule`), so
  ``self.lat`` was still ``None`` -> ``TypeError: argument of type
  'NoneType' is not iterable``. It also returned one timestamp per scan
  *line* rather than per pixel, which cannot line up with the flattened
  latitude grid ``to_df`` builds.
* ``read_data()`` read from a ``self.netcdf`` attribute this reader never
  sets (only the NetCDF-backed VIIRS/IMERG readers have one) -> ``AttributeError``.
"""

import os

import numpy
import pytest

from starepandas.io.granules import granule_factory, read_granule
from starepandas.io.granules.atms import ATMS

GRANULE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "granules",
    "1C.NOAA21.ATMS.XCAL2023-V.20250101-S201707-E215835.011117.V07A.HDF5",
)

pytestmark = pytest.mark.skipif(
    not os.path.exists(GRANULE), reason="ATMS sample granule not available"
)


@pytest.fixture
def granule():
    atms = ATMS(GRANULE, scans=['S1', 'S2'])
    yield atms
    atms.close()


def test_read_timestamps_before_read_latlon(granule):
    """Timestamps must be readable without latitude having been loaded."""
    assert granule.lat is None
    granule.read_timestamps()
    assert set(granule.timestamps) == {'S1', 'S2'}


def test_timestamp_grid_matches_latitude_grid(granule):
    """One timestamp per pixel, so ``to_df`` can flatten them together."""
    granule.read_timestamps()
    granule.read_latlon()
    for scan in granule.scans:
        assert granule.timestamps[scan].shape == granule.lat[scan].shape


def test_timestamps_constant_along_a_scan_line(granule):
    """A scan line is one sweep: every pixel in it shares its timestamp."""
    granule.read_timestamps()
    for scan in granule.scans:
        row = granule.timestamps[scan][0]
        assert len(set(row)) == 1
    # ... and distinct lines advance in time.
    first_column = granule.timestamps['S1'][:, 0]
    assert first_column[0] < first_column[-1]


def test_read_data_populates_channels(granule):
    """Brightness temperatures come off the backing file, not ``self.netcdf``."""
    granule.read_data()
    for scan in granule.scans:
        channels = granule.data[scan]
        assert channels, f"no channels read for {scan}"
        assert set(channels) <= {f'Tc{n}' for n in range(1, 7)}
        expected = granule.scan_variable(scan, 'Latitude').shape
        for values in channels.values():
            assert values.shape == expected


def test_to_df_columns_are_aligned(granule):
    """Every column flattens to the same length — the shape bug's real symptom."""
    granule.read_timestamps()
    granule.read_latlon()
    granule.read_data()
    granule.sids = None
    frames = granule.to_df()
    for scan, df in frames.items():
        assert 'timestamp' in df.columns
        assert len(df) == numpy.prod(granule.lat[scan].shape)


def test_all_four_scan_groups_read():
    """The D2 bulk-ingest shape: S1–S4 (bands K/Ka/W/G) all read cleanly."""
    atms = ATMS(GRANULE, scans=['S1', 'S2', 'S3', 'S4'])
    try:
        atms.read_timestamps()
        atms.read_latlon()
        atms.read_data()
        assert set(atms.timestamps) == {'S1', 'S2', 'S3', 'S4'}
        for scan in atms.scans:
            assert atms.timestamps[scan].shape == atms.lat[scan].shape
            assert atms.data[scan], f"no channels read for {scan}"
        # The G band (S4) carries six 165–183 GHz channels.
        assert len(atms.data['S4']) == 6
    finally:
        atms.close()


def test_granule_factory_forwards_reader_kwargs():
    """``granule_factory(..., scans=…)`` must reach the reader's constructor."""
    granule = granule_factory(GRANULE, scans=['S1', 'S3'])
    try:
        assert granule.scans == ['S1', 'S3']
    finally:
        granule.close()


def test_read_granule_reader_kwargs_reaches_scans():
    """``reader_kwargs`` threads from read_granule through the factory — the
    seam the cloud ticket ``options`` path relies on for the S1–S4 ingest."""
    frames = read_granule(GRANULE, read_timestamp=True,
                          reader_kwargs={'scans': ['S3', 'S4']})
    assert set(frames) == {'S3', 'S4'}
