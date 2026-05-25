"""Tests for starepandas.io.granules._timestamps (§C10 #2 fix)."""

import datetime

import pytest

from starepandas.io.granules._timestamps import (
    CannotDeriveTimestampError,
    derive_timestamp_from_path,
)


# ----- Per-instrument filename patterns -----------------------------------


def test_gmi_filename():
    ts = derive_timestamp_from_path(
        "1A.GPM.GMI.COUNT2021.20240115-S034512-E041234.058432.V07A.HDF5"
    )
    assert ts == datetime.datetime(2024, 1, 15, 3, 45, 12)


def test_ssmis_filename_uses_gmi_pattern():
    """SSMIS uses the same GES-DISC pattern as GMI."""
    ts = derive_timestamp_from_path(
        "1C.F17.SSMIS.XCAL2021.20230612-S120000-E121234.012345.V07A.HDF5"
    )
    assert ts == datetime.datetime(2023, 6, 12, 12, 0, 0)


def test_atms_filename():
    ts = derive_timestamp_from_path(
        "SATMS_npp_d20210109_t1834297_e1834613_b48092_"
        "c20210109184141357000_oeac_ops.h5"
    )
    # t1834297 → HH=18, MM=34, SS=29 (last digit is tenths, dropped)
    assert ts == datetime.datetime(2021, 1, 9, 18, 34, 29)


def test_amsr2_filename():
    ts = derive_timestamp_from_path("GW1AM2_202401151234_001A_L1SGRTBR_2230230.h5")
    assert ts == datetime.datetime(2024, 1, 15, 12, 34)


def test_modis_filename():
    # MOD05_L2.A2019336.0000.061.2019336211522.hdf
    # 2019336 = Dec 2, 2019 (day-of-year 336)
    ts = derive_timestamp_from_path(
        "MOD05_L2.A2019336.0000.061.2019336211522.hdf"
    )
    assert ts == datetime.datetime(2019, 12, 2, 0, 0)


# ----- Path handling ------------------------------------------------------


def test_full_path_uses_basename_only():
    ts = derive_timestamp_from_path(
        "/some/long/dir/1A.GPM.GMI.X.20240115-S000000-E001234.000.V07A.HDF5"
    )
    assert ts == datetime.datetime(2024, 1, 15)


def test_s3_uri_uses_basename_only():
    ts = derive_timestamp_from_path(
        "s3://bucket/path/1A.GPM.GMI.X.20240115-S000000-E001234.000.V07A.HDF5"
    )
    assert ts == datetime.datetime(2024, 1, 15)


# ----- Failure mode -------------------------------------------------------


def test_unknown_filename_raises():
    with pytest.raises(CannotDeriveTimestampError, match="some_random_file"):
        derive_timestamp_from_path("some_random_file.h5")


def test_unknown_filename_error_suggests_remediation():
    with pytest.raises(CannotDeriveTimestampError, match="raw_collected_time"):
        derive_timestamp_from_path("garbage.bin")


def test_cannot_derive_is_value_error():
    """CannotDeriveTimestampError should be a ValueError subclass so existing
    ``except ValueError`` blocks catch it naturally."""
    assert issubclass(CannotDeriveTimestampError, ValueError)
