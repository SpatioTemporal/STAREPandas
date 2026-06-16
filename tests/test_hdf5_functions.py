"""
Tests for STAREDataFrame.to_hdf5() and reconstitute_hdf5_from_s3().

These tests exercise the Parquet → HDF5 reconstitution pipeline introduced in
plan.md ("Reconstitute HDF5 from Parquet").

Test inventory
--------------
TestToHdf5Structure          — dataset names, dtypes, shapes, group name, attrs
TestToHdf5DataFidelity       — lat/lon/Tc values preserved; Tc column order; return value
TestToHdf5ScanTime           — 7 int16 1D fields; values correct
TestToHdf5EdgeCases          — missing params, truncation, no Tc, no timestamp
TestToHdf5OutputDirectory    — auto-creates parent directory
TestToLocalPixelWidth    — pixel_width stored/absent in Parquet kv-metadata
TestReconstituteFromParquet     — full integration round-trips (area_sids & bbox),
                               pixel_width resolution chain, error cases
TestReconstituteScanNameExtraction — dataset → scan group name for each instrument
TestScanPixelWidths          — SCAN_PIXEL_WIDTHS constant values
"""
import os
import shutil
import tempfile
import warnings

import numpy as np
import pandas as pd
import pytest

import starepandas
from starepandas import STAREDataFrame, reconstitute_hdf5_from_s3
from starepandas.io.granules import SCAN_PIXEL_WIDTHS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_sdf(n_scans=3, pixel_width=5, n_tc=2, with_timestamp=True, with_sids=True):
    """Build a synthetic STAREDataFrame with GMI-like columns."""
    import pystare

    N = n_scans * pixel_width
    lats = np.random.uniform(32, 42, N)
    lons = np.random.uniform(-125, -115, N)
    data = {
        'lat': lats,
        'lon': lons,
    }
    if with_sids:
        data['sids'] = pystare.from_latlon(lats, lons, level=6)
    else:
        data['sids'] = np.zeros(N, dtype=np.int64)

    for i in range(1, n_tc + 1):
        data[f'Tc{i}'] = np.random.rand(N).astype(np.float32)

    if with_timestamp:
        timestamps = []
        for s in range(n_scans):
            ts = pd.Timestamp('2020-06-15 12:00:00') + pd.Timedelta(seconds=s)
            timestamps.extend([ts] * pixel_width)
        data['timestamp'] = timestamps

    sdf = STAREDataFrame(data)
    sdf._sid_column_name = 'sids'
    return sdf, n_scans, pixel_width


# ---------------------------------------------------------------------------
# STAREDataFrame.to_hdf5() — unit tests
# ---------------------------------------------------------------------------

class TestToHdf5Structure:
    """Verify exact HDF5 dataset names, dtypes, and shapes."""

    def test_exact_dataset_names(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf(n_scans=2, pixel_width=5, n_tc=2)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            assert 'S1' in f
            keys = set(f['S1'].keys())
            assert 'Latitude' in keys
            assert 'Longitude' in keys
            assert 'Tc' in keys
            assert 'ScanTime' in keys
            # STARE_index must NOT be written (not in original format)
            assert 'STARE_index' not in keys

    def test_latitude_longitude_dtype_and_shape(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf(n_scans=3, pixel_width=5, n_tc=2)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            # float32 matches the original GPM/SSMIS L1C granule dtype.
            assert f['S1']['Latitude'].dtype == np.float32
            assert f['S1']['Longitude'].dtype == np.float32
            assert f['S1']['Latitude'].shape == (n_scans, pw)
            assert f['S1']['Longitude'].shape == (n_scans, pw)

    def test_tc_3d_shape_and_dtype(self, tmp_path):
        import h5py
        n_tc = 3
        sdf, n_scans, pw = _make_sdf(n_scans=2, pixel_width=5, n_tc=n_tc)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            tc = f['S1']['Tc']
            assert tc.dtype == np.float32
            assert tc.shape == (n_scans, pw, n_tc)

    def test_scan_group_name_respected(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf()
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S3", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            assert 'S3' in f
            assert 'S1' not in f

    def test_group_attributes_written(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf()
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            attrs = dict(f['S1'].attrs)
            assert attrs.get('StarePodsReconstitution')  # h5py may return numpy.bool_
            assert attrs.get('PixelWidth') == pw


class TestToHdf5ScanTime:
    """Verify ScanTime subgroup — 7 int16 1D datasets."""

    def test_scantime_datasets_present(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf(n_scans=2, pixel_width=5)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            st = f['S1']['ScanTime']
            for field in ('Year', 'Month', 'DayOfMonth', 'Hour', 'Minute', 'Second', 'MilliSecond'):
                assert field in st, f"Missing ScanTime field: {field}"

    def test_scantime_dtypes(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf(n_scans=2, pixel_width=5)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        # dtypes match the original GPM/SSMIS L1C granule format: Year and
        # MilliSecond are int16; the rest are int8.
        expected = {
            'Year': np.int16, 'Month': np.int8, 'DayOfMonth': np.int8,
            'Hour': np.int8, 'Minute': np.int8, 'Second': np.int8,
            'MilliSecond': np.int16,
        }
        with h5py.File(out, 'r') as f:
            st = f['S1']['ScanTime']
            for field, dt in expected.items():
                assert st[field].dtype == dt, f"Wrong dtype for ScanTime/{field}"

    def test_scantime_1d_length(self, tmp_path):
        import h5py
        n_scans, pw = 4, 5
        sdf, _, _ = _make_sdf(n_scans=n_scans, pixel_width=pw)
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            assert f['S1']['ScanTime']['Year'].shape == (n_scans,)

    def test_scantime_values_correct(self, tmp_path):
        import h5py
        n_scans, pw = 2, 3
        ts_base = pd.Timestamp('2021-08-15 06:30:45')
        timestamps = []
        for s in range(n_scans):
            ts = ts_base + pd.Timedelta(seconds=s)
            timestamps.extend([ts] * pw)

        import pystare
        N = n_scans * pw
        lats = np.zeros(N)
        lons = np.zeros(N)
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'timestamp': timestamps,
        })
        sdf._sid_column_name = 'sids'
        out = str(tmp_path / "test.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            assert f['S1']['ScanTime']['Year'][0] == 2021
            assert f['S1']['ScanTime']['Month'][0] == 8
            assert f['S1']['ScanTime']['DayOfMonth'][0] == 15
            assert f['S1']['ScanTime']['Hour'][0] == 6
            assert f['S1']['ScanTime']['Minute'][0] == 30
            assert f['S1']['ScanTime']['Second'][0] == 45


class TestToHdf5EdgeCases:
    """Truncation, missing columns, invalid inputs."""

    def test_no_pixel_width_raises(self, tmp_path):
        sdf, _, _ = _make_sdf()
        with pytest.raises(ValueError, match="pixel_width is required"):
            sdf.to_hdf5(str(tmp_path / "out.h5"), scan="S1", pixel_width=None)

    def test_no_scan_raises(self, tmp_path):
        sdf, _, pw = _make_sdf()
        with pytest.raises(ValueError, match="scan group name is required"):
            sdf.to_hdf5(str(tmp_path / "out.h5"), scan=None, pixel_width=pw)

    def test_truncation_warning(self, tmp_path):
        import h5py
        # N=11 is not divisible by pixel_width=5 → 1 row truncated
        sdf, _, _ = _make_sdf(n_scans=2, pixel_width=5)
        # Append one extra row
        extra = sdf.iloc[:1].copy()
        extra.index = [len(sdf)]
        sdf_extra = pd.concat([sdf, extra])
        sdf_extra = STAREDataFrame(sdf_extra)
        sdf_extra._sid_column_name = 'sids'

        out = str(tmp_path / "trunc.h5")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sdf_extra.to_hdf5(out, scan="S1", pixel_width=5)
            assert any("truncated" in str(x.message).lower() for x in w)

        with h5py.File(out, 'r') as f:
            assert f['S1']['Latitude'].shape == (2, 5)  # 2 full scans

    def test_no_tc_columns(self, tmp_path):
        import h5py
        import pystare
        N = 10
        lats = np.zeros(N)
        lons = np.zeros(N)
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
        })
        sdf._sid_column_name = 'sids'
        out = str(tmp_path / "no_tc.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=5)

        with h5py.File(out, 'r') as f:
            assert 'Tc' not in f['S1']

    def test_no_timestamp_column(self, tmp_path):
        import h5py
        sdf, n_scans, pw = _make_sdf(with_timestamp=False)
        out = str(tmp_path / "no_ts.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            assert 'ScanTime' not in f['S1']


# ---------------------------------------------------------------------------
# reconstitute_hdf5_from_s3() — integration tests (local Parquet only)
# ---------------------------------------------------------------------------

class TestReconstituteFromParquet:
    """Integration: write Parquet locally, reconstitute HDF5, verify structure.

    Note: many tests in this class produce a UserWarning about truncation.
    This is expected: STARE level-6 partitioning splits rows across groups, so
    the spatial subset recovered by ``reconstitute_hdf5_from_s3`` is rarely an
    exact multiple of ``pixel_width``.  The truncation warning fires inside
    ``to_hdf5()`` and is correct behaviour; it does not indicate a test failure.
    """

    def _write_local_parquet(self, tmp_path, n_scans=3, pixel_width=5, n_tc=2,
                             dataset="GMI_S1"):
        import pystare
        N = n_scans * pixel_width
        lats = np.random.uniform(33, 41, N)
        lons = np.random.uniform(-124, -116, N)
        timestamps = []
        for s in range(n_scans):
            ts = pd.Timestamp('2020-06-15 12:00:00') + pd.Timedelta(seconds=s)
            timestamps.extend([ts] * pixel_width)

        data = {
            'lat': lats,
            'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'timestamp': timestamps,
        }
        for i in range(1, n_tc + 1):
            data[f'Tc{i}'] = np.random.rand(N).astype(np.float32)

        sdf = STAREDataFrame(data)
        sdf._sid_column_name = 'sids'

        parquet_root = str(tmp_path / "parquet_root")
        # Store under the dataset the tests reconstitute (the flat/local layout
        # keys chunks by dataset; reconstitute filters on it).
        sdf.to_local(parquet_root, level=6, pixel_width=pixel_width, dataset=dataset)
        return parquet_root, sdf, n_scans, pixel_width

    def test_reconstitute_with_area_sids(self, tmp_path):
        import h5py, pystare
        parquet_root, _, n_scans, pw = self._write_local_parquet(tmp_path)
        out_hdf5 = str(tmp_path / "out.h5")

        # Broad area SIDs covering California coast
        area_sids = pystare.cover_from_hull(
            [33, 33, 41, 41], [-124, -116, -116, -124], 6
        )
        reconstitute_hdf5_from_s3(
            s3_root=parquet_root,
            dataset="GMI_S1",
            output_hdf5_path=out_hdf5,
            area_sids=area_sids,
            pixel_width=pw,
        )
        assert os.path.exists(out_hdf5)
        with h5py.File(out_hdf5, 'r') as f:
            assert 'S1' in f
            assert 'Latitude' in f['S1']
            assert 'Longitude' in f['S1']

    def test_reconstitute_with_bbox(self, tmp_path):
        import h5py
        parquet_root, _, n_scans, pw = self._write_local_parquet(tmp_path)
        out_hdf5 = str(tmp_path / "out_bbox.h5")

        reconstitute_hdf5_from_s3(
            s3_root=parquet_root,
            dataset="GMI_S1",
            output_hdf5_path=out_hdf5,
            bbox=(-124, 33, -116, 41),
            pixel_width=pw,
        )
        assert os.path.exists(out_hdf5)
        with h5py.File(out_hdf5, 'r') as f:
            assert 'S1' in f

    def test_reconstitute_scantime_structure(self, tmp_path):
        import h5py
        parquet_root, _, n_scans, pw = self._write_local_parquet(tmp_path)
        out_hdf5 = str(tmp_path / "out_st.h5")

        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out_hdf5,
            bbox=(-124, 33, -116, 41),
            pixel_width=pw,
        )
        with h5py.File(out_hdf5, 'r') as f:
            st = f['S1']['ScanTime']
            # Year/MilliSecond int16, the rest int8 (original L1C granule format).
            expected = {
                'Year': np.int16, 'Month': np.int8, 'DayOfMonth': np.int8,
                'Hour': np.int8, 'Minute': np.int8, 'Second': np.int8,
                'MilliSecond': np.int16,
            }
            for field, dt in expected.items():
                assert field in st
                assert st[field].dtype == dt

    def test_reconstitute_tc_3d(self, tmp_path):
        import h5py
        n_tc = 2
        parquet_root, _, n_scans, pw = self._write_local_parquet(tmp_path, n_tc=n_tc)
        out_hdf5 = str(tmp_path / "out_tc.h5")

        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out_hdf5,
            bbox=(-124, 33, -116, 41),
            pixel_width=pw,
        )
        with h5py.File(out_hdf5, 'r') as f:
            assert f['S1']['Tc'].dtype == np.float32
            assert f['S1']['Tc'].ndim == 3
            assert f['S1']['Tc'].shape[2] == n_tc

    def test_pixel_width_attrs(self, tmp_path):
        """pixel_width stored in Parquet kv-metadata → no need to pass explicitly."""
        import h5py
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path, pixel_width=5)
        out_hdf5 = str(tmp_path / "out_pw.h5")

        # Do NOT pass pixel_width — should be read from Parquet kv-metadata
        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out_hdf5,
            bbox=(-124, 33, -116, 41),
        )
        with h5py.File(out_hdf5, 'r') as f:
            assert f['S1']['Latitude'].ndim == 2

    def test_pixel_width_from_scan_pixel_widths_table(self, tmp_path):
        """Fallback: pixel_width from SCAN_PIXEL_WIDTHS when Parquet kv-metadata absent."""
        import h5py
        # Write Parquet WITHOUT pixel_width in kv-metadata by using STAREDataFrame directly
        # (without pixel_width param so kv-metadata is not set)
        import pystare
        N = 3 * 221
        lats = np.random.uniform(33, 41, N)
        lons = np.random.uniform(-124, -116, N)
        timestamps = [pd.Timestamp('2020-06-15') + pd.Timedelta(seconds=i // 221) for i in range(N)]
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'Tc1': np.random.rand(N).astype(np.float32),
            'timestamp': timestamps,
        })
        sdf._sid_column_name = 'sids'
        parquet_root = str(tmp_path / "no_pw")
        # pixel_width=None → kv-metadata not written
        sdf.to_local(parquet_root, level=6, dataset="GMI_S1")

        out_hdf5 = str(tmp_path / "out_fallback.h5")
        # SCAN_PIXEL_WIDTHS["GMI_S1"] == 221 → used as fallback
        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out_hdf5,
            bbox=(-124, 33, -116, 41),
        )
        with h5py.File(out_hdf5, 'r') as f:
            assert f['S1']['Latitude'].ndim == 2

    def test_reconstitute_no_area_sids_or_bbox_is_full_granule(self, tmp_path):
        """Neither area_sids nor bbox is now valid (task-13: full granule). With
        no data present it raises a clear 'no partitions' error, not a
        validation error."""
        with pytest.raises(ValueError, match="No Parquet partitions"):
            reconstitute_hdf5_from_s3(
                str(tmp_path), "GMI_S1", str(tmp_path / "out.h5"),
            )

    def test_reconstitute_both_area_sids_and_bbox_raises(self, tmp_path):
        import pystare
        area_sids = pystare.cover_from_hull([33, 33, 41, 41], [-124, -116, -116, -124], 6)
        with pytest.raises(ValueError, match="not both"):
            reconstitute_hdf5_from_s3(
                str(tmp_path), "GMI_S1", str(tmp_path / "out.h5"),
                area_sids=area_sids, bbox=(-124, 33, -116, 41),
            )

    def test_reconstitute_no_matching_data_raises(self, tmp_path):
        import pystare
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path)
        # Query an area far from the data (South Pacific)
        with pytest.raises(ValueError):
            reconstitute_hdf5_from_s3(
                parquet_root, "GMI_S1", str(tmp_path / "out.h5"),
                bbox=(170, -50, 180, -40),
                pixel_width=pw,
            )

    def test_reconstitute_bad_dataset_name_no_scan_suffix_raises(self, tmp_path):
        # Store under "GMI" so the reconstitute finds the data and then fails at
        # scan-group-name derivation (the behaviour under test), not earlier.
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path, dataset="GMI")
        with pytest.raises(ValueError, match="scan group name"):
            reconstitute_hdf5_from_s3(
                parquet_root, "GMI",  # no _S1 suffix
                str(tmp_path / "out.h5"),
                bbox=(-124, 33, -116, 41),
                pixel_width=pw,
            )


# ---------------------------------------------------------------------------
# SCAN_PIXEL_WIDTHS constant
# ---------------------------------------------------------------------------

class TestScanPixelWidths:
    def test_known_instruments_present(self):
        for key in ("GMI_S1", "GMI_S2", "SSMIS_S1", "SSMIS_S3",
                    "AMSR2_S1", "AMSR2_S5"):
            assert key in SCAN_PIXEL_WIDTHS

    def test_gmi_pixel_width(self):
        assert SCAN_PIXEL_WIDTHS["GMI_S1"] == 221
        assert SCAN_PIXEL_WIDTHS["GMI_S2"] == 221

    def test_ssmis_pixel_widths(self):
        assert SCAN_PIXEL_WIDTHS["SSMIS_S1"] == 90
        assert SCAN_PIXEL_WIDTHS["SSMIS_S3"] == 180

    def test_amsr2_pixel_widths(self):
        assert SCAN_PIXEL_WIDTHS["AMSR2_S1"] == 243
        assert SCAN_PIXEL_WIDTHS["AMSR2_S5"] == 486


# ---------------------------------------------------------------------------
# Data fidelity — to_hdf5() preserves actual values
# ---------------------------------------------------------------------------

class TestToHdf5DataFidelity:
    """Verify that lat/lon/Tc values are numerically preserved."""

    def test_latitude_values_preserved(self, tmp_path):
        import h5py
        n_scans, pw = 2, 4
        lats = np.array([10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0])
        lons = np.zeros(n_scans * pw)
        import pystare
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
        })
        sdf._sid_column_name = 'sids'
        out = str(tmp_path / "fid.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            lat_2d = f['S1']['Latitude'][:]
            np.testing.assert_allclose(lat_2d.ravel(), lats, rtol=1e-10)

    def test_longitude_values_preserved(self, tmp_path):
        import h5py
        n_scans, pw = 2, 3
        lons = np.array([-120.0, -119.5, -119.0, -118.5, -118.0, -117.5])
        lats = np.zeros(n_scans * pw)
        import pystare
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
        })
        sdf._sid_column_name = 'sids'
        out = str(tmp_path / "lon_fid.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            np.testing.assert_allclose(f['S1']['Longitude'][:].ravel(), lons, rtol=1e-10)

    def test_tc_values_preserved_and_channel_order(self, tmp_path):
        """Tc1 must map to channel 0, Tc2 to channel 1, etc."""
        import h5py
        n_scans, pw = 2, 3
        N = n_scans * pw
        import pystare
        tc1 = np.arange(N, dtype=np.float32) * 1.0
        tc2 = np.arange(N, dtype=np.float32) * 2.0
        tc3 = np.arange(N, dtype=np.float32) * 3.0
        lats = np.zeros(N)
        lons = np.zeros(N)
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'Tc1': tc1, 'Tc2': tc2, 'Tc3': tc3,
        })
        sdf._sid_column_name = 'sids'
        out = str(tmp_path / "tc_order.h5")
        sdf.to_hdf5(out, scan="S1", pixel_width=pw)

        with h5py.File(out, 'r') as f:
            tc = f['S1']['Tc'][:]  # (N_scans, pw, 3)
            np.testing.assert_allclose(tc[:, :, 0].ravel(), tc1, rtol=1e-6,
                                        err_msg="Tc1 not in channel 0")
            np.testing.assert_allclose(tc[:, :, 1].ravel(), tc2, rtol=1e-6,
                                        err_msg="Tc2 not in channel 1")
            np.testing.assert_allclose(tc[:, :, 2].ravel(), tc3, rtol=1e-6,
                                        err_msg="Tc3 not in channel 2")

    def test_return_value_is_file_path(self, tmp_path):
        sdf, _, pw = _make_sdf()
        out = str(tmp_path / "ret.h5")
        result = sdf.to_hdf5(out, scan="S1", pixel_width=pw)
        assert result == out


# ---------------------------------------------------------------------------
# Output directory auto-creation
# ---------------------------------------------------------------------------

class TestToHdf5OutputDirectory:
    def test_creates_parent_directory(self, tmp_path):
        import h5py
        sdf, _, pw = _make_sdf()
        nested = str(tmp_path / "a" / "b" / "c" / "out.h5")
        sdf.to_hdf5(nested, scan="S1", pixel_width=pw)
        assert os.path.exists(nested)
        with h5py.File(nested, 'r') as f:
            assert 'S1' in f


# ---------------------------------------------------------------------------
# to_local() pixel_width attr persistence
# ---------------------------------------------------------------------------

def _walk_parquet_leaves(root):
    """Yield every ``*.parquet`` path under ``root``."""
    for d, _, files in os.walk(root):
        for f in files:
            if f.endswith('.parquet'):
                yield os.path.join(d, f)


class TestToLocalPixelWidth:
    """Verify that pixel_width is correctly stored/absent in Parquet kv-metadata."""

    def test_pixel_width_stored_in_attrs(self, tmp_path):
        import pyarrow.parquet as pq
        sdf, _, _ = _make_sdf(n_scans=2, pixel_width=5)
        parquet_root = str(tmp_path / "with_pw")
        sdf.to_local(parquet_root, level=6, pixel_width=5)

        found_any = False
        for pq_path in _walk_parquet_leaves(parquet_root):
            md = pq.ParquetFile(pq_path).schema_arrow.metadata or {}
            assert b'pixel_width' in md, (
                f"pixel_width missing from Parquet kv-metadata in {pq_path}"
            )
            assert int(md[b'pixel_width'].decode()) == 5
            found_any = True
        assert found_any, "No Parquet leaves were written"

    def test_pixel_width_absent_when_not_passed(self, tmp_path):
        import pyarrow.parquet as pq
        sdf, _, _ = _make_sdf(n_scans=2, pixel_width=5)
        parquet_root = str(tmp_path / "no_pw")
        sdf.to_local(parquet_root, level=6)  # no pixel_width

        for pq_path in _walk_parquet_leaves(parquet_root):
            md = pq.ParquetFile(pq_path).schema_arrow.metadata or {}
            assert b'pixel_width' not in md, (
                f"pixel_width unexpectedly found in {pq_path}"
            )

    def test_columns_and_row_positions_written(self, tmp_path):
        """Sanity: all data columns + __row_positions__ appear in each Parquet leaf."""
        import pyarrow.parquet as pq
        sdf, _, _ = _make_sdf(n_scans=2, pixel_width=5, n_tc=2)
        parquet_root = str(tmp_path / "cols")
        sdf.to_local(parquet_root, level=6, pixel_width=5)

        for pq_path in _walk_parquet_leaves(parquet_root):
            names = set(pq.ParquetFile(pq_path).schema.names)
            assert '__row_positions__' in names
            for col in sdf.columns:
                assert col in names, f"Column '{col}' missing from {pq_path}"


# ---------------------------------------------------------------------------
# reconstitute_hdf5_from_s3() return value and data fidelity
# ---------------------------------------------------------------------------

class TestReconstituteReturnAndFidelity:
    """Return value and that lat/lon round-trip through Parquet → HDF5."""

    def _write_local_parquet(self, tmp_path, n_scans=3, pixel_width=5):
        import pystare
        N = n_scans * pixel_width
        lats = np.random.uniform(33, 41, N)
        lons = np.random.uniform(-124, -116, N)
        timestamps = []
        for s in range(n_scans):
            ts = pd.Timestamp('2020-06-15 12:00:00') + pd.Timedelta(seconds=s)
            timestamps.extend([ts] * pixel_width)
        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'Tc1': np.ones(N, dtype=np.float32),
            'timestamp': timestamps,
        })
        sdf._sid_column_name = 'sids'
        parquet_root = str(tmp_path / "parquet_root")
        # Store under the dataset the tests reconstitute (the flat/local layout
        # keys chunks by dataset; reconstitute filters on it).
        sdf.to_local(parquet_root, level=6, pixel_width=pixel_width, dataset="GMI_S1")
        return parquet_root, sdf, n_scans, pixel_width

    def test_return_value_is_output_path(self, tmp_path):
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path)
        out = str(tmp_path / "out.h5")
        result = reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out, bbox=(-124, 33, -116, 41), pixel_width=pw
        )
        assert result == out

    def test_lat_lon_values_in_valid_range(self, tmp_path):
        """Reconstituted lat/lon should stay within the original write range."""
        import h5py
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path)
        out = str(tmp_path / "out.h5")
        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out, bbox=(-124, 33, -116, 41), pixel_width=pw
        )
        with h5py.File(out, 'r') as f:
            lats = f['S1']['Latitude'][:]
            lons = f['S1']['Longitude'][:]
            assert np.all(lats >= 33) and np.all(lats <= 41)
            assert np.all(lons >= -124) and np.all(lons <= -116)

    def test_tc_values_uniform_ones_preserved(self, tmp_path):
        """Tc1=1.0 everywhere → all values in reconstituted HDF5 must be 1.0."""
        import h5py
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path)
        out = str(tmp_path / "out.h5")
        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out, bbox=(-124, 33, -116, 41), pixel_width=pw
        )
        with h5py.File(out, 'r') as f:
            np.testing.assert_allclose(f['S1']['Tc'][:], 1.0, rtol=1e-6)

    def test_output_file_created(self, tmp_path):
        parquet_root, _, _, pw = self._write_local_parquet(tmp_path)
        out = str(tmp_path / "created.h5")
        reconstitute_hdf5_from_s3(
            parquet_root, "GMI_S1", out, bbox=(-124, 33, -116, 41), pixel_width=pw
        )
        assert os.path.isfile(out)


# ---------------------------------------------------------------------------
# reconstitute_hdf5_from_s3() scan group name extraction
# ---------------------------------------------------------------------------

class TestReconstituteScanNameExtraction:
    """Verify the scan group name (S1–S6) is correctly extracted for each instrument."""

    @pytest.mark.parametrize("dataset,expected_scan", [
        ("GMI_S1",   "S1"),
        ("GMI_S2",   "S2"),
        ("SSMIS_S1", "S1"),
        ("SSMIS_S2", "S2"),
        ("SSMIS_S3", "S3"),
        ("SSMIS_S4", "S4"),
        ("AMSR2_S1", "S1"),
        ("AMSR2_S5", "S5"),
        ("AMSR2_S6", "S6"),
    ])
    def test_scan_name_extracted(self, tmp_path, dataset, expected_scan):
        """Write data, reconstitute with given dataset name, verify HDF5 group name."""
        import h5py, pystare

        pw = SCAN_PIXEL_WIDTHS.get(dataset, 5)
        N = 3 * pw
        lats = np.random.uniform(33, 41, N)
        lons = np.random.uniform(-124, -116, N)
        timestamps = []
        for s in range(3):
            ts = pd.Timestamp('2020-01-01') + pd.Timedelta(seconds=s)
            timestamps.extend([ts] * pw)

        sdf = STAREDataFrame({
            'lat': lats, 'lon': lons,
            'sids': pystare.from_latlon(lats, lons, level=6),
            'Tc1': np.random.rand(N).astype(np.float32),
            'timestamp': timestamps,
        })
        sdf._sid_column_name = 'sids'
        parquet_root = str(tmp_path / f"pq_{dataset}")
        sdf.to_local(parquet_root, level=6, pixel_width=pw, dataset=dataset)

        out = str(tmp_path / f"out_{dataset}.h5")
        reconstitute_hdf5_from_s3(
            parquet_root, dataset, out,
            bbox=(-124, 33, -116, 41),
            pixel_width=pw,
        )

        with h5py.File(out, 'r') as f:
            assert expected_scan in f, (
                f"Expected scan group '{expected_scan}' not found in HDF5 "
                f"for dataset '{dataset}'. Groups present: {list(f.keys())}"
            )

    def test_nonexistent_local_path_raises(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            reconstitute_hdf5_from_s3(
                str(tmp_path / "nonexistent"), "GMI_S1",
                str(tmp_path / "out.h5"),
                bbox=(-124, 33, -116, 41),
            )
