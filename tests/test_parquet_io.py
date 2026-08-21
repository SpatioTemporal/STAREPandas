import os
import tempfile
import shutil
import numpy as np
import pandas as pd
import starepandas as sp
from shapely.geometry import Point
import pytest

def test_parquet_local_functions():
    """Test local Parquet storage functions"""
    
    # Create a test STAREDataFrame with geometry and SIDs
    data = {
        'lat': [0, 1, 2, 3, 4],
        'lon': [0, 1, 2, 3, 4],
        'data': [1, 2, 3, 4, 5],
        'category': ['A', 'B', 'A', 'B', 'A']
    }
    
    sdf = sp.STAREDataFrame(data)
    
    # Add geometry column
    sdf['geometry'] = [Point(lon, lat) for lon, lat in zip(sdf['lon'], sdf['lat'])]
    sdf = sdf.set_geometry('geometry')
    
    # Create SIDs
    sids = sdf.make_sids(level=10)
    sdf['sids'] = sids
    
    print(f"Original STAREDataFrame shape: {sdf.shape}")
    print(f"Columns: {sdf.columns.tolist()}")
    print(f"Has SIDs: {sdf.has_sids()}")
    
    # Create temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, "test_granule")
        
        print(f"Testing to_local...")
        # Test writing to local Parquet
        written_path = sdf.to_local(local_path, level=10, chunk_size=1000)
        print(f"Written to: {written_path}")
        
        print(f"Testing from_local...")
        # Test reading from local Parquet
        sdf_read = sp.STAREDataFrame.from_local(local_path)
        print(f"Read STAREDataFrame shape: {sdf_read.shape}")
        print(f"Read columns: {sdf_read.columns.tolist()}")
        
        # Verify data integrity (ignore column order)
        assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
        assert set(sdf.columns) == set(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
        
        # Check that data values match for each column by name (excluding geometry which might be serialized differently)
        for col in sdf.columns:
            if col != 'geometry':
                assert sdf[col].equals(sdf_read[col]), f"Column {col} data mismatch"

        # Verify the quaternary pod-code hierarchy was created on disk
        # (guards against silent regressions of the local layout).
        from pathlib import Path
        pod_dirs = list(Path(local_path).glob("q*"))
        assert pod_dirs, f"Expected top-level pod-code (q*) directories under {local_path}"
        # Default fallback dataset_name="data" and granule="data" since this
        # test passes neither. Each leaf is a self-describing pod-code file.
        parquet_leaves = list(Path(local_path).rglob("q*-*-data.parquet"))
        assert parquet_leaves, "Expected at least one pod-code Parquet chunk leaf"
        # Leaf dir name and the filename's pod-code prefix are redundant.
        for leaf in parquet_leaves:
            assert leaf.name.startswith(leaf.parent.name + "-"), \
                f"filename pod prefix must match leaf dir: {leaf}"

        print("✅ Local Parquet I/O test passed!")

def test_parquet_local_with_granule_name():
    """to_local writes <podcode>-<granule>-<dataset>.parquet chunks under the
    cumulative pod-code dir tree when granule_name is provided."""
    data = {'lat': [0, 1, 2, 3], 'lon': [0, 1, 2, 3], 'data': [1, 2, 3, 4]}
    sdf = sp.STAREDataFrame(data)
    sdf['geometry'] = [Point(lon, lat) for lon, lat in zip(sdf['lon'], sdf['lat'])]
    sdf = sdf.set_geometry('geometry')
    sdf['sids'] = sdf.make_sids(level=10)

    granule = "MOD05_L2.A2019336.0000"
    dataset_name = "GMI_S1"

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, "multi_granule")
        sdf.to_local(local_path, level=10, dataset=dataset_name,
                          granule_name=granule)

        from pathlib import Path
        # Top level is the pod-code dir tree (q + 2-digit root), not Q-dirs.
        top_level = [p.name for p in Path(local_path).iterdir() if p.is_dir()]
        assert top_level, f"Nothing written under {local_path}"
        assert all(name.startswith("q") for name in top_level), \
            f"Expected only pod-code (q*) dirs at root, got {top_level}"

        # Each leaf is <podcode>-<granule>-<dataset>.parquet (one file per pod).
        leaf_files = list(Path(local_path).rglob(f"q*-{granule}-{dataset_name}.parquet"))
        assert leaf_files, "Expected pod-code chunk leaves under the dir tree"
        assert all(leaf.is_file() for leaf in leaf_files)
        # Filename's pod-code prefix is redundant with the leaf dir name.
        for leaf in leaf_files:
            assert leaf.name.startswith(leaf.parent.name + "-"), \
                f"filename pod prefix must match leaf dir: {leaf}"

        # Round-trip should still work (from_local is layout-agnostic)
        sdf_read = sp.STAREDataFrame.from_local(local_path)
        assert sdf.shape == sdf_read.shape


def test_parquet_s3_functions():
    """Test S3 Parquet storage functions (requires S3 credentials)"""
    
    # Skip this test if no S3 credentials are available
    # In a real test environment, you would use mock S3 or test credentials
    pytest.skip("S3 test requires credentials - skipping for now")
    
    # Create a test STAREDataFrame with geometry and SIDs
    data = {
        'lat': [0, 1, 2, 3, 4],
        'lon': [0, 1, 2, 3, 4],
        'data': [1, 2, 3, 4, 5],
        'category': ['A', 'B', 'A', 'B', 'A']
    }
    
    sdf = sp.STAREDataFrame(data)
    
    # Add geometry column
    sdf['geometry'] = [Point(lon, lat) for lon, lat in zip(sdf['lon'], sdf['lat'])]
    sdf = sdf.set_geometry('geometry')
    
    # Create SIDs
    sids = sdf.make_sids(level=10)
    sdf['sids'] = sids
    
    # S3 test would go here with proper credentials
    # s3_path = "s3://test-bucket/test-granule"
    # storage_options = {
    #     "key": "your-key",
    #     "secret": "your-secret",
    #     "client_kwargs": {"region_name": "us-west-2"}
    # }
    # 
    # written_path = sdf.to_s3(s3_path, level=10, storage_options=storage_options)
    # sdf_read = sp.STAREDataFrame.from_s3(s3_path, storage_options=storage_options)
    
    print("✅ S3 Parquet I/O test skipped (requires credentials)")

def test_parquet_with_real_granule_data():
    """Test Parquet I/O with actual granule data if available"""
    
    # Check if we have test data available. Pass the granule (.hdf); the
    # _stare.nc sidecar is auto-discovered via sidecar=True.
    test_data_path = "tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522.hdf"

    if os.path.exists(test_data_path):
        print(f"Testing with real granule data: {test_data_path}")

        # Read granule with starepandas
        sdf = sp.read_granule(test_data_path, sidecar=True, latlon=True, read_timestamp=False)
        
        print(f"Granule STAREDataFrame shape: {sdf.shape}")
        print(f"Columns: {sdf.columns.tolist()}")
        print(f"Has SIDs: {sdf.has_sids()}")
        
        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, "real_granule")
            
            print(f"Testing to_local with real data...")
            # Test writing to local Parquet
            written_path = sdf.to_local(local_path, level=10, chunk_size=250000)
            print(f"Written to: {written_path}")
            
            print(f"Testing from_local with real data...")
            # Test reading from local Parquet
            sdf_read = sp.STAREDataFrame.from_local(local_path)
            print(f"Read STAREDataFrame shape: {sdf_read.shape}")
            
            # Verify data integrity
            assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
            assert list(sdf.columns) == list(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
            
            print("✅ Real granule Parquet I/O test passed!")
    else:
        print(f"Test data not available at {test_data_path}, skipping real data test")

if __name__ == "__main__":
    print("Running Parquet I/O tests...")
    test_parquet_local_functions()
    test_parquet_s3_functions()
    test_parquet_with_real_granule_data()
    print("All tests completed!") 