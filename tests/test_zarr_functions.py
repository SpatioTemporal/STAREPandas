import os
import tempfile
import shutil
import numpy as np
import pandas as pd
import starepandas as sp
from shapely.geometry import Point
import pytest

def test_zarr_local_functions():
    """Test zarr local storage functions following the pattern from zarrpods_v2.ipynb"""
    
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
        local_path = os.path.join(temp_dir, "test_granule.zarr")
        
        print(f"Testing to_zarr_local...")
        # Test writing to local zarr
        written_path = sdf.to_zarr_local(local_path, level=10, chunk_size=1000)
        print(f"Written to: {written_path}")
        
        print(f"Testing from_zarr_local...")
        # Test reading from local zarr
        sdf_read = sp.STAREDataFrame.from_zarr_local(local_path)
        print(f"Read STAREDataFrame shape: {sdf_read.shape}")
        print(f"Read columns: {sdf_read.columns.tolist()}")
        
        # Verify data integrity (ignore column order)
        assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
        assert set(sdf.columns) == set(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
        
        # Check that data values match for each column by name (excluding geometry which might be serialized differently)
        for col in sdf.columns:
            if col != 'geometry':
                assert sdf[col].equals(sdf_read[col]), f"Column {col} data mismatch"

        # Verify the HTM-subtree layout was actually created on disk
        # (guards against silent regressions back to the flat layout).
        from pathlib import Path
        q00_dirs = list(Path(local_path).rglob("Q00_*"))
        assert q00_dirs, f"Expected hierarchical Q00_* directories under {local_path}"
        # Default fallback dataset_name="data" since this test passes no dataset.
        # Each leaf is now a "data.parquet" file written by to_zarr_local.
        parquet_leaves = list(Path(local_path).rglob("data.parquet"))
        assert parquet_leaves, "Expected at least one Parquet partition at the dataset leaf"

        print("✅ Local zarr functions test passed!")

def test_zarr_local_with_granule_name():
    """to_zarr_local inserts <granule_name>/<dataset>/ as the leaf when granule_name is provided."""
    data = {'lat': [0, 1, 2, 3], 'lon': [0, 1, 2, 3], 'data': [1, 2, 3, 4]}
    sdf = sp.STAREDataFrame(data)
    sdf['geometry'] = [Point(lon, lat) for lon, lat in zip(sdf['lon'], sdf['lat'])]
    sdf = sdf.set_geometry('geometry')
    sdf['sids'] = sdf.make_sids(level=10)

    granule = "MOD05_L2.A2019336.0000"
    dataset_name = "GMI_S1"

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, "multi_granule.zarr")
        sdf.to_zarr_local(local_path, level=10, dataset=dataset_name,
                          granule_name=granule)

        from pathlib import Path
        # No granule directory directly under root — Q-tree comes first
        top_level = [p.name for p in Path(local_path).iterdir() if p.is_dir()]
        assert top_level, f"Nothing written under {local_path}"
        assert all(name.startswith("Q") for name in top_level), \
            f"Expected only Q* dirs at root, got {top_level}"

        # Each leaf is <granule>/<dataset>.parquet (a single file per partition)
        leaf_files = list(Path(local_path).rglob(f"{granule}/{dataset_name}.parquet"))
        assert leaf_files, f"Expected <granule>/<dataset>.parquet leaves under the HTM tree"
        assert all(leaf.is_file() for leaf in leaf_files)

        # Round-trip should still work (from_zarr_local is layout-agnostic)
        sdf_read = sp.STAREDataFrame.from_zarr_local(local_path)
        assert sdf.shape == sdf_read.shape


def test_zarr_s3_functions():
    """Test zarr S3 storage functions (requires S3 credentials)"""
    
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
    # written_path = sdf.to_zarr_s3(s3_path, level=10, storage_options=storage_options)
    # sdf_read = sp.STAREDataFrame.from_zarr_s3(s3_path, storage_options=storage_options)
    
    print("✅ S3 zarr functions test skipped (requires credentials)")

def test_zarr_with_real_granule_data():
    """Test zarr functions with actual granule data if available"""
    
    # Check if we have test data available
    test_data_path = "tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522_stare.nc"
    
    if os.path.exists(test_data_path):
        print(f"Testing with real granule data: {test_data_path}")
        
        # Read granule with starepandas
        sdf = sp.read_granule(test_data_path, sidecar=True, latlon=True, read_timestamp=False)
        
        print(f"Granule STAREDataFrame shape: {sdf.shape}")
        print(f"Columns: {sdf.columns.tolist()}")
        print(f"Has SIDs: {sdf.has_sids()}")
        
        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, "real_granule.zarr")
            
            print(f"Testing to_zarr_local with real data...")
            # Test writing to local zarr
            written_path = sdf.to_zarr_local(local_path, level=10, chunk_size=250000)
            print(f"Written to: {written_path}")
            
            print(f"Testing from_zarr_local with real data...")
            # Test reading from local zarr
            sdf_read = sp.STAREDataFrame.from_zarr_local(local_path)
            print(f"Read STAREDataFrame shape: {sdf_read.shape}")
            
            # Verify data integrity
            assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
            assert list(sdf.columns) == list(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
            
            print("✅ Real granule zarr test passed!")
    else:
        print(f"Test data not available at {test_data_path}, skipping real data test")

if __name__ == "__main__":
    print("Running zarr functions tests...")
    test_zarr_local_functions()
    test_zarr_s3_functions()
    test_zarr_with_real_granule_data()
    print("All tests completed!") 