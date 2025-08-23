import os
import tempfile
import shutil
import numpy as np
import pandas as pd
import starepandas as sp
from shapely.geometry import Point
import pytest

def test_pickle_local_functions():
    """Test pickle local storage functions"""
    
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
        local_path = os.path.join(temp_dir, "test_granule.pkl")
        
        print(f"Testing to_pickle_local...")
        # Test writing to local pickle
        written_path = sdf.to_pickle_local(local_path)
        print(f"Written to: {written_path}")
        
        print(f"Testing from_pickle_local...")
        # Test reading from local pickle
        sdf_read = sp.STAREDataFrame.from_pickle_local(local_path)
        print(f"Read STAREDataFrame shape: {sdf_read.shape}")
        print(f"Read columns: {sdf_read.columns.tolist()}")
        
        # Verify data integrity
        assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
        assert set(sdf.columns) == set(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
        
        # Check that data values match for each column by name
        for col in sdf.columns:
            assert sdf[col].equals(sdf_read[col]), f"Column {col} data mismatch"
        
        print("✅ Local pickle functions test passed!")
        
        # Test with compression
        compressed_path = os.path.join(temp_dir, "test_granule_compressed.pkl")
        print(f"Testing to_pickle_local with compression...")
        written_path = sdf.to_pickle_local(compressed_path, compress='bz2')
        print(f"Written compressed to: {written_path}")
        
        print(f"Testing from_pickle_local with compression...")
        sdf_read_compressed = sp.STAREDataFrame.from_pickle_local(compressed_path, compress='bz2')
        print(f"Read compressed STAREDataFrame shape: {sdf_read_compressed.shape}")
        
        # Verify data integrity for compressed version
        assert sdf.shape == sdf_read_compressed.shape, f"Shape mismatch: {sdf.shape} != {sdf_read_compressed.shape}"
        assert set(sdf.columns) == set(sdf_read_compressed.columns), f"Column mismatch: {sdf.columns} != {sdf_read_compressed.columns}"
        
        for col in sdf.columns:
            assert sdf[col].equals(sdf_read_compressed[col]), f"Column {col} data mismatch in compressed version"
        
        print("✅ Local pickle functions with compression test passed!")

def test_pickle_s3_functions():
    """Test pickle S3 storage functions (requires S3 credentials)"""
    
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
    # s3_path = "s3://test-bucket/test-granule.pkl"
    # storage_options = {
    #     "key": "your-key",
    #     "secret": "your-secret",
    #     "client_kwargs": {"region_name": "us-west-2"}
    # }
    # 
    # written_path = sdf.to_pickle_s3(s3_path, storage_options=storage_options)
    # sdf_read = sp.STAREDataFrame.from_pickle_s3(s3_path, storage_options=storage_options)
    
    print("✅ S3 pickle functions test skipped (requires credentials)")

def test_pickle_with_real_granule_data():
    """Test pickle functions with actual granule data if available"""
    
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
            local_path = os.path.join(temp_dir, "real_granule.pkl")
            
            print(f"Testing to_pickle_local with real data...")
            # Test writing to local pickle
            written_path = sdf.to_pickle_local(local_path)
            print(f"Written to: {written_path}")
            
            print(f"Testing from_pickle_local with real data...")
            # Test reading from local pickle
            sdf_read = sp.STAREDataFrame.from_pickle_local(local_path)
            print(f"Read STAREDataFrame shape: {sdf_read.shape}")
            
            # Verify data integrity
            assert sdf.shape == sdf_read.shape, f"Shape mismatch: {sdf.shape} != {sdf_read.shape}"
            assert set(sdf.columns) == set(sdf_read.columns), f"Column mismatch: {sdf.columns} != {sdf_read.columns}"
            
            print("✅ Real granule pickle test passed!")
    else:
        print(f"Test data not available at {test_data_path}, skipping real data test")

if __name__ == "__main__":
    print("Running pickle functions tests...")
    test_pickle_local_functions()
    test_pickle_s3_functions()
    test_pickle_with_real_granule_data()
    print("All tests completed!") 