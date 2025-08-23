#!/usr/bin/env python3
"""
Test script for SSMIS HDF5 support.
This script tests both NetCDF4 and HDF5 file formats using the available test data.
"""

import os
import sys
import numpy as np

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from starepandas.io.granules.ssmis import SSMIS


def test_ssmis_netcdf4():
    """Test SSMIS NetCDF4 file reading."""
    print("=== Testing SSMIS NetCDF4 Support ===")
    
    file_path = "tests/data/granules/1C.F16.SSMIS.XCAL2016-V.20210110-S002714-E020909.088907.V05A.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False
    
    try:
        with SSMIS(file_path, scans=['S1', 'S2']) as ssmis:
            print(f"✓ File type detected: {ssmis.file_type}")
            print(f"✓ Available scans: {ssmis.scans}")
            
            # Test reading latitude and longitude
            ssmis.read_latlon()
            print(f"✓ Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"✓ Longitude shape for S1: {ssmis.lon['S1'].shape}")
            
            # Test reading timestamps
            ssmis.read_timestamps()
            print(f"✓ Timestamp shape for S1: {ssmis.timestamps['S1'].shape}")
            
            # Test reading data
            ssmis.read_data()
            print(f"✓ Available data for S1: {list(ssmis.data['S1'].keys())}")
            if 'Tc1' in ssmis.data['S1']:
                print(f"✓ Tc1 data shape: {ssmis.data['S1']['Tc1'].shape}")
            
            # Test converting to DataFrame
            dfs = ssmis.to_df()
            print(f"✓ DataFrame for S1 has {len(dfs['S1'])} rows")
            
            return True
            
    except Exception as e:
        print(f"✗ Error reading NetCDF4 file: {e}")
        return False


def test_ssmis_hdf5():
    """Test SSMIS HDF5 file reading."""
    print("\n=== Testing SSMIS HDF5 Support ===")
    
    file_path = "tests/data/granules/1C.F18.SSMIS.XCAL2021-V.20250105-S222535-E000725.078504.V07B.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False
    
    try:
        with SSMIS(file_path, scans=['S1', 'S2', 'S3', 'S4']) as ssmis:
            print(f"✓ File type detected: {ssmis.file_type}")
            print(f"✓ Available scans: {ssmis.scans}")
            
            # Test reading latitude and longitude
            ssmis.read_latlon()
            print(f"✓ Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"✓ Longitude shape for S1: {ssmis.lon['S1'].shape}")
            
            # Test reading timestamps
            ssmis.read_timestamps()
            print(f"✓ Timestamp shape for S1: {ssmis.timestamps['S1'].shape}")
            
            # Test reading data
            ssmis.read_data()
            print(f"✓ Available data for S1: {list(ssmis.data['S1'].keys())}")
            if 'Tc1' in ssmis.data['S1']:
                print(f"✓ Tc1 data shape: {ssmis.data['S1']['Tc1'].shape}")
            
            # Test converting to DataFrame
            dfs = ssmis.to_df()
            print(f"✓ DataFrame for S1 has {len(dfs['S1'])} rows")
            
            return True
            
    except Exception as e:
        print(f"✗ Error reading HDF5 file: {e}")
        return False


def test_auto_detection():
    """Test automatic file type detection."""
    print("\n=== Testing Automatic File Type Detection ===")
    
    netcdf_file = "tests/data/granules/1C.F16.SSMIS.XCAL2016-V.20210110-S002714-E020909.088907.V05A.HDF5"
    hdf5_file = "tests/data/granules/1C.F18.SSMIS.XCAL2021-V.20250105-S222535-E000725.078504.V07B.HDF5"
    
    files_to_test = [
        (netcdf_file, "NetCDF4"),
        (hdf5_file, "HDF5")
    ]
    
    for file_path, expected_type in files_to_test:
        if os.path.exists(file_path):
            try:
                with SSMIS(file_path) as ssmis:
                    detected_type = ssmis.file_type
                    status = "✓" if detected_type == expected_type.lower() else "✗"
                    print(f"{status} File: {os.path.basename(file_path)}")
                    print(f"   Expected: {expected_type}, Detected: {detected_type}")
            except Exception as e:
                print(f"✗ Error with {os.path.basename(file_path)}: {e}")
        else:
            print(f"✗ File not found: {file_path}")


def main():
    """Run all tests."""
    print("SSMIS HDF5/NetCDF4 Support Test")
    print("=" * 50)
    
    # Run tests
    netcdf_success = test_ssmis_netcdf4()
    hdf5_success = test_ssmis_hdf5()
    test_auto_detection()
    
    print("\n" + "=" * 50)
    print("Test Summary:")
    print(f"NetCDF4 Support: {'✓ PASS' if netcdf_success else '✗ FAIL'}")
    print(f"HDF5 Support: {'✓ PASS' if hdf5_success else '✗ FAIL'}")
    
    if netcdf_success and hdf5_success:
        print("\n🎉 All tests passed! SSMIS HDF5 support is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the error messages above.")


if __name__ == "__main__":
    main() 