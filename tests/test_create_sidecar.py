#!/usr/bin/env python3
"""
Test script for create_sidecar functionality.
This script tests the create_sidecar method for SSMIS granules.
"""

import os
import sys

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from starepandas.io.granules.ssmis import SSMIS


def test_create_sidecar():
    """Test creating a sidecar file for SSMIS."""
    print("=== Testing SSMIS create_sidecar Functionality ===")
    
    file_path = "tests/data/granules/1C.F18.SSMIS.XCAL2021-V.20250105-S222535-E000725.078504.V07B.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False
    
    try:
        with SSMIS(file_path, scans=['S1', 'S2']) as ssmis:
            print(f"✓ File type detected: {ssmis.file_type}")
            print(f"✓ Available scans: {ssmis.scans}")
            
            # Read latitude and longitude
            ssmis.read_latlon()
            print(f"✓ Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"✓ Longitude shape for S1: {ssmis.lon['S1'].shape}")
            
            # Create sidecar file
            print("Creating sidecar file...")
            sidecar = ssmis.create_sidecar(n_workers=1, out_path="test_output/")
            
            print(f"✓ Sidecar file created: {sidecar.file_path}")
            
            # Check if the sidecar file exists
            if os.path.exists(sidecar.file_path):
                print(f"✓ Sidecar file exists on disk")
                
                # Check file size
                file_size = os.path.getsize(sidecar.file_path)
                print(f"✓ Sidecar file size: {file_size} bytes")
                
                # Clean up
                os.remove(sidecar.file_path)
                print("✓ Test file cleaned up")
                
                return True
            else:
                print(f"✗ Sidecar file was not created")
                return False
            
    except Exception as e:
        print(f"✗ Error creating sidecar: {e}")
        return False


def test_create_sidecar_without_latlon():
    """Test that create_sidecar fails when lat/lon not loaded."""
    print("\n=== Testing create_sidecar Error Handling ===")
    
    file_path = "tests/data/granules/1C.F18.SSMIS.XCAL2021-V.20250105-S222535-E000725.078504.V07B.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False
    
    try:
        with SSMIS(file_path, scans=['S1']) as ssmis:
            # Try to create sidecar without reading lat/lon
            print("Attempting to create sidecar without reading lat/lon...")
            sidecar = ssmis.create_sidecar()
            print("✗ Should have raised an error")
            return False
            
    except ValueError as e:
        if "Latitude and longitude data must be loaded" in str(e):
            print("✓ Correctly raised ValueError for missing lat/lon data")
            return True
        else:
            print(f"✗ Unexpected error: {e}")
            return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False


def main():
    """Run all tests."""
    print("SSMIS create_sidecar Functionality Test")
    print("=" * 50)
    
    # Create test output directory
    os.makedirs("test_output", exist_ok=True)
    
    # Run tests
    sidecar_success = test_create_sidecar()
    error_handling_success = test_create_sidecar_without_latlon()
    
    print("\n" + "=" * 50)
    print("Test Summary:")
    print(f"create_sidecar Functionality: {'✓ PASS' if sidecar_success else '✗ FAIL'}")
    print(f"Error Handling: {'✓ PASS' if error_handling_success else '✗ FAIL'}")
    
    if sidecar_success and error_handling_success:
        print("\n🎉 All tests passed! create_sidecar functionality is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the error messages above.")
    
    # Clean up test directory
    try:
        os.rmdir("test_output")
    except:
        pass


if __name__ == "__main__":
    main() 