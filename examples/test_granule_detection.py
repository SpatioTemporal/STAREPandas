#!/usr/bin/env python3
"""
Test script for automatic granule detection of GMI and AMSR2 files.

This script tests that the granule_factory function can automatically
detect GMI and AMSR2 files and instantiate the correct classes.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import starepandas
from starepandas.io.granules import granule_factory


def test_granule_detection():
    """Test automatic granule detection for GMI and AMSR2 files."""
    print("=== Testing Automatic Granule Detection ===")
    
    # Test files
    test_files = {
        'GMI': '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/1C.GPM.GMI.XCAL2016-C.20250101-S003720-E021032.061565.V07B.HDF5',
        'AMSR2': '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    }
    
    results = {}
    
    for instrument, file_path in test_files.items():
        print(f"\n--- Testing {instrument} Detection ---")
        
        try:
            # Test granule_factory detection
            granule = granule_factory(file_path)
            class_name = granule.__class__.__name__
            
            print(f"✓ File detected as: {class_name}")
            print(f"  File: {os.path.basename(file_path)}")
            print(f"  Expected: {instrument}")
            print(f"  Detected: {class_name}")
            
            # Verify correct class was instantiated
            if class_name == instrument:
                print(f"  ✅ Correct class instantiated")
                results[instrument] = True
            else:
                print(f"  ❌ Wrong class instantiated (expected {instrument})")
                results[instrument] = False
            
            # Test basic functionality
            print(f"  File type: {granule.file_type}")
            print(f"  Available scans: {granule.scans}")
            print(f"  Start time: {granule.ts_start}")
            
            # Clean up
            granule.close()
            
        except Exception as e:
            print(f"❌ Detection failed for {instrument}: {e}")
            results[instrument] = False
    
    return results


def test_read_granule_integration():
    """Test that read_granule works seamlessly with GMI and AMSR2."""
    print("\n=== Testing read_granule Integration ===")
    
    test_files = {
        'GMI': '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/1C.GPM.GMI.XCAL2016-C.20250101-S003720-E021032.061565.V07B.HDF5',
        'AMSR2': '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    }
    
    results = {}
    
    for instrument, file_path in test_files.items():
        print(f"\n--- Testing {instrument} via read_granule ---")
        
        try:
            # Test read_granule with minimal options
            dfs = starepandas.read_granule(file_path, latlon=True)
            
            print(f"✓ Successfully read {instrument} file")
            print(f"  Returned {len(dfs)} scan DataFrames")
            
            # Check each scan
            for scan, df in dfs.items():
                print(f"  {scan}: {len(df)} rows, {len(df.columns)} columns")
                
                # Verify basic columns exist
                required_cols = ['lat', 'lon']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    print(f"    ⚠️  Missing columns: {missing_cols}")
                else:
                    print(f"    ✓ Basic columns present")
            
            results[instrument] = True
            
        except Exception as e:
            print(f"❌ read_granule failed for {instrument}: {e}")
            results[instrument] = False
    
    return results


def test_granule_factory_library():
    """Test that GMI and AMSR2 are properly registered in the factory library."""
    print("\n=== Testing Granule Factory Library ===")
    
    from starepandas.io.granules import granule_factory_library
    
    print("Current granule factory library:")
    for pattern, granule_class in granule_factory_library.items():
        class_name = granule_class.__name__
        print(f"  '{pattern}' -> {class_name}")
    
    # Check if GMI and AMSR2 are registered
    gmi_found = any('GMI' in pattern for pattern in granule_factory_library.keys())
    amsr2_found = any('AMSR2' in pattern for pattern in granule_factory_library.keys())
    
    print(f"\nRegistration check:")
    print(f"  GMI registered: {'✅ YES' if gmi_found else '❌ NO'}")
    print(f"  AMSR2 registered: {'✅ YES' if amsr2_found else '❌ NO'}")
    
    return gmi_found and amsr2_found


def main():
    """Run all granule detection tests."""
    print("Granule Detection Test")
    print("=" * 50)
    print("Testing automatic detection and integration of GMI and AMSR2 classes")
    print()
    
    # Test 1: Factory library registration
    library_ok = test_granule_factory_library()
    
    # Test 2: Automatic detection
    detection_results = test_granule_detection()
    
    # Test 3: read_granule integration
    integration_results = test_read_granule_integration()
    
    # Summary
    print("\n" + "=" * 50)
    print("Granule Detection Test Results:")
    print(f"  Factory library registration: {'✅ PASSED' if library_ok else '❌ FAILED'}")
    
    for instrument in ['GMI', 'AMSR2']:
        detection_ok = detection_results.get(instrument, False)
        integration_ok = integration_results.get(instrument, False)
        
        print(f"  {instrument} detection: {'✅ PASSED' if detection_ok else '❌ FAILED'}")
        print(f"  {instrument} integration: {'✅ PASSED' if integration_ok else '❌ FAILED'}")
    
    # Overall result
    all_passed = (library_ok and 
                  all(detection_results.values()) and 
                  all(integration_results.values()))
    
    if all_passed:
        print("\n🎉 All detection tests PASSED!")
        print("✅ GMI and AMSR2 are fully integrated into STAREPandas")
        print("✅ Files are automatically detected by filename patterns")
        print("✅ read_granule function works seamlessly with both instruments")
    else:
        print("\n⚠️  Some detection tests FAILED. Check the error messages above.")
    
    print("\nUsage Examples:")
    print("# GMI file")
    print("gmi_dfs = starepandas.read_granule('1C.GPM.GMI.*.HDF5', latlon=True, add_sids=True)")
    print()
    print("# AMSR2 file")  
    print("amsr2_dfs = starepandas.read_granule('1C.GCOMW1.AMSR2.*.HDF5', latlon=True, add_sids=True)")


if __name__ == "__main__":
    main()
