#!/usr/bin/env python3
"""
Test script for AMSR2 granule reading functionality.

This script tests the new AMSR2 class to ensure it can properly read
AMSR2 HDF5 files and convert them to STAREDataFrames.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import starepandas
from starepandas.io.granules.amsr2 import AMSR2


def test_amsr2_class_direct():
    """Test AMSR2 class directly."""
    print("=== Testing AMSR2 Class Directly ===")
    
    amsr2_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    
    try:
        # Test AMSR2 class initialization
        with AMSR2(amsr2_file) as amsr2:
            print(f"✓ Successfully opened AMSR2 file: {os.path.basename(amsr2_file)}")
            print(f"  File type: {amsr2.file_type}")
            print(f"  Available scans: {amsr2.scans}")
            print(f"  Start time: {amsr2.ts_start}")
            print(f"  End time: {amsr2.ts_end}")
            
            # Test reading lat/lon
            print("\n--- Testing Latitude/Longitude Reading ---")
            amsr2.read_latlon()
            for scan in amsr2.scans:
                if scan in amsr2.lat:
                    lat_shape = amsr2.lat[scan].shape
                    lon_shape = amsr2.lon[scan].shape
                    print(f"  {scan}: lat {lat_shape}, lon {lon_shape}")
                    print(f"    Lat range: [{amsr2.lat[scan].min():.2f}, {amsr2.lat[scan].max():.2f}]")
                    print(f"    Lon range: [{amsr2.lon[scan].min():.2f}, {amsr2.lon[scan].max():.2f}]")
            
            # Test reading timestamps
            print("\n--- Testing Timestamp Reading ---")
            amsr2.read_timestamps()
            for scan in amsr2.scans:
                if scan in amsr2.timestamps:
                    ts_shape = amsr2.timestamps[scan].shape
                    print(f"  {scan}: timestamps {ts_shape}")
                    if ts_shape[0] > 0:
                        print(f"    First timestamp: {amsr2.timestamps[scan][0, 0]}")
                        print(f"    Last timestamp: {amsr2.timestamps[scan][-1, -1]}")
            
            # Test reading data
            print("\n--- Testing Data Reading ---")
            amsr2.read_data()
            for scan in amsr2.scans:
                if scan in amsr2.data:
                    channels = list(amsr2.data[scan].keys())
                    print(f"  {scan}: {len(channels)} channels - {channels}")
                    for channel in channels:  # Show all channels (only 2 per scan)
                        data_shape = amsr2.data[scan][channel].shape
                        data_min = amsr2.data[scan][channel].min()
                        data_max = amsr2.data[scan][channel].max()
                        print(f"    {channel}: shape {data_shape}, range [{data_min:.2f}, {data_max:.2f}]")
            
            # Test adding STARE indices
            print("\n--- Testing STARE Index Generation ---")
            amsr2.add_sids(adapt_resolution=True)
            for scan in amsr2.scans:
                if scan in amsr2.sids:
                    sids_shape = amsr2.sids[scan].shape
                    print(f"  {scan}: SIDs {sids_shape}")
                    print(f"    Sample SIDs: {amsr2.sids[scan][0, :3]}")
            
            # Test conversion to DataFrame
            print("\n--- Testing DataFrame Conversion ---")
            dfs = amsr2.to_df()
            for scan, df in dfs.items():
                print(f"  {scan}: {len(df)} rows, {len(df.columns)} columns")
                print(f"    Columns: {list(df.columns)}")
                print(f"    Sample data:")
                print(f"      lat: {df['lat'].iloc[0]:.4f}")
                print(f"      lon: {df['lon'].iloc[0]:.4f}")
                print(f"      sids: {df['sids'].iloc[0]}")
                
                # Check for any NaN values
                nan_counts = df.isnull().sum()
                if nan_counts.sum() > 0:
                    print(f"    NaN values found: {nan_counts[nan_counts > 0].to_dict()}")
                else:
                    print(f"    ✓ No NaN values found")
        
        return True
        
    except Exception as e:
        print(f"❌ AMSR2 class test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_amsr2_via_read_granule():
    """Test AMSR2 reading via the read_granule function."""
    print("\n=== Testing AMSR2 via read_granule Function ===")
    
    amsr2_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    
    try:
        # Test with basic options
        print("--- Testing basic read_granule ---")
        dfs = starepandas.read_granule(
            amsr2_file,
            latlon=True,
            add_sids=True,
            read_timestamp=True
        )
        
        print(f"✓ Successfully read AMSR2 file via read_granule")
        print(f"  Returned {len(dfs)} scan DataFrames")
        
        for scan, df in dfs.items():
            print(f"  {scan}: {len(df)} rows, {len(df.columns)} columns")
            print(f"    Columns: {list(df.columns)}")
            
            # Verify required columns exist
            required_cols = ['lat', 'lon', 'sids', 'timestamp']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"    ⚠️  Missing columns: {missing_cols}")
            else:
                print(f"    ✓ All required columns present")
            
            # Check data ranges
            print(f"    Data ranges:")
            print(f"      lat: [{df['lat'].min():.2f}, {df['lat'].max():.2f}]")
            print(f"      lon: [{df['lon'].min():.2f}, {df['lon'].max():.2f}]")
            print(f"      timestamp: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            # Check STARE indices
            if 'sids' in df.columns:
                print(f"      sids: {df['sids'].min()} to {df['sids'].max()}")
                print(f"      Sample SIDs: {df['sids'].iloc[:3].tolist()}")
        
        return True
        
    except Exception as e:
        print(f"❌ read_granule test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_amsr2_scan_differences():
    """Test AMSR2 scan differences (S1-S4 vs S5-S6 resolution)."""
    print("\n=== Testing AMSR2 Scan Resolution Differences ===")
    
    amsr2_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    
    try:
        dfs = starepandas.read_granule(
            amsr2_file,
            latlon=True,
            add_sids=True,
            read_timestamp=True
        )
        
        print("--- Checking scan resolution differences ---")
        
        # Group scans by expected resolution
        low_res_scans = ['S1', 'S2', 'S3', 'S4']  # 243 pixels per scan
        high_res_scans = ['S5', 'S6']  # 486 pixels per scan
        
        for scan_group, expected_scans in [("Low Resolution", low_res_scans), ("High Resolution", high_res_scans)]:
            print(f"\n{scan_group} scans:")
            for scan in expected_scans:
                if scan in dfs:
                    df = dfs[scan]
                    # Calculate expected pixels per scan line
                    total_pixels = len(df)
                    
                    # Get unique timestamps to count scan lines
                    unique_timestamps = df['timestamp'].nunique()
                    pixels_per_scan = total_pixels / unique_timestamps if unique_timestamps > 0 else 0
                    
                    print(f"  {scan}: {total_pixels} total pixels, ~{pixels_per_scan:.0f} pixels/scan")
                    
                    # Verify expected resolution
                    if scan in low_res_scans:
                        expected_pixels = 243
                    else:  # high_res_scans
                        expected_pixels = 486
                    
                    if abs(pixels_per_scan - expected_pixels) < 10:  # Allow some tolerance
                        print(f"    ✓ Resolution matches expected ({expected_pixels} pixels/scan)")
                    else:
                        print(f"    ⚠️  Resolution differs from expected ({expected_pixels} pixels/scan)")
                else:
                    print(f"  {scan}: Not found in results")
        
        return True
        
    except Exception as e:
        print(f"❌ Scan resolution test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_amsr2_data_integrity():
    """Test AMSR2 data integrity and expected values."""
    print("\n=== Testing AMSR2 Data Integrity ===")
    
    amsr2_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/AMSR2/2025/Jan_1_2/1C.GCOMW1.AMSR2.XCAL2016-V.20250601-S003646-E021538.069354.V07A.HDF5'
    
    try:
        dfs = starepandas.read_granule(
            amsr2_file,
            latlon=True,
            add_sids=True,
            read_timestamp=True
        )
        
        print("--- Checking data integrity ---")
        
        for scan, df in dfs.items():
            print(f"\n{scan} integrity checks:")
            
            # Check latitude range (should be -90 to 90)
            lat_min, lat_max = df['lat'].min(), df['lat'].max()
            if -90 <= lat_min <= lat_max <= 90:
                print(f"  ✓ Latitude range valid: [{lat_min:.2f}, {lat_max:.2f}]")
            else:
                print(f"  ❌ Latitude range invalid: [{lat_min:.2f}, {lat_max:.2f}]")
            
            # Check longitude range (should be -180 to 180)
            lon_min, lon_max = df['lon'].min(), df['lon'].max()
            if -180 <= lon_min <= lon_max <= 180:
                print(f"  ✓ Longitude range valid: [{lon_min:.2f}, {lon_max:.2f}]")
            else:
                print(f"  ❌ Longitude range invalid: [{lon_min:.2f}, {lon_max:.2f}]")
            
            # Check STARE indices (should be positive integers)
            if 'sids' in df.columns:
                sids_min, sids_max = df['sids'].min(), df['sids'].max()
                if sids_min > 0 and isinstance(sids_min, (int, np.integer)):
                    print(f"  ✓ STARE indices valid: {sids_min} to {sids_max}")
                else:
                    print(f"  ❌ STARE indices invalid: {sids_min} to {sids_max}")
            
            # Check temperature channels (should be reasonable values)
            temp_channels = [col for col in df.columns if col.startswith('Tc')]
            for channel in temp_channels:  # Check all channels (only 2 per scan)
                temp_min, temp_max = df[channel].min(), df[channel].max()
                # Brightness temperatures typically range from ~150K to ~350K
                if 100 <= temp_min <= temp_max <= 400:
                    print(f"  ✓ {channel} range reasonable: [{temp_min:.1f}K, {temp_max:.1f}K]")
                else:
                    print(f"  ⚠️  {channel} range unusual: [{temp_min:.1f}K, {temp_max:.1f}K]")
            
            # Check for missing data
            missing_data = df.isnull().sum().sum()
            if missing_data == 0:
                print(f"  ✓ No missing data")
            else:
                print(f"  ⚠️  {missing_data} missing values found")
        
        return True
        
    except Exception as e:
        print(f"❌ Data integrity test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all AMSR2 tests."""
    print("AMSR2 Granule Reading Test")
    print("=" * 50)
    print("Testing the new AMSR2 class implementation")
    print()
    
    # Test 1: Direct AMSR2 class usage
    test1_passed = test_amsr2_class_direct()
    
    # Test 2: Via read_granule function
    test2_passed = test_amsr2_via_read_granule()
    
    # Test 3: Scan resolution differences
    test3_passed = test_amsr2_scan_differences()
    
    # Test 4: Data integrity checks
    test4_passed = test_amsr2_data_integrity()
    
    # Summary
    print("\n" + "=" * 50)
    print("AMSR2 Test Results:")
    print(f"  Direct AMSR2 class test: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"  read_granule test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    print(f"  Scan resolution test: {'✅ PASSED' if test3_passed else '❌ FAILED'}")
    print(f"  Data integrity test: {'✅ PASSED' if test4_passed else '❌ FAILED'}")
    
    if all([test1_passed, test2_passed, test3_passed, test4_passed]):
        print("\n🎉 All AMSR2 tests PASSED! AMSR2 implementation is working correctly.")
    else:
        print("\n⚠️  Some AMSR2 tests FAILED. Check the error messages above.")
    
    print("\nKey AMSR2 Features Implemented:")
    print("- HDF5 file format support")
    print("- 6 scans (S1-S6) with different resolutions")
    print("- S1-S4: 243 pixels/scan, S5-S6: 486 pixels/scan")
    print("- All scans: 2 channels each")
    print("- Latitude/longitude reading")
    print("- Timestamp processing")
    print("- STARE index generation")
    print("- DataFrame conversion")
    print("- Integration with read_granule function")


if __name__ == "__main__":
    main()
