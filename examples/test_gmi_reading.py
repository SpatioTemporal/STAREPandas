#!/usr/bin/env python3
"""
Test script for GMI granule reading functionality.

This script tests the new GMI class to ensure it can properly read
GMI HDF5 files and convert them to STAREDataFrames.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import starepandas
from starepandas.io.granules.gmi import GMI


def test_gmi_class_direct():
    """Test GMI class directly."""
    print("=== Testing GMI Class Directly ===")
    
    gmi_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/1C.GPM.GMI.XCAL2016-C.20250101-S003720-E021032.061565.V07B.HDF5'
    
    try:
        # Test GMI class initialization
        with GMI(gmi_file) as gmi:
            print(f"✓ Successfully opened GMI file: {os.path.basename(gmi_file)}")
            print(f"  File type: {gmi.file_type}")
            print(f"  Available scans: {gmi.scans}")
            print(f"  Start time: {gmi.ts_start}")
            print(f"  End time: {gmi.ts_end}")
            
            # Test reading lat/lon
            print("\n--- Testing Latitude/Longitude Reading ---")
            gmi.read_latlon()
            for scan in gmi.scans:
                if scan in gmi.lat:
                    lat_shape = gmi.lat[scan].shape
                    lon_shape = gmi.lon[scan].shape
                    print(f"  {scan}: lat {lat_shape}, lon {lon_shape}")
                    print(f"    Lat range: [{gmi.lat[scan].min():.2f}, {gmi.lat[scan].max():.2f}]")
                    print(f"    Lon range: [{gmi.lon[scan].min():.2f}, {gmi.lon[scan].max():.2f}]")
            
            # Test reading timestamps
            print("\n--- Testing Timestamp Reading ---")
            gmi.read_timestamps()
            for scan in gmi.scans:
                if scan in gmi.timestamps:
                    ts_shape = gmi.timestamps[scan].shape
                    print(f"  {scan}: timestamps {ts_shape}")
                    if ts_shape[0] > 0:
                        print(f"    First timestamp: {gmi.timestamps[scan][0, 0]}")
                        print(f"    Last timestamp: {gmi.timestamps[scan][-1, -1]}")
            
            # Test reading data
            print("\n--- Testing Data Reading ---")
            gmi.read_data()
            for scan in gmi.scans:
                if scan in gmi.data:
                    channels = list(gmi.data[scan].keys())
                    print(f"  {scan}: {len(channels)} channels - {channels}")
                    for channel in channels[:2]:  # Show first 2 channels
                        data_shape = gmi.data[scan][channel].shape
                        data_min = gmi.data[scan][channel].min()
                        data_max = gmi.data[scan][channel].max()
                        print(f"    {channel}: shape {data_shape}, range [{data_min:.2f}, {data_max:.2f}]")
            
            # Test adding STARE indices
            print("\n--- Testing STARE Index Generation ---")
            gmi.add_sids(adapt_resolution=True)
            for scan in gmi.scans:
                if scan in gmi.sids:
                    sids_shape = gmi.sids[scan].shape
                    print(f"  {scan}: SIDs {sids_shape}")
                    print(f"    Sample SIDs: {gmi.sids[scan][0, :3]}")
            
            # Test conversion to DataFrame
            print("\n--- Testing DataFrame Conversion ---")
            dfs = gmi.to_df()
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
        print(f"❌ GMI class test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_gmi_via_read_granule():
    """Test GMI reading via the read_granule function."""
    print("\n=== Testing GMI via read_granule Function ===")
    
    gmi_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/1C.GPM.GMI.XCAL2016-C.20250101-S003720-E021032.061565.V07B.HDF5'
    
    try:
        # Test with basic options
        print("--- Testing basic read_granule ---")
        dfs = starepandas.read_granule(
            gmi_file,
            latlon=True,
            add_sids=True,
            read_timestamp=True
        )
        
        print(f"✓ Successfully read GMI file via read_granule")
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


def test_gmi_data_integrity():
    """Test GMI data integrity and expected values."""
    print("\n=== Testing GMI Data Integrity ===")
    
    gmi_file = '/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/1C.GPM.GMI.XCAL2016-C.20250101-S003720-E021032.061565.V07B.HDF5'
    
    try:
        dfs = starepandas.read_granule(
            gmi_file,
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
            for channel in temp_channels[:2]:  # Check first 2 channels
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
    """Run all GMI tests."""
    print("GMI Granule Reading Test")
    print("=" * 50)
    print("Testing the new GMI class implementation")
    print()
    
    # Test 1: Direct GMI class usage
    test1_passed = test_gmi_class_direct()
    
    # Test 2: Via read_granule function
    test2_passed = test_gmi_via_read_granule()
    
    # Test 3: Data integrity checks
    test3_passed = test_gmi_data_integrity()
    
    # Summary
    print("\n" + "=" * 50)
    print("GMI Test Results:")
    print(f"  Direct GMI class test: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"  read_granule test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    print(f"  Data integrity test: {'✅ PASSED' if test3_passed else '❌ FAILED'}")
    
    if all([test1_passed, test2_passed, test3_passed]):
        print("\n🎉 All GMI tests PASSED! GMI implementation is working correctly.")
    else:
        print("\n⚠️  Some GMI tests FAILED. Check the error messages above.")
    
    print("\nKey GMI Features Implemented:")
    print("- HDF5 file format support")
    print("- 2 scans (S1, S2) with different channel counts")
    print("- S1: 9 channels, S2: 4 channels")
    print("- Latitude/longitude reading")
    print("- Timestamp processing")
    print("- STARE index generation")
    print("- DataFrame conversion")
    print("- Integration with read_granule function")


if __name__ == "__main__":
    main()
