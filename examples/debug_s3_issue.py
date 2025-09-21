#!/usr/bin/env python3
"""
Debug script to investigate the S3 FileNotFoundError issue.

This script helps diagnose why the directory creation fix isn't working
and provides detailed debugging information.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame
from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS, _load_config_from_default_locations
import s3fs


def debug_s3_configuration():
    """Debug S3 configuration and credentials."""
    print("=== S3 Configuration Debug ===")
    
    # Check current AWS configuration
    print("1. Current AWS configuration in STAREPandas:")
    print(f"   _AWS_S3_STORAGE_OPTIONS: {_AWS_S3_STORAGE_OPTIONS}")
    
    # Try to load default configuration
    print("\n2. Attempting to load default configuration:")
    try:
        _load_config_from_default_locations()
        print(f"   After loading: {_AWS_S3_STORAGE_OPTIONS}")
    except Exception as e:
        print(f"   Error loading config: {e}")
    
    # Test S3 connection
    print("\n3. Testing S3 connection:")
    if _AWS_S3_STORAGE_OPTIONS:
        try:
            fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
            print("   ✓ S3FileSystem created successfully")
            
            # Try to list buckets
            try:
                buckets = fs.ls('/')
                print(f"   ✓ Can list buckets: {len(buckets)} buckets found")
                for bucket in buckets[:5]:  # Show first 5 buckets
                    print(f"     - {bucket}")
                if len(buckets) > 5:
                    print(f"     ... and {len(buckets) - 5} more")
            except Exception as e:
                print(f"   ✗ Cannot list buckets: {e}")
                
        except Exception as e:
            print(f"   ✗ Cannot create S3FileSystem: {e}")
    else:
        print("   ✗ No AWS configuration available")


def debug_bucket_access(bucket_name="zarrpods"):
    """Debug access to a specific bucket."""
    print(f"\n=== Bucket Access Debug: {bucket_name} ===")
    
    if not _AWS_S3_STORAGE_OPTIONS:
        print("   ✗ No AWS configuration - cannot test bucket access")
        return
    
    try:
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        
        # Check if bucket exists
        print(f"1. Checking if bucket '{bucket_name}' exists:")
        try:
            bucket_exists = fs.exists(bucket_name)
            print(f"   Bucket exists: {bucket_exists}")
            
            if bucket_exists:
                # Try to list contents
                try:
                    contents = fs.ls(bucket_name, detail=False)
                    print(f"   ✓ Can list bucket contents: {len(contents)} items")
                    for item in contents[:3]:
                        print(f"     - {item}")
                    if len(contents) > 3:
                        print(f"     ... and {len(contents) - 3} more")
                except Exception as e:
                    print(f"   ✗ Cannot list bucket contents: {e}")
            else:
                print(f"   ✗ Bucket '{bucket_name}' does not exist or is not accessible")
                
        except Exception as e:
            print(f"   ✗ Error checking bucket existence: {e}")
            
    except Exception as e:
        print(f"   ✗ Cannot create S3FileSystem: {e}")


def debug_directory_creation(test_bucket="zarrpods", test_path="test-directory-creation"):
    """Debug directory creation on S3."""
    print(f"\n=== Directory Creation Debug ===")
    
    if not _AWS_S3_STORAGE_OPTIONS:
        print("   ✗ No AWS configuration - cannot test directory creation")
        return
    
    try:
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        
        # Test hierarchical path creation
        test_paths = [
            f"{test_bucket}/{test_path}",
            f"{test_bucket}/{test_path}/Q00_5",
            f"{test_bucket}/{test_path}/Q00_5/Q01_3",
            f"{test_bucket}/{test_path}/Q00_5/Q01_3/Q02_2"
        ]
        
        print("1. Testing directory creation:")
        for path in test_paths:
            try:
                print(f"   Testing path: {path}")
                
                # Check if exists
                exists = fs.exists(path)
                print(f"     Exists: {exists}")
                
                if not exists:
                    # Try to create
                    fs.makedirs(path, exist_ok=True)
                    print(f"     ✓ Created successfully")
                    
                    # Verify creation
                    exists_after = fs.exists(path)
                    print(f"     Exists after creation: {exists_after}")
                else:
                    print(f"     Already exists")
                    
            except Exception as e:
                print(f"     ✗ Error with path {path}: {e}")
                break
        
        # Cleanup test directories
        print("\n2. Cleaning up test directories:")
        try:
            cleanup_path = f"{test_bucket}/{test_path}"
            if fs.exists(cleanup_path):
                fs.rm(cleanup_path, recursive=True)
                print(f"   ✓ Cleaned up {cleanup_path}")
        except Exception as e:
            print(f"   Warning: Could not clean up test directories: {e}")
            
    except Exception as e:
        print(f"   ✗ Cannot create S3FileSystem: {e}")


def debug_zarr_path_creation():
    """Debug the hierarchical zarr path creation process."""
    print(f"\n=== Zarr Path Creation Debug ===")
    
    # Create test data
    data = {
        'sids': [3445253714938429444],
        'lat': [32.0],
        'lon': [-120.0],
        'value': [1.0]
    }
    
    sdf = STAREDataFrame(pd.DataFrame(data), sids='sids')
    
    # Test path generation
    dataset = "DEBUG_TEST"
    level = 8
    
    print(f"1. Test parameters:")
    print(f"   Dataset: {dataset}")
    print(f"   Level: {level}")
    print(f"   SID: {data['sids'][0]}")
    
    # Generate hierarchical path
    coerced = sdf.to_sids_level(level=level, clear_to_level=True)
    grouped = sdf.groupby(coerced[sdf._sid_column_name], sort=False)
    
    print(f"\n2. Generated paths:")
    for group_id, gdf in grouped:
        if isinstance(group_id, (int, np.integer)) and group_id >= 0:
            hierarchical_path = sdf.generate_zarr_path(group_id, dataset)
            
            print(f"   Group ID: {group_id}")
            print(f"   Hierarchical path: {hierarchical_path}")
            
            # Test with different S3 base paths
            test_bases = [
                "s3://zarrpods/debug-test",
                "s3://test-bucket/debug-test"
            ]
            
            for base in test_bases:
                full_path = f"{base}/{hierarchical_path}"
                parent_path = '/'.join(full_path.split('/')[:-1])
                
                print(f"\n   Base: {base}")
                print(f"   Full path: {full_path}")
                print(f"   Parent path: {parent_path}")
                
                # Test what our directory creation logic would do
                if _AWS_S3_STORAGE_OPTIONS:
                    try:
                        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
                        bucket_name = base.split('/')[2]  # Extract bucket name
                        
                        print(f"   Bucket name: {bucket_name}")
                        
                        # Check bucket existence
                        bucket_exists = fs.exists(bucket_name)
                        print(f"   Bucket exists: {bucket_exists}")
                        
                        if bucket_exists:
                            parent_exists = fs.exists(parent_path)
                            print(f"   Parent exists: {parent_exists}")
                        else:
                            print(f"   ✗ Cannot test parent - bucket doesn't exist")
                            
                    except Exception as e:
                        print(f"   ✗ Error testing paths: {e}")


def debug_zarr_open_group():
    """Debug the zarr.open_group call specifically."""
    print(f"\n=== Zarr Open Group Debug ===")
    
    if not _AWS_S3_STORAGE_OPTIONS:
        print("   ✗ No AWS configuration - cannot test zarr.open_group")
        return
    
    import zarr
    
    # Test zarr.open_group with different scenarios
    test_scenarios = [
        {
            "name": "Simple path (should work)",
            "path": "s3://zarrpods/debug-simple-test"
        },
        {
            "name": "Hierarchical path (causing issues)",
            "path": "s3://zarrpods/debug-hierarchical/Q00_5/Q01_3/Q02_2/TEST"
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n1. Testing scenario: {scenario['name']}")
        print(f"   Path: {scenario['path']}")
        
        try:
            # Try to open zarr group
            zg = zarr.open_group(scenario['path'], mode="w", storage_options=_AWS_S3_STORAGE_OPTIONS)
            print(f"   ✓ zarr.open_group succeeded")
            
            # Try to create a simple array
            arr = zg.empty('test_array', shape=(10,), dtype='f4')
            arr[:] = np.random.rand(10)
            print(f"   ✓ Array creation succeeded")
            
            # Clean up
            try:
                fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
                base_path = scenario['path'].replace('s3://', '').split('/')[0]
                cleanup_path = '/'.join(scenario['path'].replace('s3://', '').split('/')[1:])
                fs.rm(f"{base_path}/{cleanup_path}", recursive=True)
                print(f"   ✓ Cleanup successful")
            except Exception as cleanup_error:
                print(f"   Warning: Cleanup failed: {cleanup_error}")
                
        except Exception as e:
            print(f"   ✗ zarr.open_group failed: {e}")
            print(f"   Error type: {type(e).__name__}")


def main():
    """Run all debugging tests."""
    print("S3 FileNotFoundError Debug Script")
    print("=" * 60)
    
    # Debug S3 configuration
    debug_s3_configuration()
    
    # Debug bucket access
    debug_bucket_access()
    
    # Debug directory creation
    debug_directory_creation()
    
    # Debug zarr path creation
    debug_zarr_path_creation()
    
    # Debug zarr.open_group
    debug_zarr_open_group()
    
    print("\n" + "=" * 60)
    print("Debug Summary:")
    print("If you see errors above, they indicate the root cause of the issue.")
    print("Common causes:")
    print("- Missing or incorrect AWS credentials")
    print("- Bucket doesn't exist or is not accessible")
    print("- Permission issues with the S3 bucket")
    print("- Network connectivity problems")
    print("- Region mismatch between credentials and bucket")
    
    print("\nNext Steps:")
    print("1. Verify AWS credentials are correct")
    print("2. Confirm the bucket exists and is accessible")
    print("3. Check bucket permissions")
    print("4. Verify region configuration")


if __name__ == "__main__":
    main()
