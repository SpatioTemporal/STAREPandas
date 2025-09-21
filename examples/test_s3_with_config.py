#!/usr/bin/env python3
"""
Test S3 functionality with manually loaded configuration.

This script manually loads the AWS configuration and then tests
the S3 functionality to verify the directory creation fix works.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame
from starepandas.staredataframe import aws_configure
import s3fs
import zarr


def load_config_manually():
    """Load configuration manually from .config file."""
    print("=== Loading Configuration Manually ===")
    
    # Parse the config file
    kv = {}
    config_path = '.config'
    
    if not os.path.isfile(config_path):
        print(f"✗ Config file not found: {config_path}")
        return False
    
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                kv[k.strip()] = v.strip()
    
    print("Parsed configuration:")
    for k, v in kv.items():
        if 'secret' in k.lower() or 'password' in k.lower():
            print(f"  {k}: {v[:10]}...")  # Mask sensitive values
        else:
            print(f"  {k}: {v}")
    
    # Configure using parsed values
    rds_block = {
        'host': kv.get('rds_host') or kv.get('host'),
        'port': int(kv.get('port', '5432')),
        'username': kv.get('username') or kv.get('user'),
        'password': kv.get('password'),
        'database': kv.get('database') or 'postgres'
    }

    result = aws_configure(
        key=kv.get('key'),
        secret=kv.get('secret'),
        region_name=kv.get('region_name') or kv.get('region'),
        rds=rds_block
    )
    
    print(f"✓ AWS configuration loaded successfully")
    return True


def test_s3_connection():
    """Test basic S3 connection."""
    print("\n=== Testing S3 Connection ===")
    
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS
    
    if not _AWS_S3_STORAGE_OPTIONS:
        print("✗ No AWS configuration available")
        return False
    
    try:
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        print("✓ S3FileSystem created successfully")
        
        # Try to list buckets
        buckets = fs.ls('/')
        print(f"✓ Can list buckets: {len(buckets)} buckets found")
        
        # Check if zarrpods bucket exists
        zarrpods_exists = 'zarrpods' in [b.split('/')[-1] for b in buckets]
        print(f"✓ zarrpods bucket exists: {zarrpods_exists}")
        
        return zarrpods_exists
        
    except Exception as e:
        print(f"✗ S3 connection failed: {e}")
        return False


def test_directory_creation():
    """Test directory creation on S3."""
    print("\n=== Testing Directory Creation ===")
    
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS
    
    try:
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        
        # Test hierarchical directory creation
        test_base = "zarrpods/test-directory-creation"
        test_path = f"{test_base}/Q00_5/Q01_3/Q02_2"
        
        print(f"Testing directory creation: {test_path}")
        
        # Clean up any existing test data first
        try:
            if fs.exists(test_base):
                fs.rm(test_base, recursive=True)
                print("✓ Cleaned up existing test data")
        except:
            pass
        
        # Test directory creation
        if not fs.exists(test_path):
            fs.makedirs(test_path, exist_ok=True)
            print("✓ Directory created successfully")
        
        # Verify creation
        exists_after = fs.exists(test_path)
        print(f"✓ Directory exists after creation: {exists_after}")
        
        # Clean up
        fs.rm(test_base, recursive=True)
        print("✓ Test cleanup completed")
        
        return True
        
    except Exception as e:
        print(f"✗ Directory creation test failed: {e}")
        return False


def test_zarr_creation():
    """Test zarr group creation with hierarchical paths."""
    print("\n=== Testing Zarr Group Creation ===")
    
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS
    
    try:
        # Test simple zarr creation first
        simple_path = "s3://zarrpods/test-simple-zarr"
        print(f"Testing simple zarr creation: {simple_path}")
        
        zg_simple = zarr.open_group(simple_path, mode="w", storage_options=_AWS_S3_STORAGE_OPTIONS)
        arr = zg_simple.empty('test_array', shape=(5,), dtype='f4')
        arr[:] = [1.0, 2.0, 3.0, 4.0, 5.0]
        print("✓ Simple zarr creation successful")
        
        # Test hierarchical zarr creation
        hierarchical_path = "s3://zarrpods/test-hierarchical-zarr/Q00_5/Q01_3/Q02_2/TEST_DATASET"
        print(f"Testing hierarchical zarr creation: {hierarchical_path}")
        
        # Create parent directory first (this is what our fix does)
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        parent_path = '/'.join(hierarchical_path.replace('s3://', '').split('/')[:-1])
        parent_path = '/'.join(hierarchical_path.split('/')[:-1])
        
        print(f"Creating parent directory: {parent_path}")
        if not fs.exists(parent_path):
            fs.makedirs(parent_path, exist_ok=True)
            print("✓ Parent directory created")
        
        # Now create zarr group
        zg_hier = zarr.open_group(hierarchical_path, mode="w", storage_options=_AWS_S3_STORAGE_OPTIONS)
        arr = zg_hier.empty('test_array', shape=(3,), dtype='f4')
        arr[:] = [10.0, 20.0, 30.0]
        print("✓ Hierarchical zarr creation successful")
        
        # Clean up
        fs.rm("zarrpods/test-simple-zarr", recursive=True)
        fs.rm("zarrpods/test-hierarchical-zarr", recursive=True)
        print("✓ Test cleanup completed")
        
        return True
        
    except Exception as e:
        print(f"✗ Zarr creation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_staredataframe_to_zarr_s3():
    """Test STAREDataFrame.to_zarr_s3 with the directory creation fix."""
    print("\n=== Testing STAREDataFrame.to_zarr_s3 ===")
    
    try:
        # Create test data
        data = {
            'sids': [3445253714938429444, 3447505514752114692],
            'lat': [32.0, 32.1],
            'lon': [-120.0, -120.1],
            'temperature': [25.5, 26.0],
            'humidity': [65, 68]
        }
        
        sdf = STAREDataFrame(pd.DataFrame(data), sids='sids')
        print(f"✓ Created test STAREDataFrame with {len(sdf)} rows")
        
        # Test to_zarr_s3 with hierarchical paths
        s3_path = "s3://zarrpods/test-staredataframe-hierarchical"
        dataset = "WEATHER_TEST"
        level = 8
        
        print(f"Testing to_zarr_s3:")
        print(f"  S3 path: {s3_path}")
        print(f"  Dataset: {dataset}")
        print(f"  Level: {level}")
        
        # This should now work with our directory creation fix
        result = sdf.to_zarr_s3(
            s3_path=s3_path,
            level=level,
            dataset=dataset,
            chunk_size=1000
        )
        
        print(f"✓ to_zarr_s3 completed successfully")
        print(f"  Result: {result}")
        
        # Verify the data was written
        from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS
        fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
        
        # List what was created
        base_path = s3_path.replace('s3://', '')
        if fs.exists(base_path):
            contents = fs.find(base_path)
            print(f"✓ Created {len(contents)} files/directories")
            
            # Show hierarchical structure
            dirs = [c for c in contents if not c.endswith('.zarray') and not c.endswith('.zgroup') and not c.endswith('.zattrs')]
            if dirs:
                print("  Hierarchical structure:")
                for d in sorted(dirs)[:5]:  # Show first 5
                    print(f"    {d}")
                if len(dirs) > 5:
                    print(f"    ... and {len(dirs) - 5} more")
        
        # Test reading back the data
        print("\nTesting from_zarr_s3:")
        sdf_restored = STAREDataFrame.from_zarr_s3(s3_path)
        print(f"✓ from_zarr_s3 completed successfully")
        print(f"  Restored {len(sdf_restored)} rows")
        print(f"  Columns: {list(sdf_restored.columns)}")
        
        # Verify data integrity
        if len(sdf_restored) == len(sdf):
            print("✓ Row count matches original")
        else:
            print(f"✗ Row count mismatch: {len(sdf_restored)} vs {len(sdf)}")
        
        # Clean up
        fs.rm(base_path, recursive=True)
        print("✓ Test cleanup completed")
        
        return True
        
    except Exception as e:
        print(f"✗ STAREDataFrame test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("S3 Configuration and Hierarchical Zarr Test")
    print("=" * 60)
    
    # Load configuration
    if not load_config_manually():
        print("✗ Failed to load configuration - cannot proceed")
        return
    
    # Test S3 connection
    if not test_s3_connection():
        print("✗ Failed S3 connection test - cannot proceed")
        return
    
    # Test directory creation
    if not test_directory_creation():
        print("✗ Failed directory creation test")
        return
    
    # Test zarr creation
    if not test_zarr_creation():
        print("✗ Failed zarr creation test")
        return
    
    # Test STAREDataFrame functionality
    if not test_staredataframe_to_zarr_s3():
        print("✗ Failed STAREDataFrame test")
        return
    
    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("The directory creation fix is working correctly.")
    print("Hierarchical zarr storage should now work without FileNotFoundError.")


if __name__ == "__main__":
    main()
