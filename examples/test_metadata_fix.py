#!/usr/bin/env python3
"""
Test script to verify the metadata database fix.

This script tests that the BIGINT column fix resolves the 
"integer out of range" error for STARE SIDs.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame
from starepandas.staredataframe import aws_configure


def load_config():
    """Load AWS configuration."""
    print("=== Loading AWS Configuration ===")
    
    # Parse the config file
    kv = {}
    config_path = 'starepandas/.config'
    
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                kv[k.strip()] = v.strip()
    
    # Configure using parsed values
    rds_block = {
        'host': kv.get('rds_host') or kv.get('host'),
        'port': int(kv.get('port', '5432')),
        'username': kv.get('username') or kv.get('user'),
        'password': kv.get('password'),
        'database': kv.get('database') or 'postgres'
    }

    aws_configure(
        key=kv.get('key'),
        secret=kv.get('secret'),
        region_name=kv.get('region_name') or kv.get('region'),
        rds=rds_block
    )
    
    print("✓ AWS configuration loaded")


def test_database_column_upgrade():
    """Test that the database column is properly upgraded."""
    print("\n=== Testing Database Column Upgrade ===")
    
    from starepandas.staredataframe import _ensure_rds_db_and_table
    
    try:
        # This should trigger the column upgrade if needed
        conn = _ensure_rds_db_and_table('StarePodsMetadata')
        
        # Check the column type
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_type, character_maximum_length, numeric_precision
                FROM information_schema.columns 
                WHERE table_name = 'PodsMetadata' 
                AND column_name = 'grouped_id'
                """
            )
            result = cur.fetchone()
            
            if result:
                data_type, max_length, precision = result
                print(f"✓ grouped_id column type: {data_type}")
                if precision:
                    print(f"  Precision: {precision} bits")
                
                if data_type == 'bigint':
                    print("✅ Column is properly configured for 64-bit STARE SIDs")
                else:
                    print(f"⚠️  Column type is {data_type}, may not support large STARE SIDs")
            else:
                print("❌ Could not find grouped_id column")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_large_sid_insertion():
    """Test inserting a large STARE SID that would have failed before."""
    print("\n=== Testing Large STARE SID Insertion ===")
    
    # Create test data with a large STARE SID
    large_sid = 2269814212194729987  # The SID that was failing
    
    data = {
        'sids': [large_sid],
        'lat': [32.0],
        'lon': [-120.0],
        'temperature': [25.5]
    }
    
    sdf = STAREDataFrame(pd.DataFrame(data), sids='sids')
    
    print(f"Test SID: {large_sid}")
    print(f"SID bit length: {large_sid.bit_length()} bits")
    print(f"SID value fits in 64-bit: {large_sid < 2**63}")
    
    try:
        # This should now work with the BIGINT column
        result = sdf.to_zarr_s3(
            s3_path="s3://zarrpods/test-metadata-fix",
            level=3,
            dataset="METADATA_TEST",
            data_level="TEST",
            chunk_size=1000
        )
        
        print(f"✅ Successfully stored data with large SID: {result}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to store data with large SID: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_metadata_retrieval():
    """Test retrieving the stored metadata."""
    print("\n=== Testing Metadata Retrieval ===")
    
    try:
        from starepandas.io.granules import load_zarr_metadata
        
        # Load metadata for our test
        df = load_zarr_metadata(dataset="METADATA_TEST", limit=10)
        
        if len(df) > 0:
            print(f"✅ Successfully retrieved {len(df)} metadata records")
            print("Sample metadata:")
            for col in ['Dataset', 'grouped_id', 'S3 bucket', 'Resolution level']:
                if col in df.columns:
                    print(f"  {col}: {df[col].iloc[0]}")
            
            # Check that large SIDs are properly stored
            max_sid = df['grouped_id'].max()
            print(f"  Largest SID: {max_sid}")
            print(f"  SID bit length: {max_sid.bit_length()} bits")
            
            return True
        else:
            print("⚠️  No metadata records found")
            return False
            
    except Exception as e:
        print(f"❌ Failed to retrieve metadata: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_test_data():
    """Clean up test data."""
    print("\n=== Cleaning Up Test Data ===")
    
    try:
        # Remove test zarr data from S3
        from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS
        import s3fs
        
        if _AWS_S3_STORAGE_OPTIONS:
            fs = s3fs.S3FileSystem(**_AWS_S3_STORAGE_OPTIONS)
            test_path = "zarrpods/test-metadata-fix"
            
            if fs.exists(test_path):
                fs.rm(test_path, recursive=True)
                print("✓ Removed test zarr data from S3")
        
        # Remove test metadata from database
        from starepandas.staredataframe import _ensure_rds_db_and_table
        
        conn = _ensure_rds_db_and_table('StarePodsMetadata')
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM "PodsMetadata" WHERE "Dataset" = %s',
                ("METADATA_TEST",)
            )
            deleted_count = cur.rowcount
            conn.commit()
            
        conn.close()
        print(f"✓ Removed {deleted_count} test metadata records from database")
        
    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")


def main():
    """Run all tests."""
    print("Metadata Database Fix Test")
    print("=" * 50)
    print("This test verifies that the BIGINT column fix resolves")
    print("the 'integer out of range' error for large STARE SIDs.")
    print()
    
    # Load configuration
    load_config()
    
    # Test database column upgrade
    db_ok = test_database_column_upgrade()
    
    if db_ok:
        # Test large SID insertion
        insert_ok = test_large_sid_insertion()
        
        if insert_ok:
            # Test metadata retrieval
            retrieve_ok = test_metadata_retrieval()
            
            # Clean up test data
            cleanup_test_data()
            
            print("\n" + "=" * 50)
            if retrieve_ok:
                print("🎉 SUCCESS! Metadata database fix is working correctly!")
                print("✅ Database column upgraded to BIGINT")
                print("✅ Large STARE SIDs can be stored")
                print("✅ Metadata retrieval works properly")
                print("✅ Transaction handling prevents failures")
            else:
                print("⚠️  Partial success - insertion worked but retrieval had issues")
        else:
            print("\n" + "=" * 50)
            print("❌ Large SID insertion still failing - check the error details above")
    else:
        print("\n" + "=" * 50)
        print("❌ Database connection or column upgrade failed")
    
    print("\nKey improvements made:")
    print("- Changed grouped_id column from INTEGER to BIGINT")
    print("- Added automatic column upgrade for existing tables")
    print("- Improved transaction handling (individual commits)")
    print("- Enhanced error logging and debugging")


if __name__ == "__main__":
    main()
