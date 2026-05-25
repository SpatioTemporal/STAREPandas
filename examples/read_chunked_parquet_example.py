#!/usr/bin/env python3
"""
Example script demonstrating how to read chunked Parquet data from S3.

This script shows how to use the from_s3 function to read
Parquet data stored in a chunked format (single Parquet partition) rather than
the grouped format expected by STAREDataFrame.from_s3().
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import from_s3


def example_read_chunked_parquet():
    """Example of reading chunked Parquet data from S3."""
    print("=== Reading Chunked Parquet Data from S3 ===")
    
    # S3 path to the chunked Parquet data
    s3_path = "s3://zarrpods/MOD09.A2020032.1940.006.2020034015024/"
    
    # Storage options - use None to load from config file or environment
    # You can also provide explicit credentials here if needed:
    # storage_options = {
    #     'key': 'your-access-key',
    #     'secret': 'your-secret-key', 
    #     'client_kwargs': {'region_name': 'us-west-2'}
    # }
    storage_options = None  # Will load from .config file or environment
    
    try:
        # Read the chunked Parquet data
        df = from_s3(s3_path, storage_options=storage_options)
        
        print(f"✓ Successfully loaded data: {df.shape}")
        print(f"✓ Data type: {type(df)}")
        print(f"✓ Columns: {len(df.columns)}")
        
        # Show basic information
        print(f"\nData Summary:")
        print(f"  - Rows: {len(df):,}")
        print(f"  - Columns: {len(df.columns)}")
        
        # Show coordinate information
        if 'lat' in df.columns and 'lon' in df.columns:
            print(f"  - Latitude range: {df['lat'].min():.3f} to {df['lat'].max():.3f}")
            print(f"  - Longitude range: {df['lon'].min():.3f} to {df['lon'].max():.3f}")
        
        # Show STARE information
        if 'sids' in df.columns:
            print(f"  - STARE SIDs range: {df['sids'].min()} to {df['sids'].max()}")
            print(f"  - Unique SIDs: {df['sids'].nunique():,}")
        
        # Show sample data
        print(f"\nSample data (first 5 rows):")
        key_columns = ['lat', 'lon', 'sids'] if all(col in df.columns for col in ['lat', 'lon', 'sids']) else df.columns[:3]
        print(df[key_columns].head())
        
        # Show data columns (excluding coordinates)
        data_columns = [col for col in df.columns if col not in ['lat', 'lon', 'sids', 'chunk_name']]
        if data_columns:
            print(f"\nData columns ({len(data_columns)}):")
            for i, col in enumerate(data_columns[:10]):  # Show first 10
                print(f"  - {col}")
            if len(data_columns) > 10:
                print(f"  ... and {len(data_columns) - 10} more")
        
        return df
        
    except Exception as e:
        print(f"✗ Error reading chunked Parquet data: {e}")
        return None


def example_compare_formats():
    """Example showing the difference between chunked and grouped Parquet formats."""
    print("\n=== Chunked vs Grouped Parquet Formats ===")
    
    print("Chunked Parquet Format (what we just read):")
    print("  - Single Parquet partition at root level")
    print("  - All arrays stored as chunked arrays in the root")
    print("  - Efficient for large datasets")
    print("  - Use: from_s3()")
    
    print("\nGrouped Parquet Format (expected by from_s3):")
    print("  - Root contains multiple group directories")
    print("  - Each group directory contains arrays for that STARE group")
    print("  - Each group has a .zgroup file")
    print("  - Use: STAREDataFrame.from_s3()")
    
    print("\nWhy from_s3() returned empty DataFrame:")
    print("  - It looks for group directories with .zgroup files")
    print("  - Your data is stored in chunked format, not grouped format")
    print("  - Solution: Use from_s3() instead")


def example_analyze_data(df):
    """Example of analyzing the loaded data."""
    if df is None or df.empty:
        print("No data to analyze.")
        return
    
    print("\n=== Data Analysis ===")
    
    # Basic statistics
    print("Basic Statistics:")
    print(f"  - Total data points: {len(df):,}")
    print(f"  - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Coordinate analysis
    if 'lat' in df.columns and 'lon' in df.columns:
        print(f"\nGeographic Coverage:")
        print(f"  - Latitude: {df['lat'].min():.3f}° to {df['lat'].max():.3f}°")
        print(f"  - Longitude: {df['lon'].min():.3f}° to {df['lon'].max():.3f}°")
        print(f"  - Approximate area: {df['lat'].max() - df['lat'].min():.3f}° × {df['lon'].max() - df['lon'].min():.3f}°")
    
    # STARE analysis
    if 'sids' in df.columns:
        print(f"\nSTARE Analysis:")
        print(f"  - Unique SIDs: {df['sids'].nunique():,}")
        print(f"  - SID density: {df['sids'].nunique() / len(df) * 100:.2f}%")
        
        # Check for any patterns in SIDs
        sids_sorted = df['sids'].sort_values()
        sid_diffs = sids_sorted.diff().dropna()
        print(f"  - SID differences: min={sid_diffs.min()}, max={sid_diffs.max()}, mean={sid_diffs.mean():.0f}")
    
    # Data quality
    print(f"\nData Quality:")
    for col in df.columns:
        if col not in ['lat', 'lon', 'sids', 'chunk_name']:
            null_count = df[col].isnull().sum()
            null_pct = null_count / len(df) * 100
            print(f"  - {col}: {null_pct:.1f}% missing values")


def main():
    """Run all examples."""
    print("Chunked Parquet Reading Examples")
    print("=" * 50)
    
    # Read the data
    df = example_read_chunked_parquet()
    
    # Compare formats
    example_compare_formats()
    
    # Analyze the data
    example_analyze_data(df)
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nKey Points:")
    print("- Use from_s3() for chunked Parquet format")
    print("- Use STAREDataFrame.from_s3() for grouped Parquet format")
    print("- Chunked format is more efficient for large datasets")
    print("- Grouped format allows for better partitioning by STARE groups")
    print("- Your data is in chunked format, which is why from_s3() returned empty")


if __name__ == "__main__":
    main()
