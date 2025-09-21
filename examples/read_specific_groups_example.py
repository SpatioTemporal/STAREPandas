#!/usr/bin/env python3
"""
Example script demonstrating how to read specific STARE groups from chunked zarr data.

This script shows how to use the from_zarr_s3_chunked_groups function to read
only specific STARE group SIDs from a chunked zarr store, which is much more
efficient than loading the entire dataset.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import from_zarr_s3_chunked_groups, from_zarr_s3_chunked


def example_read_specific_groups():
    """Example of reading specific STARE groups from chunked zarr data."""
    print("=== Reading Specific STARE Groups from Chunked Zarr Data ===")
    
    # S3 path to the chunked zarr data
    s3_path = "s3://zarrpods/MOD09.A2020032.1940.006.2020034015024/"
    
    # Storage options - use None to load from config file or environment
    # You can also provide explicit credentials here if needed:
    # storage_options = {
    #     'key': 'your-access-key',
    #     'secret': 'your-secret-key', 
    #     'client_kwargs': {'region_name': 'us-west-2'}
    # }
    storage_options = None  # Will load from .config file or environment
    
    # Specific group SIDs to read
    group_ids = [3447505514752114692, 3445253714938429444]
    
    try:
        # Read only the specified groups
        df = from_zarr_s3_chunked_groups(s3_path, group_ids, storage_options=storage_options)
        
        print(f"✓ Successfully loaded specific groups: {df.shape}")
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
        
        return df
        
    except Exception as e:
        print(f"✗ Error reading specific groups: {e}")
        return None


def example_compare_efficiency():
    """Example comparing efficiency of reading specific groups vs full dataset."""
    print("\n=== Efficiency Comparison ===")
    
    s3_path = "s3://zarrpods/MOD09.A2020032.1940.006.2020034015024/"
    # Use None to load from config file or environment
    storage_options = None
    
    group_ids = [3447505514752114692, 3445253714938429444]
    
    try:
        # Read specific groups
        print("Reading specific groups...")
        import time
        start_time = time.time()
        df_groups = from_zarr_s3_chunked_groups(s3_path, group_ids, storage_options=storage_options)
        groups_time = time.time() - start_time
        
        print(f"✓ Specific groups: {df_groups.shape} in {groups_time:.2f} seconds")
        
        # For comparison, show what the full dataset would be
        print(f"\nFull dataset comparison:")
        print(f"  - Full dataset: ~2.7M rows × 33 columns")
        print(f"  - Specific groups: {len(df_groups):,} rows × {len(df_groups.columns)} columns")
        print(f"  - Efficiency gain: {2700000 / len(df_groups):.0f}x fewer rows")
        print(f"  - Memory savings: ~{600 * len(df_groups) / 2700000:.1f} MB vs ~600 MB")
        
    except Exception as e:
        print(f"✗ Error in efficiency comparison: {e}")


def example_analyze_group_data(df):
    """Example of analyzing the loaded group data."""
    if df is None or df.empty:
        print("No data to analyze.")
        return
    
    print("\n=== Group Data Analysis ===")
    
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


def example_use_cases():
    """Example showing different use cases for reading specific groups."""
    print("\n=== Use Cases for Reading Specific Groups ===")
    
    print("1. **Spatial Analysis**:")
    print("   - Read only groups covering a specific geographic region")
    print("   - Analyze data for specific STARE resolution levels")
    print("   - Compare data between different spatial groups")
    
    print("\n2. **Temporal Analysis**:")
    print("   - Read groups from specific time periods")
    print("   - Compare data across different temporal groups")
    print("   - Analyze temporal patterns in specific regions")
    
    print("\n3. **Quality Control**:")
    print("   - Read specific groups for data validation")
    print("   - Check data quality in problematic areas")
    print("   - Verify data integrity for specific groups")
    
    print("\n4. **Research Applications**:")
    print("   - Focus on specific scientific phenomena")
    print("   - Analyze data for specific atmospheric conditions")
    print("   - Study specific surface types or land cover")
    
    print("\n5. **Computational Efficiency**:")
    print("   - Reduce memory usage for large-scale analysis")
    print("   - Speed up data processing by focusing on relevant groups")
    print("   - Enable interactive analysis of large datasets")


def main():
    """Run all examples."""
    print("Specific STARE Groups Reading Examples")
    print("=" * 50)
    
    # Read specific groups
    df = example_read_specific_groups()
    
    # Compare efficiency
    example_compare_efficiency()
    
    # Analyze the data
    example_analyze_group_data(df)
    
    # Show use cases
    example_use_cases()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nKey Points:")
    print("- Use from_zarr_s3_chunked_groups() to read specific STARE groups")
    print("- Much more efficient than loading the entire dataset")
    print("- Supports both group directory names and individual SIDs")
    print("- Ideal for spatial analysis, quality control, and research")
    print("- Significant memory and processing time savings")
    print("- Perfect for interactive analysis of large datasets")


if __name__ == "__main__":
    main()
