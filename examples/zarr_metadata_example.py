#!/usr/bin/env python3
"""
Example demonstrating the zarr metadata functions in starepandas.io.granules.

This example shows how to:
1. Load metadata from the RDS database for zarr data stored in S3
2. Get summary statistics about stored zarr datasets
3. Filter and query metadata by various criteria
"""

import os
import sys
import datetime

# Add the parent directory to the path to import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import load_zarr_metadata, get_zarr_summary


def example_load_all_metadata():
    """Example loading all metadata from the database."""
    print("=== Load All Metadata Example ===")
    
    try:
        # Load all metadata
        df = load_zarr_metadata()
        
        if df.empty:
            print("No metadata found in database.")
            print("This is expected if no zarr data has been stored yet.")
            return
        
        print(f"✓ Loaded {len(df)} metadata records")
        print(f"✓ Columns: {list(df.columns)}")
        
        # Check if we have date data
        if 'RawData Collected Time' in df.columns and not df['RawData Collected Time'].isna().all():
            print(f"✓ Date range: {df['RawData Collected Time'].min()} to {df['RawData Collected Time'].max()}")
        
        # Check if we have num_rows data (from parsed JSON metadata)
        if 'num_rows' in df.columns and not df['num_rows'].isna().all():
            print(f"✓ Total rows across all groups: {df['num_rows'].sum()}")
        
        # Show sample data
        print("\nSample data:")
        print(df.head())
        
    except Exception as e:
        print(f"✗ Error loading metadata: {e}")
        print("This might be expected if RDS database is not configured.")


def example_filter_by_dataset():
    """Example filtering metadata by dataset."""
    print("\n=== Filter by Dataset Example ===")
    
    try:
        # Load metadata for specific dataset
        df = load_zarr_metadata(dataset="MOD05_L2")
        
        if df.empty:
            print("No MOD05_L2 metadata found.")
            return
        
        print(f"✓ Found {len(df)} MOD05_L2 records")
        print(f"✓ Unique data levels: {df['DataLevel'].unique()}")
        print(f"✓ Unique resolution levels: {df['Resolution level'].unique()}")
        
    except Exception as e:
        print(f"✗ Error filtering by dataset: {e}")


def example_filter_by_date_range():
    """Example filtering metadata by date range."""
    print("\n=== Filter by Date Range Example ===")
    
    try:
        # Load metadata for specific date range
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        df = load_zarr_metadata(
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            print(f"No metadata found for date range {start_date} to {end_date}")
            return
        
        print(f"✓ Found {len(df)} records in date range {start_date} to {end_date}")
        print(f"✓ Datasets: {df['Dataset'].unique()}")
        
    except Exception as e:
        print(f"✗ Error filtering by date range: {e}")


def example_filter_by_s3_bucket():
    """Example filtering metadata by S3 bucket."""
    print("\n=== Filter by S3 Bucket Example ===")
    
    try:
        # Load metadata for specific S3 bucket
        df = load_zarr_metadata(s3_bucket="my-data-bucket")
        
        if df.empty:
            print("No metadata found for S3 bucket 'my-data-bucket'")
            return
        
        print(f"✓ Found {len(df)} records in S3 bucket")
        print(f"✓ Total rows across all groups: {df['num_rows'].sum()}")
        
    except Exception as e:
        print(f"✗ Error filtering by S3 bucket: {e}")


def example_custom_ordering_and_limit():
    """Example with custom ordering and limit."""
    print("\n=== Custom Ordering and Limit Example ===")
    
    try:
        # Load metadata with custom ordering and limit
        df = load_zarr_metadata(
            order_by="RawData Collected Time",
            limit=10
        )
        
        if df.empty:
            print("No metadata found.")
            return
        
        print(f"✓ Loaded {len(df)} most recent records")
        print("Recent records:")
        for _, row in df.iterrows():
            print(f"  - {row['Dataset']} ({row['DataLevel']}) - {row['RawData Collected Time']}")
        
    except Exception as e:
        print(f"✗ Error with custom ordering: {e}")


def example_get_summary():
    """Example getting summary statistics."""
    print("\n=== Get Summary Statistics Example ===")
    
    try:
        # Get summary of all data
        summary = get_zarr_summary()
        
        if summary.empty:
            print("No data found for summary.")
            return
        
        print("✓ Summary statistics:")
        print(summary)
        
        # Get summary for specific dataset
        modis_summary = get_zarr_summary(dataset="MOD05_L2")
        
        if not modis_summary.empty:
            print("\n✓ MODIS summary:")
            print(modis_summary)
        
    except Exception as e:
        print(f"✗ Error getting summary: {e}")


def example_analyze_metadata():
    """Example analyzing metadata in detail."""
    print("\n=== Analyze Metadata Example ===")
    
    try:
        # Load all metadata
        df = load_zarr_metadata()
        
        if df.empty:
            print("No metadata to analyze.")
            return
        
        print("✓ Metadata Analysis:")
        print(f"  - Total records: {len(df)}")
        print(f"  - Unique datasets: {df['Dataset'].nunique()}")
        print(f"  - Unique data levels: {df['DataLevel'].nunique()}")
        print(f"  - Unique S3 buckets: {df['S3 bucket'].nunique()}")
        
        # Check if num_rows is available (from parsed JSON metadata)
        if 'num_rows' in df.columns and not df['num_rows'].isna().all():
            print(f"  - Total rows across all groups: {df['num_rows'].sum()}")
        
        # Dataset breakdown
        print("\nDataset breakdown:")
        dataset_counts = df['Dataset'].value_counts()
        for dataset, count in dataset_counts.items():
            print(f"  - {dataset}: {count} groups")
        
        # Resolution level breakdown
        print("\nResolution level breakdown:")
        resolution_counts = df['Resolution level'].value_counts().sort_index()
        for level, count in resolution_counts.items():
            print(f"  - Level {level}: {count} groups")
        
        # Date range analysis
        if 'RawData Collected Time' in df.columns and not df['RawData Collected Time'].isna().all():
            print(f"\nDate range: {df['RawData Collected Time'].min()} to {df['RawData Collected Time'].max()}")
        
    except Exception as e:
        print(f"✗ Error analyzing metadata: {e}")


def main():
    """Run all examples."""
    print("Zarr Metadata Functions Examples")
    print("=" * 50)
    
    # Run examples
    example_load_all_metadata()
    example_filter_by_dataset()
    example_filter_by_date_range()
    example_filter_by_s3_bucket()
    example_custom_ordering_and_limit()
    example_get_summary()
    example_analyze_metadata()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nKey Features:")
    print("- Load metadata from RDS database for zarr data stored in S3")
    print("- Filter by dataset, data level, S3 bucket, resolution level, date range")
    print("- Get summary statistics and aggregations")
    print("- Custom ordering and limiting of results")
    print("- Automatic parsing of JSON metadata into separate columns")
    
    print("\nUsage Notes:")
    print("- RDS database must be configured with aws_configure() or load_aws_configure()")
    print("- Functions return empty DataFrames if no data is found")
    print("- All date filters are inclusive")
    print("- JSON metadata is automatically expanded into separate columns")


if __name__ == "__main__":
    main()
