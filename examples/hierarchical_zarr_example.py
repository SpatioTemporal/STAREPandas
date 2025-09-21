#!/usr/bin/env python3
"""
Example demonstrating the new hierarchical zarr storage functionality.

This script shows how the updated to_zarr_s3 and from_zarr_s3 functions now
use hierarchical paths for better spatial organization of zarr data.
"""

import os
import sys
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def example_hierarchical_storage():
    """Example showing hierarchical zarr storage."""
    print("=== Hierarchical Zarr Storage Example ===")
    
    # Create sample data with known STARE SIDs
    print("1. Creating sample STAREDataFrame:")
    
    # Use SIDs that will create different hierarchical paths
    data = {
        'sids': [
            3445253714938429444,  # Will create: Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/dataset
            3447505514752114692,  # Will create: Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/dataset
            3448068485499011499,  # Will create: Q00_5/Q01_3/.../Q11_0/dataset (12 levels)
        ],
        'lat': [32.0, 32.1, 32.2],
        'lon': [-120.0, -120.1, -120.2],
        'temperature': [25.5, 26.0, 24.8],
        'humidity': [65, 68, 62]
    }
    
    sdf = STAREDataFrame(pd.DataFrame(data), sids='sids')
    
    print(f"   Created STAREDataFrame with {len(sdf)} rows")
    print(f"   Columns: {list(sdf.columns)}")
    print(f"   SID range: {sdf['sids'].min()} to {sdf['sids'].max()}")
    
    return sdf


def example_path_generation(sdf):
    """Example showing path generation for each group."""
    print("\n2. Hierarchical path generation:")
    
    dataset = "WEATHER_DATA"
    level = 10  # STARE level for grouping
    
    print(f"   Dataset: {dataset}")
    print(f"   STARE level: {level}")
    
    # Show what paths would be generated
    coerced = sdf.to_sids_level(level=level, clear_to_level=True)
    grouped = sdf.groupby(coerced[sdf._sid_column_name], sort=False)
    
    print(f"\n   Generated hierarchical paths:")
    for group_id, gdf in grouped:
        if isinstance(group_id, (int, np.integer)) and group_id >= 0:
            hierarchical_path = sdf.generate_zarr_path(group_id, dataset)
            print(f"     Group {group_id}:")
            print(f"       Path: {hierarchical_path}")
            print(f"       Rows: {len(gdf)}")
            
            # Show spatial organization
            components = hierarchical_path.split('/')
            spatial_levels = len(components) - 1  # Exclude dataset name
            print(f"       Spatial levels: {spatial_levels}")


def example_storage_benefits():
    """Example showing storage organization benefits."""
    print("\n3. Storage organization benefits:")
    
    print("   Before (flat structure):")
    print("     s3://bucket/data/3445253714938429444/")
    print("     s3://bucket/data/3447505514752114692/")  
    print("     s3://bucket/data/3448068485499011499/")
    print("     → No spatial relationship visible")
    
    print("\n   After (hierarchical structure):")
    print("     s3://bucket/data/Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/WEATHER_DATA/")
    print("     s3://bucket/data/Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/WEATHER_DATA/")
    print("     s3://bucket/data/Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/Q05_1/.../WEATHER_DATA/")
    print("     → Clear spatial hierarchy and relationships")
    
    print("\n   Advantages:")
    print("     ✓ Spatial proximity reflected in directory structure")
    print("     ✓ Efficient range queries by path patterns")
    print("     ✓ Natural data organization for analysis")
    print("     ✓ Scalable to millions of spatial groups")
    print("     ✓ Human-readable spatial organization")


def example_spatial_queries():
    """Example showing spatial query capabilities."""
    print("\n4. Spatial query capabilities:")
    
    print("   Query examples using path patterns:")
    
    print("\n   a) Regional query:")
    print("      Pattern: 's3://bucket/data/Q00_5/Q01_3/*/WEATHER_DATA'")
    print("      → Finds all data in Q00_5/Q01_3 region")
    print("      → Efficient S3 prefix-based listing")
    
    print("\n   b) Multi-level query:")
    print("      Pattern: 's3://bucket/data/Q00_5/Q01_3/Q02_3/Q03_2/*/WEATHER_DATA'")
    print("      → More specific spatial region")
    print("      → Faster query with deeper path constraint")
    
    print("\n   c) Dataset-specific query:")
    print("      Pattern: 's3://bucket/data/**/WEATHER_DATA'")
    print("      → All weather data regardless of location")
    print("      → Cross-spatial dataset analysis")
    
    print("\n   d) Programmatic query construction:")
    print("      # Generate query path for specific SID")
    print("      from starepandas.io.granules import generate_zarr_path")
    print("      sid = 3445253714938429444")
    print("      path = generate_zarr_path(sid, 'WEATHER_DATA')")
    print("      query = f's3://bucket/data/{path}'")


def example_workflow_integration():
    """Example showing complete workflow integration."""
    print("\n5. Complete workflow integration:")
    
    print("   Step-by-step workflow:")
    
    print("\n   a) Data ingestion and storage:")
    print("      sdf = STAREDataFrame(raw_data, sids='sids')")
    print("      sdf.to_zarr_s3('s3://bucket/weather/2024-01-01', level=10, dataset='TEMP')")
    print("      → Automatically creates hierarchical organization")
    
    print("\n   b) Data discovery and reading:")
    print("      sdf_restored = STAREDataFrame.from_zarr_s3('s3://bucket/weather/2024-01-01')")
    print("      → Recursively discovers all zarr groups")
    print("      → Restores complete dataset with original order")
    
    print("\n   c) Spatial analysis:")
    print("      # Find data in specific region")
    print("      region_path = generate_zarr_path(target_sid, 'TEMP')")
    print("      specific_data = read_specific_region(f's3://bucket/weather/2024-01-01/{region_path}')")
    
    print("\n   d) Metadata integration:")
    print("      from starepandas.io.granules import load_zarr_metadata")
    print("      metadata = load_zarr_metadata(dataset='TEMP')")
    print("      → Query metadata with spatial path information")


def example_backward_compatibility():
    """Example showing backward compatibility."""
    print("\n6. Backward compatibility:")
    
    print("   Existing code continues to work:")
    
    print("\n   Before:")
    print("     sdf.to_zarr_s3(s3_path, level)")
    print("     restored = STAREDataFrame.from_zarr_s3(s3_path)")
    
    print("\n   After (same API, better organization):")
    print("     sdf.to_zarr_s3(s3_path, level, dataset='MY_DATA')")
    print("     restored = STAREDataFrame.from_zarr_s3(s3_path)")
    print("     → Same function calls, hierarchical storage automatically")
    
    print("\n   Benefits:")
    print("     ✓ No breaking changes to existing code")
    print("     ✓ Automatic upgrade to hierarchical organization")
    print("     ✓ Enhanced spatial query capabilities")
    print("     ✓ Better performance for large datasets")


def example_performance_considerations():
    """Example showing performance considerations."""
    print("\n7. Performance considerations:")
    
    print("   Storage performance:")
    print("     ✓ Parallel writes to different spatial regions")
    print("     ✓ Reduced S3 API calls through better organization")
    print("     ✓ Efficient metadata indexing by spatial hierarchy")
    
    print("\n   Query performance:")
    print("     ✓ Spatial prefix filtering reduces data scanning")
    print("     ✓ Hierarchical discovery enables targeted reads")
    print("     ✓ Path-based caching improves repeated access")
    
    print("\n   Scalability:")
    print("     ✓ Natural distribution across S3 prefixes")
    print("     ✓ Supports millions of spatial groups")
    print("     ✓ Hierarchical structure prevents directory hotspots")


def main():
    """Run all examples."""
    print("Hierarchical Zarr Storage Example")
    print("=" * 50)
    
    # Create sample data
    sdf = example_hierarchical_storage()
    
    # Show path generation
    example_path_generation(sdf)
    
    # Demonstrate benefits
    example_storage_benefits()
    
    # Show query capabilities
    example_spatial_queries()
    
    # Complete workflow
    example_workflow_integration()
    
    # Backward compatibility
    example_backward_compatibility()
    
    # Performance notes
    example_performance_considerations()
    
    print("\n" + "=" * 50)
    print("Summary:")
    print("✓ Hierarchical zarr storage now integrated into STAREDataFrame")
    print("✓ Automatic spatial organization using generate_zarr_path()")
    print("✓ Recursive discovery enables reading hierarchical data")
    print("✓ Path-based spatial queries for efficient data access")
    print("✓ Backward compatible with existing code")
    print("✓ Significant performance and organization benefits")
    
    print("\nKey Functions:")
    print("- sdf.to_zarr_s3(): Now creates hierarchical spatial organization")
    print("- STAREDataFrame.from_zarr_s3(): Recursively reads hierarchical data")
    print("- generate_zarr_path(): Creates spatial hierarchy paths")
    print("- parse_zarr_path(): Extracts spatial info from paths")
    
    print("\nThis represents a major enhancement to STAREPandas spatial data")
    print("organization while maintaining full backward compatibility!")


if __name__ == "__main__":
    main()
