#!/usr/bin/env python3
"""
Example script demonstrating the generate_zarr_path function in STAREDataFrame.

This script shows how to use the generate_zarr_path method to create hierarchical
paths for storing zarr files based on STARE SID structure.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def example_basic_usage():
    """Basic usage example of generate_zarr_path."""
    print("=== Basic Usage Example ===")
    
    # Create a STAREDataFrame instance
    sdf = STAREDataFrame()
    
    # Example SIDs from real data
    example_sids = [
        3448068485499011499,  # 12-level SID
        3445253714938429444,  # 5-level SID
        3447505514752114692,  # 5-level SID
    ]
    
    # Example dataset names
    datasets = ["MOD09", "VIIRS_L2", "SSMIS_Data"]
    
    print("Generating paths for different SID and dataset combinations:")
    
    for i, (sid, dataset) in enumerate(zip(example_sids, datasets)):
        path = sdf.generate_zarr_path(sid, dataset)
        print(f"\n{i+1}. SID: {sid}")
        print(f"   Dataset: {dataset}")
        print(f"   Path: {path}")
        
        # Show path depth
        levels = len(path.split('/')) - 1
        print(f"   Levels: {levels}")


def example_hierarchical_organization():
    """Example showing hierarchical organization benefits."""
    print("\n=== Hierarchical Organization Example ===")
    
    sdf = STAREDataFrame()
    
    # Simulate multiple SIDs that might share common prefixes
    similar_sids = [
        3445253714938429444,  # Q00_5/Q01_3/Q02_3/Q03_2/Q04_2
        3447505514752114692,  # Q00_5/Q01_3/Q02_3/Q03_2/Q04_3
    ]
    
    dataset = "MOD09"
    
    print("SIDs with similar hierarchical structure:")
    for i, sid in enumerate(similar_sids):
        path = sdf.generate_zarr_path(sid, dataset)
        components = path.split('/')
        
        print(f"\n{i+1}. SID: {sid}")
        print(f"   Path: {path}")
        print(f"   Common prefix: {'/'.join(components[:-2])}")
        print(f"   Unique part: {components[-2]}")
        print(f"   Dataset: {components[-1]}")
    
    print("\nBenefits:")
    print("- Files with similar SIDs are stored in nearby directories")
    print("- Enables efficient spatial queries and data locality")
    print("- Supports hierarchical data management strategies")


def example_storage_applications():
    """Example showing storage and retrieval applications."""
    print("\n=== Storage Applications Example ===")
    
    sdf = STAREDataFrame()
    
    # Example: Organizing data by resolution levels
    sid = 3448068485499011499
    datasets = ["MOD09_L1", "MOD09_L2", "MOD09_L3"]
    
    print("Organizing different processing levels for the same spatial region:")
    for dataset in datasets:
        path = sdf.generate_zarr_path(sid, dataset)
        print(f"  {dataset}: {path}")
    
    print("\nStorage structure benefits:")
    print("- All processing levels for same region stored together")
    print("- Easy to find related datasets")
    print("- Supports efficient batch processing")
    
    # Example: Different spatial regions
    print(f"\nComparing different spatial regions:")
    different_sids = [
        3448068485499011499,  # Region 1
        3445253714938429444,  # Region 2
    ]
    
    for i, sid in enumerate(different_sids):
        path = sdf.generate_zarr_path(sid, "MOD09")
        print(f"  Region {i+1}: {path}")


def example_path_analysis():
    """Example analyzing the generated paths."""
    print("\n=== Path Analysis Example ===")
    
    sdf = STAREDataFrame()
    sid = 3448068485499011499
    dataset = "MOD09"
    
    path = sdf.generate_zarr_path(sid, dataset)
    components = path.split('/')
    
    print(f"Analyzing path: {path}")
    print(f"\nPath breakdown:")
    print(f"  Total components: {len(components)}")
    print(f"  Hierarchy levels: {len(components) - 1}")
    print(f"  Dataset name: {components[-1]}")
    
    print(f"\nLevel analysis:")
    for i, component in enumerate(components[:-1]):
        level_num = component.split('_')[0][1:]  # Remove 'Q'
        level_val = component.split('_')[1]
        print(f"  Level {level_num}: value {level_val}")
    
    # Show bit representation
    print(f"\nSID bit analysis:")
    print(f"  SID: {sid}")
    print(f"  Hex: {hex(sid)}")
    print(f"  Binary: {bin(sid)}")
    
    # Extract key information
    num_levels = (sid & 0x1F) + 1
    level_0_value = (sid >> 59) & 0x7
    print(f"  Number of levels (bits 0-4): {num_levels}")
    print(f"  Level 0 value (bits 59-61): {level_0_value}")


def example_use_cases():
    """Example showing different use cases."""
    print("\n=== Use Cases Example ===")
    
    print("1. **Cloud Storage Organization**:")
    print("   s3://my-bucket/data/{generate_zarr_path(sid, dataset)}")
    print("   Example: s3://my-bucket/data/Q00_5/Q01_3/.../MOD09")
    
    print("\n2. **Local File System**:")
    print("   /data/zarr/{generate_zarr_path(sid, dataset)}")
    print("   Example: /data/zarr/Q00_5/Q01_3/.../VIIRS_L2")
    
    print("\n3. **Database Indexing**:")
    print("   Use path components as database keys for spatial indexing")
    print("   Level 0-2 for coarse spatial queries, deeper levels for precision")
    
    print("\n4. **Parallel Processing**:")
    print("   Distribute processing based on path prefixes")
    print("   Each worker handles specific Q00_X/Q01_Y branches")
    
    print("\n5. **Data Archival**:")
    print("   Archive old data by moving entire directory trees")
    print("   Maintain spatial organization in archives")


def main():
    """Run all examples."""
    print("STARE SID Zarr Path Generation Examples")
    print("=" * 50)
    
    # Run examples
    example_basic_usage()
    example_hierarchical_organization()
    example_storage_applications()
    example_path_analysis()
    example_use_cases()
    
    print("\n" + "=" * 50)
    print("Key Features:")
    print("✓ Hierarchical path generation based on STARE SID structure")
    print("✓ Automatic level extraction from SID bit patterns")
    print("✓ Supports up to 32 levels of hierarchy")
    print("✓ Perfect for spatial data organization")
    print("✓ Enables efficient storage and retrieval")
    print("✓ Compatible with cloud storage and local filesystems")
    
    print("\nUsage:")
    print("sdf = STAREDataFrame()")
    print("path = sdf.generate_zarr_path(sid, dataset_name)")
    print("# Use path for zarr storage organization")


if __name__ == "__main__":
    main()
