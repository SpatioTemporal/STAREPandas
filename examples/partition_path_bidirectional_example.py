#!/usr/bin/env python3
"""
Example script demonstrating bidirectional partition path operations.

This script shows how to use both generate_partition_path and parse_partition_path
functions together for hierarchical Parquet storage organization.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def example_basic_bidirectional():
    """Basic example showing both functions working together."""
    print("=== Basic Bidirectional Operations ===")
    
    sdf = STAREDataFrame()
    
    # Example: Start with a STARE SID
    original_sid = 3445253714938429444
    dataset_name = "MOD09_L2"
    
    print(f"1. Original STARE SID: {original_sid}")
    print(f"   Dataset: '{dataset_name}'")
    print(f"   SID (hex): {hex(original_sid)}")
    
    # Generate hierarchical path
    path = sdf.generate_partition_path(original_sid, dataset_name)
    print(f"\n2. Generated hierarchical path:")
    print(f"   {path}")
    
    # Parse path back to SID
    reconstructed_sid, reconstructed_dataset = sdf.parse_partition_path(path)
    print(f"\n3. Parsed from path:")
    print(f"   SID: {reconstructed_sid}")
    print(f"   Dataset: '{reconstructed_dataset}'")
    print(f"   SID (hex): {hex(reconstructed_sid)}")
    
    # Show level structure consistency
    original_levels = (original_sid & 0x1F) + 1
    reconstructed_levels = (reconstructed_sid & 0x1F) + 1
    
    print(f"\n4. Level structure comparison:")
    print(f"   Original levels: {original_levels}")
    print(f"   Reconstructed levels: {reconstructed_levels}")
    print(f"   Level structure preserved: {original_levels == reconstructed_levels}")


def example_storage_workflow():
    """Example showing a complete storage workflow."""
    print("\n=== Storage Organization Workflow ===")
    
    sdf = STAREDataFrame()
    
    # Simulate multiple STARE SIDs from a dataset
    stare_data = [
        (3445253714938429444, "Region_A_Data"),
        (3447505514752114692, "Region_B_Data"), 
        (3448068485499011499, "Region_C_Data"),
    ]
    
    print("1. Organizing multiple STARE regions for storage:")
    
    storage_paths = {}
    for i, (sid, region_name) in enumerate(stare_data):
        dataset = f"MOD09_{region_name}"
        path = sdf.generate_partition_path(sid, dataset)
        storage_paths[sid] = path
        
        print(f"   {region_name} (SID {sid}):")
        print(f"     → {path}")
    
    print(f"\n2. Storage locations generated: {len(storage_paths)} paths")
    
    # Simulate reading back from storage paths
    print(f"\n3. Reading back from storage paths:")
    
    for original_sid, path in storage_paths.items():
        reconstructed_sid, dataset = sdf.parse_partition_path(path)
        
        # Check if we can identify the original region
        level_match = ((original_sid & 0x1F) + 1) == ((reconstructed_sid & 0x1F) + 1)
        
        print(f"   Path: {path}")
        print(f"     Original SID: {original_sid}")
        print(f"     Reconstructed SID: {reconstructed_sid}")
        print(f"     Level structure match: {level_match}")
        print(f"     Dataset: '{dataset}'")


def example_spatial_queries():
    """Example showing how paths enable spatial queries."""
    print("\n=== Spatial Query Applications ===")
    
    sdf = STAREDataFrame()
    
    # Generate paths for different regions
    regions = [
        (3445253714938429444, "MOD09"),  # 5 levels: Q00_5/Q01_3/Q02_3/Q03_2/Q04_2
        (3447505514752114692, "MOD09"),  # 5 levels: Q00_5/Q01_3/Q02_3/Q03_2/Q04_3
        (3448068485499011499, "MOD09"),  # 12 levels: Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/...
    ]
    
    print("1. Generated storage paths:")
    paths = []
    for sid, dataset in regions:
        path = sdf.generate_partition_path(sid, dataset)
        paths.append(path)
        print(f"   SID {sid}: {path}")
    
    print(f"\n2. Spatial relationship analysis:")
    
    # Find common prefixes (spatial proximity)
    for i, path1 in enumerate(paths):
        for j, path2 in enumerate(paths[i+1:], i+1):
            components1 = path1.split('/')[:-1]  # Remove dataset name
            components2 = path2.split('/')[:-1]  # Remove dataset name
            
            # Find common prefix length
            common_length = 0
            for c1, c2 in zip(components1, components2):
                if c1 == c2:
                    common_length += 1
                else:
                    break
            
            if common_length > 0:
                common_prefix = '/'.join(components1[:common_length])
                print(f"   Regions {i+1} & {j+1} share {common_length} levels: {common_prefix}")
    
    print(f"\n3. Query pattern examples:")
    print(f"   Query: 'Q00_5/Q01_3/Q02_3/Q03_2/*/MOD09'")
    print(f"   → Finds all MOD09 data in Q00_5/Q01_3/Q02_3/Q03_2 region")
    print(f"   Query: 'Q00_5/*/MOD09'")
    print(f"   → Finds all MOD09 data in Q00_5 region (broader area)")


def example_path_validation():
    """Example showing path validation and error handling."""
    print("\n=== Path Validation and Error Handling ===")
    
    sdf = STAREDataFrame()
    
    # Test valid paths
    valid_paths = [
        "Q00_5/Q01_3/MOD09",
        "Q00_0/SIMPLE_DATASET",
        "Q00_7/Q01_3/Q02_1/Q03_2/COMPLEX_DATA",
    ]
    
    print("1. Valid path parsing:")
    for path in valid_paths:
        try:
            sid, dataset = sdf.parse_partition_path(path)
            print(f"   ✓ '{path}' → SID: {sid}, Dataset: '{dataset}'")
        except Exception as e:
            print(f"   ✗ '{path}' → Error: {e}")
    
    # Test invalid paths
    invalid_paths = [
        "Q00_8/DATASET",          # Level 0 value out of range
        "Q00_5/Q01_4/DATASET",    # Level 1 value out of range
        "Q00_5/Q02_1/DATASET",    # Missing level Q01
        "DATASET_ONLY",           # No levels
    ]
    
    print(f"\n2. Invalid path handling:")
    for path in invalid_paths:
        try:
            sid, dataset = sdf.parse_partition_path(path)
            print(f"   ✗ '{path}' → Unexpected success: SID: {sid}")
        except ValueError as e:
            print(f"   ✓ '{path}' → Expected error: {e}")


def example_level_analysis():
    """Example analyzing level structures from paths."""
    print("\n=== Level Structure Analysis ===")
    
    sdf = STAREDataFrame()
    
    example_paths = [
        "Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09_5_LEVELS",
        "Q00_7/Q01_1/MOD09_2_LEVELS", 
        "Q00_0/MOD09_1_LEVEL",
        "Q00_3/Q01_2/Q02_1/Q03_0/Q04_3/Q05_1/Q06_2/MOD09_7_LEVELS",
    ]
    
    print("Analyzing level structures from paths:")
    
    for path in example_paths:
        sid, dataset = sdf.parse_partition_path(path)
        
        # Extract level information
        num_levels = (sid & 0x1F) + 1
        components = path.split('/')[:-1]  # Remove dataset name
        
        print(f"\n  Path: {path}")
        print(f"    Levels in path: {len(components)}")
        print(f"    Levels in SID: {num_levels}")
        print(f"    Dataset: '{dataset}'")
        
        # Show level values
        level_values = []
        for component in components:
            level_value = int(component.split('_')[1])
            level_values.append(level_value)
        
        print(f"    Level values: {level_values}")
        
        # Verify by regenerating path
        regenerated_path = sdf.generate_partition_path(sid, dataset)
        print(f"    Regenerated: {regenerated_path}")
        print(f"    Consistent: {path == regenerated_path}")


def main():
    """Run all examples."""
    print("STARE SID Bidirectional Partition Path Operations")
    print("=" * 60)
    
    # Run examples
    example_basic_bidirectional()
    example_storage_workflow()
    example_spatial_queries()
    example_path_validation()
    example_level_analysis()
    
    print("\n" + "=" * 60)
    print("Summary of Bidirectional Operations:")
    print("✓ generate_partition_path: SID → Hierarchical Path")
    print("✓ parse_partition_path: Hierarchical Path → SID + Dataset")
    print("✓ Level structure preservation in both directions")
    print("✓ Spatial organization and query capabilities")
    print("✓ Comprehensive validation and error handling")
    print("✓ Perfect for Parquet storage organization")
    
    print("\nKey Use Cases:")
    print("1. **Storage Organization**: Create hierarchical Parquet storage")
    print("2. **Spatial Queries**: Query data by spatial regions")
    print("3. **Data Management**: Organize and locate spatial datasets")
    print("4. **Path Validation**: Ensure storage path integrity")
    print("5. **Level Analysis**: Understand spatial hierarchy")


if __name__ == "__main__":
    main()
