#!/usr/bin/env python3
"""
Example script demonstrating the convenience functions for partition path operations.

This script shows how to use the module-level convenience functions
starepandas.generate_partition_path() and starepandas.parse_partition_path()
for easy access without creating STAREDataFrame instances.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import starepandas as sp


def example_basic_usage():
    """Basic usage of convenience functions."""
    print("=== Basic Usage Example ===")
    
    # Example STARE SIDs and datasets
    examples = [
        (3445253714938429444, "MOD09_Aqua"),
        (3447505514752114692, "VIIRS_L2_NPP"),
        (3448068485499011499, "SSMIS_F17"),
        (0, "Simple_Dataset"),
    ]
    
    print("Using convenience functions directly from starepandas module:")
    
    for i, (sid, dataset) in enumerate(examples):
        print(f"\n{i+1}. SID: {sid}")
        print(f"   Dataset: '{dataset}'")
        
        # Generate path using convenience function
        path = sp.generate_partition_path(sid, dataset)
        print(f"   Generated path: {path}")
        
        # Parse path back using convenience function
        reconstructed_sid, reconstructed_dataset = sp.parse_partition_path(path)
        print(f"   Parsed SID: {reconstructed_sid}")
        print(f"   Parsed dataset: '{reconstructed_dataset}'")
        
        # Check consistency
        level_consistent = ((sid & 0x1F) + 1) == ((reconstructed_sid & 0x1F) + 1)
        dataset_consistent = dataset == reconstructed_dataset
        
        if level_consistent and dataset_consistent:
            print(f"   ✓ Round-trip successful")
        else:
            print(f"   ✗ Round-trip failed")


def example_storage_workflow():
    """Example showing a complete storage workflow using convenience functions."""
    print("\n=== Storage Workflow Example ===")
    
    # Simulate processing multiple granules
    granules = [
        {"sid": 3445253714938429444, "dataset": "MOD09", "processing_level": "L2"},
        {"sid": 3447505514752114692, "dataset": "VIIRS", "processing_level": "L2"},
        {"sid": 3448068485499011499, "dataset": "SSMIS", "processing_level": "L1B"},
    ]
    
    print("1. Processing granules and generating storage paths:")
    
    storage_manifest = []
    for i, granule in enumerate(granules):
        sid = granule["sid"]
        dataset_name = f"{granule['dataset']}_{granule['processing_level']}"
        
        # Generate storage path
        path = sp.generate_partition_path(sid, dataset_name)
        
        storage_info = {
            "granule_id": i + 1,
            "sid": sid,
            "dataset": dataset_name,
            "partition_path": path,
            "full_s3_path": f"s3://my-parquet-bucket/{path}",
            "local_path": f"/data/parquet/{path}"
        }
        
        storage_manifest.append(storage_info)
        
        print(f"   Granule {i+1}:")
        print(f"     SID: {sid}")
        print(f"     Dataset: {dataset_name}")
        print(f"     Partition path: {path}")
        print(f"     S3 location: {storage_info['full_s3_path']}")
    
    print(f"\n2. Storage manifest created with {len(storage_manifest)} entries")
    
    print(f"\n3. Later: Reading back from storage paths:")
    
    for storage_info in storage_manifest:
        path = storage_info["partition_path"]
        
        # Parse path to extract information
        parsed_sid, parsed_dataset = sp.parse_partition_path(path)
        
        print(f"   Path: {path}")
        print(f"     Original SID: {storage_info['sid']}")
        print(f"     Parsed SID: {parsed_sid}")
        print(f"     Original dataset: {storage_info['dataset']}")
        print(f"     Parsed dataset: {parsed_dataset}")
        
        # Verify consistency
        level_match = ((storage_info['sid'] & 0x1F) + 1) == ((parsed_sid & 0x1F) + 1)
        dataset_match = storage_info['dataset'] == parsed_dataset
        
        status = "✓ Consistent" if (level_match and dataset_match) else "✗ Inconsistent"
        print(f"     Status: {status}")


def example_spatial_organization():
    """Example showing spatial organization benefits."""
    print("\n=== Spatial Organization Example ===")
    
    # Generate paths for spatially related data
    spatial_data = [
        (3445253714938429444, "MOD09"),  # Same spatial region, different datasets
        (3445253714938429444, "MOD11"),
        (3445253714938429444, "MOD13"),
        (3447505514752114692, "MOD09"),  # Different spatial region
    ]
    
    print("Generating paths for spatially organized data:")
    
    paths = []
    for sid, dataset in spatial_data:
        path = sp.generate_partition_path(sid, dataset)
        paths.append(path)
        print(f"  SID {sid}, Dataset {dataset}:")
        print(f"    → {path}")
    
    print(f"\nSpatial relationship analysis:")
    
    # Group by common prefixes
    prefix_groups = {}
    for path in paths:
        components = path.split('/')[:-1]  # Remove dataset name
        if len(components) >= 4:  # At least 4 levels for meaningful grouping
            prefix = '/'.join(components[:4])
            if prefix not in prefix_groups:
                prefix_groups[prefix] = []
            prefix_groups[prefix].append(path)
    
    for prefix, group_paths in prefix_groups.items():
        print(f"  Spatial region '{prefix}':")
        for path in group_paths:
            dataset = path.split('/')[-1]
            print(f"    - {dataset}")
        print(f"    → {len(group_paths)} datasets in this region")


def example_error_handling():
    """Example showing error handling."""
    print("\n=== Error Handling Example ===")
    
    print("Demonstrating robust error handling:")
    
    # Test invalid paths
    invalid_paths = [
        ("", "Empty path"),
        ("DATASET_ONLY", "Missing levels"),
        ("Q00_8/DATASET", "Level 0 value out of range"),
        ("Q00_5/Q01_4/DATASET", "Level 1 value out of range"),
        ("Q00_5/Q02_1/DATASET", "Missing level Q01"),
        ("L00_5/Q01_2/DATASET", "Wrong prefix"),
    ]
    
    for path, description in invalid_paths:
        print(f"\n  Testing: '{path}' ({description})")
        try:
            result = sp.parse_partition_path(path)
            print(f"    ✗ Unexpected success: {result}")
        except ValueError as e:
            print(f"    ✓ Properly caught error: {e}")
        except Exception as e:
            print(f"    ? Unexpected error type: {type(e).__name__}: {e}")


def example_api_comparison():
    """Example comparing convenience functions with class methods."""
    print("\n=== API Comparison Example ===")
    
    sid = 3445253714938429444
    dataset = "COMPARISON_TEST"
    
    print("Comparing convenience functions vs class methods:")
    
    # Method 1: Using convenience functions (recommended)
    print("\n1. Using convenience functions (recommended):")
    print("   import starepandas as sp")
    print(f"   path = sp.generate_partition_path({sid}, '{dataset}')")
    
    path1 = sp.generate_partition_path(sid, dataset)
    print(f"   # Result: {path1}")
    
    print(f"   sid, dataset = sp.parse_partition_path('{path1}')")
    result1 = sp.parse_partition_path(path1)
    print(f"   # Result: {result1}")
    
    # Method 2: Using class methods
    print("\n2. Using class methods (more verbose):")
    print("   from starepandas import STAREDataFrame")
    print("   sdf = STAREDataFrame()")
    print(f"   path = sdf.generate_partition_path({sid}, '{dataset}')")
    
    sdf = sp.STAREDataFrame()
    path2 = sdf.generate_partition_path(sid, dataset)
    print(f"   # Result: {path2}")
    
    print(f"   sid, dataset = sdf.parse_partition_path('{path2}')")
    result2 = sdf.parse_partition_path(path2)
    print(f"   # Result: {result2}")
    
    # Compare results
    print(f"\n3. Results comparison:")
    print(f"   Paths identical: {path1 == path2}")
    print(f"   Parse results identical: {result1 == result2}")
    
    print(f"\n4. Recommendation:")
    print("   Use convenience functions for cleaner, more intuitive code!")


def main():
    """Run all examples."""
    print("STARE Partition Path Convenience Functions Examples")
    print("=" * 60)
    
    # Run examples
    example_basic_usage()
    example_storage_workflow()
    example_spatial_organization()
    example_error_handling()
    example_api_comparison()
    
    print("\n" + "=" * 60)
    print("Summary:")
    print("✓ Convenience functions provide easy module-level access")
    print("✓ No need to create STAREDataFrame instances")
    print("✓ Same functionality as class methods")
    print("✓ Cleaner, more intuitive API")
    print("✓ Perfect for storage organization and spatial analysis")
    
    print("\nAvailable Functions:")
    print("- starepandas.generate_partition_path(sid, dataset_name)")
    print("- starepandas.parse_partition_path(partition_path)")
    
    print("\nTypical Usage Pattern:")
    print("import starepandas as sp")
    print("path = sp.generate_partition_path(your_sid, 'your_dataset')")
    print("sid, dataset = sp.parse_partition_path(path)")


if __name__ == "__main__":
    main()
