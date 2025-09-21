#!/usr/bin/env python3
"""
Example script demonstrating the zarr path functions in starepandas.io.granules.

This script shows how to use generate_zarr_path() and parse_zarr_path()
from their new location in the granules I/O module.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import generate_zarr_path, parse_zarr_path


def example_basic_usage():
    """Basic usage of the path functions from granules module."""
    print("=== Basic Usage Example ===")
    
    # Example STARE SIDs and datasets
    examples = [
        (3445253714938429444, "MOD09_Aqua"),
        (3447505514752114692, "VIIRS_L2_NPP"),
        (3448068485499011499, "SSMIS_F17"),
        (0, "Simple_Dataset"),
    ]
    
    print("Using zarr path functions from starepandas.io.granules:")
    
    for i, (sid, dataset) in enumerate(examples):
        print(f"\n{i+1}. SID: {sid}")
        print(f"   Dataset: '{dataset}'")
        
        # Generate path using granules function
        path = generate_zarr_path(sid, dataset)
        print(f"   Generated path: {path}")
        
        # Parse path back using granules function
        reconstructed_sid, reconstructed_dataset = parse_zarr_path(path)
        print(f"   Parsed SID: {reconstructed_sid}")
        print(f"   Parsed dataset: '{reconstructed_dataset}'")
        
        # Check consistency
        level_consistent = ((sid & 0x1F) + 1) == ((reconstructed_sid & 0x1F) + 1)
        dataset_consistent = dataset == reconstructed_dataset
        
        if level_consistent and dataset_consistent:
            print(f"   ✓ Round-trip successful")
        else:
            print(f"   ✗ Round-trip failed")


def example_integrated_workflow():
    """Example showing integration with other granules functions."""
    print("\n=== Integrated I/O Workflow Example ===")
    
    # Import other granules functions for demonstration
    from starepandas.io.granules import (
        to_zarr_s3, 
        load_zarr_metadata,
        from_zarr_s3_chunked_groups
    )
    
    print("Complete zarr I/O workflow using granules module functions:")
    
    # Step 1: Generate storage path
    sid = 3445253714938429444
    dataset = "MOD09_L2_Workflow"
    
    storage_path = generate_zarr_path(sid, dataset)
    s3_path = f"s3://my-zarr-bucket/{storage_path}"
    
    print(f"\n1. Storage Path Generation:")
    print(f"   SID: {sid}")
    print(f"   Dataset: {dataset}")
    print(f"   Storage path: {storage_path}")
    print(f"   Full S3 path: {s3_path}")
    
    # Step 2: Simulate data processing and storage
    print(f"\n2. Data Processing (simulated):")
    print(f"   # Read granule data")
    print(f"   # data = read_granule(granule_file)")
    print(f"   # Store to zarr")
    print(f"   # to_zarr_s3(granule_file, '{s3_path}', level=10)")
    
    # Step 3: Later, parse path to understand stored data
    print(f"\n3. Path Analysis:")
    parsed_sid, parsed_dataset = parse_zarr_path(storage_path)
    
    print(f"   Parsed from path '{storage_path}':")
    print(f"   - SID: {parsed_sid}")
    print(f"   - Dataset: {parsed_dataset}")
    print(f"   - Level structure preserved: {((sid & 0x1F) + 1) == ((parsed_sid & 0x1F) + 1)}")
    
    # Step 4: Use parsed information for data retrieval
    print(f"\n4. Data Retrieval (simulated):")
    print(f"   # Use parsed SID for spatial queries")
    print(f"   # group_sids = [parsed_sid]")
    print(f"   # data = from_zarr_s3_chunked_groups(s3_path, group_sids)")
    
    print(f"\n5. Workflow Benefits:")
    print(f"   ✓ Consistent path generation and parsing")
    print(f"   ✓ Integration with other I/O functions")
    print(f"   ✓ Hierarchical organization enables efficient queries")
    print(f"   ✓ All functions available in single module")


def example_import_patterns():
    """Example showing different import patterns."""
    print("\n=== Import Patterns Example ===")
    
    print("Different ways to import and use the functions:")
    
    # Pattern 1: Direct function import
    print(f"\n1. Direct function import (recommended for frequent use):")
    print("   from starepandas.io.granules import generate_zarr_path, parse_zarr_path")
    print("   path = generate_zarr_path(sid, dataset)")
    print("   sid, dataset = parse_zarr_path(path)")
    
    # Pattern 2: Module import
    print(f"\n2. Module import (good for organized workflows):")
    print("   from starepandas.io import granules")
    print("   path = granules.generate_zarr_path(sid, dataset)")
    print("   sid, dataset = granules.parse_zarr_path(path)")
    
    # Pattern 3: Full module path
    print(f"\n3. Full module path (explicit and clear):")
    print("   import starepandas.io.granules as granules_io")
    print("   path = granules_io.generate_zarr_path(sid, dataset)")
    print("   sid, dataset = granules_io.parse_zarr_path(path)")
    
    # Pattern 4: Combined with other functions
    print(f"\n4. Combined with other I/O functions:")
    print("   from starepandas.io.granules import (")
    print("       read_granule, to_zarr_s3, generate_zarr_path, parse_zarr_path,")
    print("       load_zarr_metadata, from_zarr_s3_chunked_groups")
    print("   )")
    print("   # Complete I/O workflow with all functions available")
    
    # Demonstrate actual usage
    print(f"\nDemonstrating Pattern 2 (module import):")
    from starepandas.io import granules
    
    test_sid = 3447505514752114692
    test_dataset = "PATTERN_DEMO"
    
    path = granules.generate_zarr_path(test_sid, test_dataset)
    parsed_sid, parsed_dataset = granules.parse_zarr_path(path)
    
    print(f"   Input: SID={test_sid}, Dataset='{test_dataset}'")
    print(f"   Path: {path}")
    print(f"   Parsed: SID={parsed_sid}, Dataset='{parsed_dataset}'")
    print(f"   Consistent: {test_sid == parsed_sid and test_dataset == parsed_dataset}")


def example_spatial_organization():
    """Example showing spatial organization benefits."""
    print("\n=== Spatial Organization Example ===")
    
    # Generate paths for related datasets
    base_sid = 3445253714938429444
    datasets = [
        "MOD09_Surface_Reflectance",
        "MOD11_Land_Surface_Temperature", 
        "MOD13_Vegetation_Indices",
        "MOD15_Leaf_Area_Index"
    ]
    
    print("Generating paths for related MODIS products:")
    
    paths = []
    for dataset in datasets:
        path = generate_zarr_path(base_sid, dataset)
        paths.append(path)
        print(f"  {dataset}:")
        print(f"    → {path}")
    
    # Analyze spatial relationships
    print(f"\nSpatial organization analysis:")
    
    # Extract common prefix (spatial location)
    common_components = paths[0].split('/')[:-1]  # Remove dataset name
    spatial_prefix = '/'.join(common_components)
    
    print(f"  Common spatial location: {spatial_prefix}")
    print(f"  All datasets share the same spatial hierarchy")
    print(f"  Storage benefits:")
    print(f"    - Related data stored together")
    print(f"    - Efficient spatial queries")
    print(f"    - Easy batch processing by region")
    
    # Show how to parse any path to get spatial info
    print(f"\nExtracting spatial info from paths:")
    for i, path in enumerate(paths[:2]):  # Show first 2
        sid, dataset = parse_zarr_path(path)
        levels = (sid & 0x1F) + 1
        print(f"  {dataset}:")
        print(f"    - Parsed SID: {sid}")
        print(f"    - Hierarchy levels: {levels}")
        print(f"    - Spatial region: {'/'.join(path.split('/')[:-1])}")


def example_error_handling():
    """Example showing error handling."""
    print("\n=== Error Handling Example ===")
    
    print("The functions include robust error handling:")
    
    # Test invalid paths
    invalid_paths = [
        ("", "Empty path"),
        ("DATASET_ONLY", "Missing hierarchy"),
        ("Q00_8/DATASET", "Level 0 out of range"),
        ("Q00_5/Q01_4/DATASET", "Level 1 out of range"),
        ("Q00_5/Q02_1/DATASET", "Missing level"),
    ]
    
    for path, description in invalid_paths:
        print(f"\n  Testing: '{path}' ({description})")
        try:
            result = parse_zarr_path(path)
            print(f"    ✗ Unexpected success: {result}")
        except ValueError as e:
            print(f"    ✓ Properly handled: {e}")
        except Exception as e:
            print(f"    ? Unexpected error: {type(e).__name__}: {e}")
    
    print(f"\nError handling benefits:")
    print(f"  ✓ Clear, descriptive error messages")
    print(f"  ✓ Input validation prevents invalid operations")
    print(f"  ✓ Consistent error types for easy handling")
    print(f"  ✓ Helps debug path generation issues")


def main():
    """Run all examples."""
    print("STARE Zarr Path Functions in Granules Module")
    print("=" * 60)
    
    # Run examples
    example_basic_usage()
    example_integrated_workflow()
    example_import_patterns()
    example_spatial_organization()
    example_error_handling()
    
    print("\n" + "=" * 60)
    print("Summary:")
    print("✓ Functions moved to logical location in granules I/O module")
    print("✓ Perfect integration with other I/O functions")
    print("✓ Multiple import patterns supported")
    print("✓ Consistent API with comprehensive error handling")
    print("✓ Enables complete zarr I/O workflows")
    
    print("\nNew Function Locations:")
    print("- starepandas.io.granules.generate_zarr_path(sid, dataset_name)")
    print("- starepandas.io.granules.parse_zarr_path(zarr_path)")
    
    print("\nRecommended Usage:")
    print("from starepandas.io.granules import generate_zarr_path, parse_zarr_path")
    print("# or")
    print("from starepandas.io import granules")
    print("path = granules.generate_zarr_path(sid, dataset)")


if __name__ == "__main__":
    main()
