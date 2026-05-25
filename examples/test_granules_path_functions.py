#!/usr/bin/env python3
"""
Test script for the partition path functions in starepandas.io.granules module.

This script demonstrates and tests the generate_partition_path() and parse_partition_path()
functions that are now located in the granules I/O module.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import generate_partition_path, parse_partition_path


def test_granules_path_functions():
    """Test the partition path functions in granules module."""
    print("=== Testing Granules Path Functions ===")
    
    # Test data
    test_cases = [
        (3445253714938429444, "MOD09"),
        (3447505514752114692, "VIIRS_L2"),
        (3448068485499011499, "SSMIS_Data"),
        (0, "SIMPLE_DATASET"),
    ]
    
    success_count = 0
    total_tests = len(test_cases)
    
    for i, (original_sid, dataset) in enumerate(test_cases):
        print(f"\nTest {i+1}: SID {original_sid}, Dataset '{dataset}'")
        
        try:
            # Test generate_partition_path from granules module
            path = generate_partition_path(original_sid, dataset)
            print(f"  Generated path: {path}")
            
            # Test parse_partition_path from granules module
            reconstructed_sid, reconstructed_dataset = parse_partition_path(path)
            print(f"  Reconstructed SID: {reconstructed_sid}")
            print(f"  Reconstructed dataset: '{reconstructed_dataset}'")
            
            # Check level structure consistency
            original_levels = (original_sid & 0x1F) + 1
            reconstructed_levels = (reconstructed_sid & 0x1F) + 1
            
            level_match = original_levels == reconstructed_levels
            dataset_match = dataset == reconstructed_dataset
            
            if level_match and dataset_match:
                print(f"  ✓ Granules functions SUCCESSFUL")
                success_count += 1
            else:
                print(f"  ✗ Granules functions FAILED")
                if not level_match:
                    print(f"    Level mismatch: {original_levels} != {reconstructed_levels}")
                if not dataset_match:
                    print(f"    Dataset mismatch: '{dataset}' != '{reconstructed_dataset}'")
                    
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
    
    print(f"\nGranules function tests: {success_count}/{total_tests} successful")
    return success_count == total_tests


def test_comparison_with_class_methods():
    """Test that granules functions produce identical results to class methods."""
    print("\n=== Comparing Granules Functions vs Class Methods ===")
    
    # Import STAREDataFrame for comparison
    from starepandas import STAREDataFrame
    sdf = STAREDataFrame()
    
    test_sids = [3445253714938429444, 3447505514752114692, 0]
    dataset = "TEST_DATASET"
    
    all_match = True
    
    for i, sid in enumerate(test_sids):
        print(f"\nTest {i+1}: SID {sid}")
        
        # Generate paths using both methods
        granules_path = generate_partition_path(sid, dataset)
        class_path = sdf.generate_partition_path(sid, dataset)
        
        print(f"  Granules function: {granules_path}")
        print(f"  Class method:      {class_path}")
        
        path_match = granules_path == class_path
        if path_match:
            print(f"  ✓ Paths MATCH")
        else:
            print(f"  ✗ Paths DIFFER")
            all_match = False
        
        # Parse paths using both methods
        granules_result = parse_partition_path(granules_path)
        class_result = sdf.parse_partition_path(class_path)
        
        print(f"  Granules parse: {granules_result}")
        print(f"  Class parse:    {class_result}")
        
        parse_match = granules_result == class_result
        if parse_match:
            print(f"  ✓ Parse results MATCH")
        else:
            print(f"  ✗ Parse results DIFFER")
            all_match = False
    
    print(f"\nComparison result: {'✓ ALL MATCH' if all_match else '✗ SOME DIFFER'}")
    return all_match


def test_import_patterns():
    """Test different import patterns for the functions."""
    print("\n=== Testing Import Patterns ===")
    
    test_sid = 3445253714938429444
    test_dataset = "IMPORT_TEST"
    
    print("1. Direct function import:")
    print("   from starepandas.io.granules import generate_partition_path, parse_partition_path")
    
    # Already imported at top of file
    path1 = generate_partition_path(test_sid, test_dataset)
    result1 = parse_partition_path(path1)
    print(f"   Result: {path1} → {result1}")
    
    print("\n2. Module import:")
    print("   from starepandas.io import granules")
    
    from starepandas.io import granules
    path2 = granules.generate_partition_path(test_sid, test_dataset)
    result2 = granules.parse_partition_path(path2)
    print(f"   Result: {path2} → {result2}")
    
    print("\n3. Full module path import:")
    print("   import starepandas.io.granules as granules_io")
    
    import starepandas.io.granules as granules_io
    path3 = granules_io.generate_partition_path(test_sid, test_dataset)
    result3 = granules_io.parse_partition_path(path3)
    print(f"   Result: {path3} → {result3}")
    
    # Check all methods produce identical results
    all_identical = (path1 == path2 == path3) and (result1 == result2 == result3)
    print(f"\nAll import patterns identical: {'✓ YES' if all_identical else '✗ NO'}")
    
    return all_identical


def test_integration_with_other_functions():
    """Test integration with other functions in the granules module."""
    print("\n=== Testing Integration with Other Granules Functions ===")
    
    print("Available functions in starepandas.io.granules:")
    
    # Import the module to check available functions
    import starepandas.io.granules as granules_io
    
    # List key functions
    key_functions = [
        'read_granule',
        'to_s3', 
        'load_s3_metadata',
        'get_s3_summary',
        'from_s3',
        'from_s3_groups',
        'generate_partition_path',
        'parse_partition_path'
    ]
    
    available_functions = []
    for func_name in key_functions:
        if hasattr(granules_io, func_name):
            available_functions.append(func_name)
            print(f"  ✓ {func_name}")
        else:
            print(f"  ✗ {func_name} (not found)")
    
    print(f"\nAvailable functions: {len(available_functions)}/{len(key_functions)}")
    
    # Test that path functions work well with other functions
    print(f"\nTesting integration workflow:")
    
    try:
        # Generate a path
        sid = 3445253714938429444
        dataset = "INTEGRATION_TEST"
        path = generate_partition_path(sid, dataset)
        print(f"  1. Generated path: {path}")
        
        # Parse it back
        parsed_sid, parsed_dataset = parse_partition_path(path)
        print(f"  2. Parsed back: SID={parsed_sid}, Dataset='{parsed_dataset}'")
        
        # Check consistency
        consistent = ((sid & 0x1F) + 1) == ((parsed_sid & 0x1F) + 1) and dataset == parsed_dataset
        print(f"  3. Integration consistent: {'✓ YES' if consistent else '✗ NO'}")
        
        return consistent and len(available_functions) >= 6
        
    except Exception as e:
        print(f"  ✗ Integration error: {e}")
        return False


def demonstrate_new_usage():
    """Demonstrate the new usage patterns."""
    print("\n=== New Usage Patterns ===")
    
    print("The functions are now part of the granules I/O module:")
    print("This makes more sense organizationally as they're I/O utilities.")
    
    print(f"\nRecommended usage patterns:")
    
    print(f"\n1. For general use:")
    print("   from starepandas.io.granules import generate_partition_path, parse_partition_path")
    print("   path = generate_partition_path(sid, dataset)")
    print("   sid, dataset = parse_partition_path(path)")
    
    print(f"\n2. For I/O workflows:")
    print("   from starepandas.io.granules import (")
    print("       read_granule, to_s3, generate_partition_path, parse_partition_path")
    print("   )")
    print("   # Use all functions together for complete workflow")
    
    print(f"\n3. For module-based approach:")
    print("   from starepandas.io import granules")
    print("   path = granules.generate_partition_path(sid, dataset)")
    print("   data = granules.read_granule(file_path)")
    print("   granules.to_s3(file_path, s3_path, level)")
    
    # Example workflow
    print(f"\nExample complete workflow:")
    
    sid = 3445253714938429444
    dataset = "WORKFLOW_EXAMPLE"
    
    # Generate storage path
    storage_path = generate_partition_path(sid, dataset)
    print(f"  1. Storage path: {storage_path}")
    
    # Simulate storage location
    s3_location = f"s3://my-bucket/parquet/{storage_path}"
    local_location = f"/data/parquet/{storage_path}"
    
    print(f"  2. S3 location: {s3_location}")
    print(f"  3. Local location: {local_location}")
    
    # Later, parse path to understand what's stored
    parsed_sid, parsed_dataset = parse_partition_path(storage_path)
    print(f"  4. Parsed info: SID={parsed_sid}, Dataset='{parsed_dataset}'")
    
    print(f"\nBenefits of new location:")
    print("  ✓ Logical organization with other I/O functions")
    print("  ✓ Clear namespace: starepandas.io.granules")
    print("  ✓ Easy to find alongside related functions")
    print("  ✓ Consistent with other I/O utilities")


def main():
    """Run all tests and demonstrations."""
    print("STARE Partition Path Functions in Granules Module")
    print("=" * 60)
    
    # Run tests
    test1_success = test_granules_path_functions()
    test2_success = test_comparison_with_class_methods()
    test3_success = test_import_patterns()
    test4_success = test_integration_with_other_functions()
    
    # Demonstrate usage
    demonstrate_new_usage()
    
    print("\n" + "=" * 60)
    print("Test Summary:")
    if test1_success:
        print("✓ Granules path functions test PASSED")
    else:
        print("✗ Granules path functions test FAILED")
    
    if test2_success:
        print("✓ Comparison with class methods PASSED")
    else:
        print("✗ Comparison with class methods FAILED")
    
    if test3_success:
        print("✓ Import patterns test PASSED")
    else:
        print("✗ Import patterns test FAILED")
    
    if test4_success:
        print("✓ Integration test PASSED")
    else:
        print("✗ Integration test FAILED")
    
    all_passed = all([test1_success, test2_success, test3_success, test4_success])
    
    print(f"\nOverall Result: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}")
    
    print("\nFunctions Now Available In:")
    print("- starepandas.io.granules.generate_partition_path(sid, dataset_name)")
    print("- starepandas.io.granules.parse_partition_path(partition_path)")
    
    print("\nThis location makes more sense as these are I/O utility functions!")


if __name__ == "__main__":
    main()
