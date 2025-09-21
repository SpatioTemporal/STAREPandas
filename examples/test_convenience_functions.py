#!/usr/bin/env python3
"""
Test script for the convenience functions in starepandas.__init__.py

This script demonstrates and tests the convenience functions generate_zarr_path()
and parse_zarr_path() that can be called directly from the starepandas module.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import starepandas as sp


def test_convenience_functions():
    """Test the convenience functions in starepandas module."""
    print("=== Testing Convenience Functions ===")
    
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
            # Test generate_zarr_path convenience function
            path = sp.generate_zarr_path(original_sid, dataset)
            print(f"  Generated path: {path}")
            
            # Test parse_zarr_path convenience function
            reconstructed_sid, reconstructed_dataset = sp.parse_zarr_path(path)
            print(f"  Reconstructed SID: {reconstructed_sid}")
            print(f"  Reconstructed dataset: '{reconstructed_dataset}'")
            
            # Check level structure consistency
            original_levels = (original_sid & 0x1F) + 1
            reconstructed_levels = (reconstructed_sid & 0x1F) + 1
            
            level_match = original_levels == reconstructed_levels
            dataset_match = dataset == reconstructed_dataset
            
            if level_match and dataset_match:
                print(f"  ✓ Convenience functions SUCCESSFUL")
                success_count += 1
            else:
                print(f"  ✗ Convenience functions FAILED")
                if not level_match:
                    print(f"    Level mismatch: {original_levels} != {reconstructed_levels}")
                if not dataset_match:
                    print(f"    Dataset mismatch: '{dataset}' != '{reconstructed_dataset}'")
                    
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
    
    print(f"\nConvenience function tests: {success_count}/{total_tests} successful")
    return success_count == total_tests


def test_comparison_with_class_methods():
    """Test that convenience functions produce identical results to class methods."""
    print("\n=== Comparing Convenience Functions vs Class Methods ===")
    
    # Create STAREDataFrame instance for comparison
    sdf = sp.STAREDataFrame()
    
    test_sids = [3445253714938429444, 3447505514752114692, 0]
    dataset = "TEST_DATASET"
    
    all_match = True
    
    for i, sid in enumerate(test_sids):
        print(f"\nTest {i+1}: SID {sid}")
        
        # Generate paths using both methods
        convenience_path = sp.generate_zarr_path(sid, dataset)
        class_path = sdf.generate_zarr_path(sid, dataset)
        
        print(f"  Convenience function: {convenience_path}")
        print(f"  Class method:         {class_path}")
        
        path_match = convenience_path == class_path
        if path_match:
            print(f"  ✓ Paths MATCH")
        else:
            print(f"  ✗ Paths DIFFER")
            all_match = False
        
        # Parse paths using both methods
        convenience_result = sp.parse_zarr_path(convenience_path)
        class_result = sdf.parse_zarr_path(class_path)
        
        print(f"  Convenience parse: {convenience_result}")
        print(f"  Class parse:       {class_result}")
        
        parse_match = convenience_result == class_result
        if parse_match:
            print(f"  ✓ Parse results MATCH")
        else:
            print(f"  ✗ Parse results DIFFER")
            all_match = False
    
    print(f"\nComparison result: {'✓ ALL MATCH' if all_match else '✗ SOME DIFFER'}")
    return all_match


def test_error_handling():
    """Test error handling in convenience functions."""
    print("\n=== Testing Error Handling ===")
    
    # Test invalid paths for parse_zarr_path
    invalid_paths = [
        "",
        "DATASET_ONLY",
        "Q00_8/DATASET",  # Out of range
        "Q00_5/Q01_4/DATASET",  # Out of range
        "Q00_5/Q02_1/DATASET",  # Missing level
    ]
    
    expected_errors = len(invalid_paths)
    actual_errors = 0
    
    print("Testing parse_zarr_path error handling:")
    for i, path in enumerate(invalid_paths):
        print(f"  Test {i+1}: '{path}'")
        
        try:
            result = sp.parse_zarr_path(path)
            print(f"    ✗ Unexpected success: {result}")
        except ValueError as e:
            print(f"    ✓ Expected error: {e}")
            actual_errors += 1
        except Exception as e:
            print(f"    ? Unexpected error type: {type(e).__name__}: {e}")
            actual_errors += 1
    
    print(f"\nError handling: {actual_errors}/{expected_errors} errors caught")
    return actual_errors == expected_errors


def test_usage_examples():
    """Test the usage examples from the docstrings."""
    print("\n=== Testing Docstring Examples ===")
    
    try:
        # Test generate_zarr_path example
        print("Testing generate_zarr_path example:")
        path = sp.generate_zarr_path(3445253714938429444, "MOD09")
        expected_path = "Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09"
        print(f"  Generated: {path}")
        print(f"  Expected:  {expected_path}")
        generate_match = path == expected_path
        print(f"  Result: {'✓ MATCH' if generate_match else '✗ DIFFER'}")
        
        # Test parse_zarr_path example
        print("\nTesting parse_zarr_path example:")
        sid, dataset = sp.parse_zarr_path("Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09")
        expected_sid = 3445253714938429444
        expected_dataset = "MOD09"
        print(f"  Parsed SID: {sid} (expected: {expected_sid})")
        print(f"  Parsed dataset: '{dataset}' (expected: '{expected_dataset}')")
        
        sid_match = sid == expected_sid
        dataset_match = dataset == expected_dataset
        parse_match = sid_match and dataset_match
        print(f"  Result: {'✓ MATCH' if parse_match else '✗ DIFFER'}")
        
        return generate_match and parse_match
        
    except Exception as e:
        print(f"  ✗ ERROR in docstring examples: {e}")
        return False


def demonstrate_usage():
    """Demonstrate typical usage patterns."""
    print("\n=== Usage Demonstration ===")
    
    print("1. Simple usage:")
    print("   import starepandas as sp")
    print("   path = sp.generate_zarr_path(sid, dataset)")
    print("   sid, dataset = sp.parse_zarr_path(path)")
    
    # Example workflow
    original_sid = 3445253714938429444
    dataset = "MODIS_Aqua_L2"
    
    print(f"\n2. Example workflow:")
    print(f"   Original SID: {original_sid}")
    print(f"   Dataset: '{dataset}'")
    
    # Generate path
    path = sp.generate_zarr_path(original_sid, dataset)
    print(f"   Generated path: {path}")
    
    # Use path for storage
    print(f"   Storage location: /data/zarr/{path}")
    print(f"   S3 location: s3://my-bucket/zarr/{path}")
    
    # Later, parse path back
    reconstructed_sid, reconstructed_dataset = sp.parse_zarr_path(path)
    print(f"   Parsed SID: {reconstructed_sid}")
    print(f"   Parsed dataset: '{reconstructed_dataset}'")
    
    # Check consistency
    level_consistent = ((original_sid & 0x1F) + 1) == ((reconstructed_sid & 0x1F) + 1)
    print(f"   Level structure consistent: {level_consistent}")
    
    print("\n3. Benefits of convenience functions:")
    print("   - No need to create STAREDataFrame instances")
    print("   - Direct module-level access")
    print("   - Same functionality as class methods")
    print("   - Cleaner, more intuitive API")


def main():
    """Run all tests and demonstrations."""
    print("STARE Zarr Path Convenience Functions Tests")
    print("=" * 60)
    
    # Run tests
    test1_success = test_convenience_functions()
    test2_success = test_comparison_with_class_methods()
    test3_success = test_error_handling()
    test4_success = test_usage_examples()
    
    # Demonstrate usage
    demonstrate_usage()
    
    print("\n" + "=" * 60)
    print("Test Summary:")
    if test1_success:
        print("✓ Convenience functions test PASSED")
    else:
        print("✗ Convenience functions test FAILED")
    
    if test2_success:
        print("✓ Comparison with class methods PASSED")
    else:
        print("✗ Comparison with class methods FAILED")
    
    if test3_success:
        print("✓ Error handling test PASSED")
    else:
        print("✗ Error handling test FAILED")
    
    if test4_success:
        print("✓ Docstring examples test PASSED")
    else:
        print("✗ Docstring examples test FAILED")
    
    all_passed = all([test1_success, test2_success, test3_success, test4_success])
    
    print(f"\nOverall Result: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}")
    
    print("\nConvenience Functions Available:")
    print("- starepandas.generate_zarr_path(sid, dataset_name)")
    print("- starepandas.parse_zarr_path(zarr_path)")
    print("\nThese provide the same functionality as the class methods but with")
    print("a more convenient module-level interface.")


if __name__ == "__main__":
    main()
