#!/usr/bin/env python3
"""
Test script for the generate_partition_path function in STAREDataFrame.

This script demonstrates and tests the generate_partition_path function that creates
relative paths for storing Parquet files based on STARE SID structure.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def test_sid_bit_extraction():
    """Test the bit extraction logic for STARE SIDs."""
    print("=== Testing SID Bit Extraction ===")
    
    # Create a test SID with known bit patterns
    # We'll construct a SID with specific values for testing
    
    # Test SID construction:
    # - Bits 0-4 (num_levels): 11 (binary: 01011) -> 12 levels (0-11)
    # - Level 0 (bits 59-61): 5 (binary: 101)
    # - Level 1 (bits 57-58): 2 (binary: 10)
    # - Level 2 (bits 55-56): 1 (binary: 01)
    # - Level 3 (bits 53-54): 3 (binary: 11)
    # - Other levels: 0
    
    # Build the SID bit by bit
    sid = 0
    
    # Set number of levels to 11 (bits 0-4)
    sid |= 11  # 01011 in binary
    
    # Set level 0 to 5 (bits 59-61)
    sid |= (5 << 59)  # 101 in binary
    
    # Set level 1 to 2 (bits 57-58)
    sid |= (2 << 57)  # 10 in binary
    
    # Set level 2 to 1 (bits 55-56)
    sid |= (1 << 55)  # 01 in binary
    
    # Set level 3 to 3 (bits 53-54)
    sid |= (3 << 53)  # 11 in binary
    
    print(f"Test SID: {sid}")
    print(f"Test SID (binary): {bin(sid)}")
    print(f"Test SID (hex): {hex(sid)}")
    
    # Test the function
    sdf = STAREDataFrame()
    path = sdf.generate_partition_path(sid, "TEST_DATASET")
    
    print(f"\nGenerated path: {path}")
    
    # Expected path: Q00_5/Q01_2/Q02_1/Q03_3/Q04_0/Q05_0/.../Q11_0/TEST_DATASET
    expected_components = [
        "Q00_5",  # Level 0: 5
        "Q01_2",  # Level 1: 2
        "Q02_1",  # Level 2: 1
        "Q03_3",  # Level 3: 3
        "Q04_0",  # Level 4: 0 (not set)
        "Q05_0",  # Level 5: 0 (not set)
        "Q06_0",  # Level 6: 0 (not set)
        "Q07_0",  # Level 7: 0 (not set)
        "Q08_0",  # Level 8: 0 (not set)
        "Q09_0",  # Level 9: 0 (not set)
        "Q10_0",  # Level 10: 0 (not set)
        "Q11_0",  # Level 11: 0 (not set)
        "TEST_DATASET"
    ]
    expected_path = "/".join(expected_components)
    
    print(f"Expected path: {expected_path}")
    print(f"Match: {path == expected_path}")
    
    return path == expected_path


def test_real_sids():
    """Test with real STARE SIDs from the dataset."""
    print("\n=== Testing with Real STARE SIDs ===")
    
    # Test with some real SIDs from the dataset
    real_sids = [
        3448068485499011499,
        3448068600672222987,
        3445253714938429444,
        3447505514752114692
    ]
    
    sdf = STAREDataFrame()
    
    for i, sid in enumerate(real_sids):
        print(f"\nSID {i+1}: {sid}")
        print(f"  Hex: {hex(sid)}")
        print(f"  Binary: {bin(sid)}")
        
        # Extract key information manually for verification
        num_levels = (sid & 0x1F) + 1
        level_0_value = (sid >> 59) & 0x7
        level_1_value = (sid >> 57) & 0x3 if num_levels > 1 else None
        
        print(f"  Number of levels: {num_levels}")
        print(f"  Level 0 value: {level_0_value}")
        if level_1_value is not None:
            print(f"  Level 1 value: {level_1_value}")
        
        # Generate path
        path = sdf.generate_partition_path(sid, "MOD09")
        print(f"  Generated path: {path}")
        
        # Show path components
        components = path.split('/')
        print(f"  Path components: {len(components)} total")
        print(f"    Levels: {components[:-1]}")
        print(f"    Dataset: {components[-1]}")


def test_edge_cases():
    """Test edge cases and boundary conditions."""
    print("\n=== Testing Edge Cases ===")
    
    sdf = STAREDataFrame()
    
    # Test case 1: Minimum SID (0 levels, all values 0)
    sid_min = 0  # All bits 0
    path_min = sdf.generate_partition_path(sid_min, "DATASET")
    print(f"Minimum SID (0): {path_min}")
    
    # Test case 2: Maximum levels (31 levels)
    sid_max_levels = 31  # Bits 0-4 all set to 1
    path_max_levels = sdf.generate_partition_path(sid_max_levels, "DATASET")
    print(f"Maximum levels SID: {path_max_levels}")
    print(f"  Number of components: {len(path_max_levels.split('/'))}")
    
    # Test case 3: Maximum values at each level
    sid_max_values = 0
    sid_max_values |= 11  # 12 levels (reasonable number)
    sid_max_values |= (7 << 59)  # Level 0: max value 7
    sid_max_values |= (3 << 57)  # Level 1: max value 3
    sid_max_values |= (3 << 55)  # Level 2: max value 3
    
    path_max_values = sdf.generate_partition_path(sid_max_values, "DATASET")
    print(f"Maximum values SID: {path_max_values}")
    
    # Test case 4: Large SID value
    sid_large = 0xFFFFFFFFFFFFFFFF  # All bits set
    path_large = sdf.generate_partition_path(sid_large, "LARGE_DATASET")
    print(f"Large SID: {path_large}")


def test_dataset_names():
    """Test with different dataset names."""
    print("\n=== Testing Different Dataset Names ===")
    
    sdf = STAREDataFrame()
    test_sid = 3448068485499011499  # Use a real SID
    
    dataset_names = [
        "MOD09",
        "VIIRS_L2",
        "SSMIS_Data",
        "Dataset_With_Underscores",
        "Dataset-With-Hyphens",
        "VeryLongDatasetNameWithManyCharacters",
        "123_Numeric_Start",
        "UPPER_CASE",
        "lower_case",
        "Mixed_Case_123"
    ]
    
    for dataset in dataset_names:
        path = sdf.generate_partition_path(test_sid, dataset)
        print(f"Dataset '{dataset}': {path.split('/')[-1]}")


def analyze_path_structure():
    """Analyze the structure of generated paths."""
    print("\n=== Path Structure Analysis ===")
    
    sdf = STAREDataFrame()
    test_sids = [
        3448068485499011499,
        3445253714938429444,
        3447505514752114692
    ]
    
    for i, sid in enumerate(test_sids):
        print(f"\nSID {i+1}: {sid}")
        path = sdf.generate_partition_path(sid, "DATASET")
        components = path.split('/')
        
        print(f"  Full path: {path}")
        print(f"  Total components: {len(components)}")
        print(f"  Level components: {len(components) - 1}")
        print(f"  Path depth: {len(components) - 1} levels")
        
        # Analyze level patterns
        level_components = components[:-1]
        for j, component in enumerate(level_components):
            level_num = component.split('_')[0][1:]  # Remove 'Q' and get number
            level_val = component.split('_')[1]
            print(f"    Level {level_num}: value {level_val}")


def main():
    """Run all tests."""
    print("STARE SID Partition Path Generation Tests")
    print("=" * 50)
    
    # Run tests
    success = test_sid_bit_extraction()
    test_real_sids()
    test_edge_cases()
    test_dataset_names()
    analyze_path_structure()
    
    print("\n" + "=" * 50)
    if success:
        print("✓ Basic functionality test PASSED")
    else:
        print("✗ Basic functionality test FAILED")
    
    print("\nFunction Details:")
    print("- Extracts STARE level structure from SID bits")
    print("- Generates hierarchical path: Q00_X/Q01_Y/.../QN_Z/Dataset")
    print("- Supports up to 32 levels (limited by 5-bit level count)")
    print("- Level 0 uses 3 bits (values 0-7)")
    print("- Levels 1-27 use 2 bits each (values 0-3)")
    print("- Perfect for hierarchical Parquet storage organization")


if __name__ == "__main__":
    main()
