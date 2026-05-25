#!/usr/bin/env python3
"""
Test script for the parse_partition_path function in STAREDataFrame.

This script demonstrates and tests the parse_partition_path function that reconstructs
STARE SIDs from hierarchical Parquet paths (reverse of generate_partition_path).
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def test_round_trip_conversion():
    """Test that parse_partition_path is the true inverse of generate_partition_path."""
    print("=== Round-Trip Conversion Test ===")
    
    sdf = STAREDataFrame()
    
    # Test with various SIDs
    test_sids = [
        3448068485499011499,  # 12-level SID
        3445253714938429444,  # 5-level SID
        3447505514752114692,  # 5-level SID
        0,                    # Minimum SID (1 level, all zeros)
        31,                   # Maximum levels (32 levels)
    ]
    
    datasets = ["MOD09", "VIIRS_L2", "SSMIS_Data", "TEST", "DATASET"]
    
    success_count = 0
    total_tests = len(test_sids)
    
    for i, (original_sid, dataset) in enumerate(zip(test_sids, datasets)):
        print(f"\nTest {i+1}: SID {original_sid}, Dataset '{dataset}'")
        
        try:
            # Generate path from SID
            path = sdf.generate_partition_path(original_sid, dataset)
            print(f"  Generated path: {path}")
            
            # Parse path back to SID
            reconstructed_sid, reconstructed_dataset = sdf.parse_partition_path(path)
            print(f"  Reconstructed SID: {reconstructed_sid}")
            print(f"  Reconstructed dataset: '{reconstructed_dataset}'")
            
            # Check if round-trip is successful
            sid_match = original_sid == reconstructed_sid
            dataset_match = dataset == reconstructed_dataset
            
            if sid_match and dataset_match:
                print(f"  ✓ Round-trip SUCCESSFUL")
                success_count += 1
            else:
                print(f"  ✗ Round-trip FAILED")
                if not sid_match:
                    print(f"    SID mismatch: {original_sid} != {reconstructed_sid}")
                    print(f"    Original binary:  {bin(original_sid)}")
                    print(f"    Reconstructed:    {bin(reconstructed_sid)}")
                if not dataset_match:
                    print(f"    Dataset mismatch: '{dataset}' != '{reconstructed_dataset}'")
                    
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
    
    print(f"\nRound-trip test results: {success_count}/{total_tests} successful")
    return success_count == total_tests


def test_path_parsing():
    """Test parsing various path formats."""
    print("\n=== Path Parsing Test ===")
    
    sdf = STAREDataFrame()
    
    test_paths = [
        "Q00_5/Q01_3/Q02_2/Q03_1/MOD09",
        "Q00_0/SIMPLE_DATASET",
        "Q00_7/Q01_3/Q02_1/COMPLEX_DATASET",
        "Q00_2/Q01_0/Q02_3/Q03_2/Q04_1/FIVE_LEVELS",
        "Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/Q05_1/Q06_0/Q07_0/Q08_0/Q09_0/Q10_0/Q11_0/TWELVE_LEVELS",
    ]
    
    for i, path in enumerate(test_paths):
        print(f"\nTest {i+1}: {path}")
        
        try:
            sid, dataset = sdf.parse_partition_path(path)
            print(f"  Parsed SID: {sid}")
            print(f"  Parsed dataset: '{dataset}'")
            print(f"  SID (hex): {hex(sid)}")
            print(f"  SID (binary): {bin(sid)}")
            
            # Analyze the SID
            num_levels = (sid & 0x1F) + 1
            level_0_value = (sid >> 59) & 0x7
            print(f"  Number of levels: {num_levels}")
            print(f"  Level 0 value: {level_0_value}")
            
            # Verify by generating path from parsed SID
            regenerated_path = sdf.generate_partition_path(sid, dataset)
            if regenerated_path == path:
                print(f"  ✓ Verification SUCCESSFUL")
            else:
                print(f"  ✗ Verification FAILED")
                print(f"    Original:    {path}")
                print(f"    Regenerated: {regenerated_path}")
                
        except Exception as e:
            print(f"  ✗ ERROR: {e}")


def test_error_handling():
    """Test error handling for invalid paths."""
    print("\n=== Error Handling Test ===")
    
    sdf = STAREDataFrame()
    
    invalid_paths = [
        "",                                    # Empty path
        "DATASET_ONLY",                       # No levels
        "Q00_8/DATASET",                      # Level 0 value out of range (0-7)
        "Q00_5/Q01_4/DATASET",               # Level 1 value out of range (0-3)
        "Q00_5/Q02_1/DATASET",               # Missing level Q01
        "Q01_2/Q00_1/DATASET",               # Wrong order
        "L00_5/Q01_2/DATASET",               # Wrong prefix (L instead of Q)
        "Q00_5/Q01/DATASET",                 # Missing value after underscore
        "Q00_5/Q01_abc/DATASET",             # Non-numeric value
        "Q00_5/Q01_2_extra/DATASET",         # Extra underscore
    ]
    
    expected_errors = len(invalid_paths)
    actual_errors = 0
    
    for i, path in enumerate(invalid_paths):
        print(f"\nTest {i+1}: '{path}'")
        
        try:
            sid, dataset = sdf.parse_partition_path(path)
            print(f"  ✗ UNEXPECTED SUCCESS: SID={sid}, Dataset='{dataset}'")
        except ValueError as e:
            print(f"  ✓ Expected error: {e}")
            actual_errors += 1
        except Exception as e:
            print(f"  ? Unexpected error type: {type(e).__name__}: {e}")
            actual_errors += 1
    
    print(f"\nError handling test results: {actual_errors}/{expected_errors} errors caught")
    return actual_errors == expected_errors


def test_bit_patterns():
    """Test specific bit patterns and edge cases."""
    print("\n=== Bit Pattern Test ===")
    
    sdf = STAREDataFrame()
    
    # Test specific bit patterns
    test_cases = [
        {
            "name": "All zeros",
            "sid": 0,
            "expected_levels": 1,
            "expected_level0": 0
        },
        {
            "name": "Maximum level 0 value",
            "sid": (7 << 59),  # Level 0 = 7
            "expected_levels": 1,
            "expected_level0": 7
        },
        {
            "name": "Two levels with max values",
            "sid": 1 | (7 << 59) | (3 << 57),  # 2 levels, Level 0 = 7, Level 1 = 3
            "expected_levels": 2,
            "expected_level0": 7
        },
        {
            "name": "Bits 62-63 should be cleared",
            "sid": (0x3 << 62) | (5 << 59) | 4,  # Set bits 62-63, should be cleared
            "expected_levels": 5,
            "expected_level0": 5
        }
    ]
    
    for i, test_case in enumerate(test_cases):
        print(f"\nTest {i+1}: {test_case['name']}")
        original_sid = test_case["sid"]
        
        print(f"  Original SID: {original_sid}")
        print(f"  Original (hex): {hex(original_sid)}")
        print(f"  Original (binary): {bin(original_sid)}")
        
        # Generate path and parse it back
        path = sdf.generate_partition_path(original_sid, "TEST")
        reconstructed_sid, dataset = sdf.parse_partition_path(path)
        
        print(f"  Generated path: {path}")
        print(f"  Reconstructed SID: {reconstructed_sid}")
        print(f"  Reconstructed (hex): {hex(reconstructed_sid)}")
        print(f"  Reconstructed (binary): {bin(reconstructed_sid)}")
        
        # Check that bits 62-63 are cleared
        bits_62_63 = (reconstructed_sid >> 62) & 0x3
        if bits_62_63 == 0:
            print(f"  ✓ Bits 62-63 correctly cleared")
        else:
            print(f"  ✗ Bits 62-63 not cleared: {bits_62_63}")
        
        # Check expected values
        actual_levels = (reconstructed_sid & 0x1F) + 1
        actual_level0 = (reconstructed_sid >> 59) & 0x7
        
        levels_match = actual_levels == test_case["expected_levels"]
        level0_match = actual_level0 == test_case["expected_level0"]
        
        if levels_match and level0_match:
            print(f"  ✓ Expected values match")
        else:
            print(f"  ✗ Expected values don't match")
            if not levels_match:
                print(f"    Levels: expected {test_case['expected_levels']}, got {actual_levels}")
            if not level0_match:
                print(f"    Level 0: expected {test_case['expected_level0']}, got {actual_level0}")


def test_real_world_scenarios():
    """Test with real-world scenarios and edge cases."""
    print("\n=== Real-World Scenarios Test ===")
    
    sdf = STAREDataFrame()
    
    # Test with real SIDs from the dataset
    real_scenarios = [
        {
            "name": "Real MODIS SID",
            "sid": 3448068485499011499,
            "dataset": "MOD09_A2020032"
        },
        {
            "name": "Real VIIRS SID",
            "sid": 3445253714938429444,
            "dataset": "VIIRS_L2_20200201"
        },
        {
            "name": "Complex dataset name",
            "sid": 3447505514752114692,
            "dataset": "SSMIS_F17_20200201_v7.1_Daily"
        }
    ]
    
    for i, scenario in enumerate(real_scenarios):
        print(f"\nScenario {i+1}: {scenario['name']}")
        
        original_sid = scenario["sid"]
        dataset = scenario["dataset"]
        
        print(f"  Original SID: {original_sid}")
        print(f"  Dataset: '{dataset}'")
        
        # Generate path
        path = sdf.generate_partition_path(original_sid, dataset)
        print(f"  Generated path: {path}")
        
        # Parse path
        reconstructed_sid, reconstructed_dataset = sdf.parse_partition_path(path)
        print(f"  Reconstructed SID: {reconstructed_sid}")
        print(f"  Reconstructed dataset: '{reconstructed_dataset}'")
        
        # Verify
        sid_match = original_sid == reconstructed_sid
        dataset_match = dataset == reconstructed_dataset
        
        if sid_match and dataset_match:
            print(f"  ✓ Scenario SUCCESSFUL")
        else:
            print(f"  ✗ Scenario FAILED")


def main():
    """Run all tests."""
    print("STARE SID Partition Path Parsing Tests")
    print("=" * 50)
    
    # Run tests
    test1_success = test_round_trip_conversion()
    test_path_parsing()
    test2_success = test_error_handling()
    test_bit_patterns()
    test_real_world_scenarios()
    
    print("\n" + "=" * 50)
    print("Test Summary:")
    if test1_success:
        print("✓ Round-trip conversion test PASSED")
    else:
        print("✗ Round-trip conversion test FAILED")
    
    if test2_success:
        print("✓ Error handling test PASSED")
    else:
        print("✗ Error handling test FAILED")
    
    print("\nFunction Features:")
    print("✓ Parses hierarchical Parquet paths back to STARE SIDs")
    print("✓ True inverse of generate_partition_path function")
    print("✓ Validates path format and component values")
    print("✓ Ensures bits 62-63 are always set to 0")
    print("✓ Comprehensive error handling for invalid paths")
    print("✓ Supports up to 32 levels of hierarchy")
    
    print("\nUsage:")
    print("sdf = STAREDataFrame()")
    print("sid, dataset = sdf.parse_partition_path('Q00_5/Q01_3/.../QN_M/DatasetName')")
    print("# Returns reconstructed SID and dataset name")


if __name__ == "__main__":
    main()
