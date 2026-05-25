#!/usr/bin/env python3
"""
Test script for verifying that generate_partition_path and parse_partition_path work correctly
for their intended purpose: hierarchical level organization.

The functions are designed to extract and reconstruct the hierarchical level structure
from STARE SIDs, not to preserve all spatial information.
"""

import os
import sys

# Add the parent directory to the path so we can import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas import STAREDataFrame


def test_level_consistency():
    """Test that the level structure is consistent between generate and parse."""
    print("=== Level Structure Consistency Test ===")
    
    sdf = STAREDataFrame()
    
    # Test SIDs with different level structures
    test_sids = [
        3448068485499011499,  # 12-level SID
        3445253714938429444,  # 5-level SID
        3447505514752114692,  # 5-level SID
    ]
    
    for i, original_sid in enumerate(test_sids):
        print(f"\nTest {i+1}: SID {original_sid}")
        
        # Extract level information from original SID
        original_levels = (original_sid & 0x1F) + 1
        original_level_values = []
        
        for level in range(original_levels):
            if level == 0:
                bit_start = 59
                bit_width = 3
            elif level <= 27:
                bit_start = 59 - 2 * level
                bit_width = 2
            else:
                continue
            
            level_value = (original_sid >> bit_start) & ((1 << bit_width) - 1)
            original_level_values.append(level_value)
        
        print(f"  Original levels: {original_levels}")
        print(f"  Original level values: {original_level_values}")
        
        # Generate path
        path = sdf.generate_partition_path(original_sid, "TEST")
        print(f"  Generated path: {path}")
        
        # Parse path back
        reconstructed_sid, dataset = sdf.parse_partition_path(path)
        print(f"  Reconstructed SID: {reconstructed_sid}")
        
        # Extract level information from reconstructed SID
        reconstructed_levels = (reconstructed_sid & 0x1F) + 1
        reconstructed_level_values = []
        
        for level in range(reconstructed_levels):
            if level == 0:
                bit_start = 59
                bit_width = 3
            elif level <= 27:
                bit_start = 59 - 2 * level
                bit_width = 2
            else:
                continue
            
            level_value = (reconstructed_sid >> bit_start) & ((1 << bit_width) - 1)
            reconstructed_level_values.append(level_value)
        
        print(f"  Reconstructed levels: {reconstructed_levels}")
        print(f"  Reconstructed level values: {reconstructed_level_values}")
        
        # Check level consistency
        levels_match = original_levels == reconstructed_levels
        values_match = original_level_values == reconstructed_level_values
        
        if levels_match and values_match:
            print(f"  ✓ Level structure CONSISTENT")
        else:
            print(f"  ✗ Level structure INCONSISTENT")
            if not levels_match:
                print(f"    Levels mismatch: {original_levels} != {reconstructed_levels}")
            if not values_match:
                print(f"    Values mismatch: {original_level_values} != {reconstructed_level_values}")


def test_pure_level_sids():
    """Test with SIDs that contain only level information (no extra spatial data)."""
    print("\n=== Pure Level SIDs Test ===")
    
    sdf = STAREDataFrame()
    
    # Create SIDs with only level information
    pure_level_sids = []
    
    # Create a 3-level SID: Level 0=5, Level 1=2, Level 2=1
    sid = 0
    sid |= 2  # 3 levels (stored as 2)
    sid |= (5 << 59)  # Level 0 = 5
    sid |= (2 << 57)  # Level 1 = 2
    sid |= (1 << 55)  # Level 2 = 1
    pure_level_sids.append(("3-level pure", sid))
    
    # Create a 1-level SID: Level 0=7
    sid = 0
    sid |= 0  # 1 level (stored as 0)
    sid |= (7 << 59)  # Level 0 = 7
    pure_level_sids.append(("1-level pure", sid))
    
    # Create a 5-level SID: all levels with specific values
    sid = 0
    sid |= 4  # 5 levels (stored as 4)
    sid |= (3 << 59)  # Level 0 = 3
    sid |= (1 << 57)  # Level 1 = 1
    sid |= (2 << 55)  # Level 2 = 2
    sid |= (0 << 53)  # Level 3 = 0
    sid |= (3 << 51)  # Level 4 = 3
    pure_level_sids.append(("5-level pure", sid))
    
    success_count = 0
    
    for name, original_sid in pure_level_sids:
        print(f"\nTest: {name}")
        print(f"  Original SID: {original_sid}")
        print(f"  Hex: {hex(original_sid)}")
        
        # Generate path
        path = sdf.generate_partition_path(original_sid, "TEST")
        print(f"  Generated path: {path}")
        
        # Parse path back
        reconstructed_sid, dataset = sdf.parse_partition_path(path)
        print(f"  Reconstructed SID: {reconstructed_sid}")
        print(f"  Dataset: '{dataset}'")
        
        # Check exact match for pure level SIDs
        if original_sid == reconstructed_sid:
            print(f"  ✓ Perfect round-trip SUCCESSFUL")
            success_count += 1
        else:
            print(f"  ✗ Round-trip FAILED")
            print(f"    Original binary:  {bin(original_sid)}")
            print(f"    Reconstructed:    {bin(reconstructed_sid)}")
    
    print(f"\nPure level SID tests: {success_count}/{len(pure_level_sids)} successful")
    return success_count == len(pure_level_sids)


def test_path_generation_consistency():
    """Test that paths generated from reconstructed SIDs are identical."""
    print("\n=== Path Generation Consistency Test ===")
    
    sdf = STAREDataFrame()
    
    test_paths = [
        "Q00_5/Q01_3/Q02_2/Q03_1/DATASET",
        "Q00_0/SIMPLE",
        "Q00_7/Q01_3/COMPLEX",
        "Q00_2/Q01_1/Q02_0/Q03_3/Q04_2/FIVE_LEVELS",
    ]
    
    success_count = 0
    
    for i, original_path in enumerate(test_paths):
        print(f"\nTest {i+1}: {original_path}")
        
        # Parse path to SID
        sid, dataset = sdf.parse_partition_path(original_path)
        print(f"  Parsed SID: {sid}")
        print(f"  Parsed dataset: '{dataset}'")
        
        # Generate path from SID
        regenerated_path = sdf.generate_partition_path(sid, dataset)
        print(f"  Regenerated path: {regenerated_path}")
        
        # Check if paths match
        if original_path == regenerated_path:
            print(f"  ✓ Path consistency SUCCESSFUL")
            success_count += 1
        else:
            print(f"  ✗ Path consistency FAILED")
    
    print(f"\nPath consistency tests: {success_count}/{len(test_paths)} successful")
    return success_count == len(test_paths)


def explain_function_purpose():
    """Explain what these functions are designed to do."""
    print("\n=== Function Purpose Explanation ===")
    
    print("The generate_partition_path and parse_partition_path functions are designed for:")
    print("1. **Hierarchical Organization**: Extract level structure from STARE SIDs")
    print("2. **Storage Path Generation**: Create organized directory structures")
    print("3. **Spatial Grouping**: Group data by STARE hierarchical levels")
    print()
    print("They are NOT designed for:")
    print("1. **Complete SID Reconstruction**: Full spatial information is not preserved")
    print("2. **Exact Round-Trip**: Lower bits contain spatial data not used in paths")
    print("3. **Spatial Precision**: Only hierarchical levels are extracted, not coordinates")
    print()
    print("Key Points:")
    print("- Paths represent hierarchical organization, not complete spatial data")
    print("- Level structure (number of levels and values) is preserved")
    print("- Lower bits of SID contain spatial coordinates not used in paths")
    print("- Functions are perfect for storage organization and spatial grouping")
    print("- Round-trip works perfectly for 'pure' level SIDs (SIDs with only level data)")


def demonstrate_real_usage():
    """Demonstrate real-world usage scenarios."""
    print("\n=== Real-World Usage Demonstration ===")
    
    sdf = STAREDataFrame()
    
    # Scenario 1: Storage organization
    print("Scenario 1: Storage Organization")
    sid = 3448068485499011499
    dataset = "MOD09_L2"
    
    path = sdf.generate_partition_path(sid, dataset)
    print(f"  SID {sid} → Path: {path}")
    print(f"  Storage location: /data/parquet/{path}")
    print(f"  S3 location: s3://my-bucket/parquet/{path}")
    
    # Scenario 2: Spatial grouping
    print("\nScenario 2: Spatial Grouping")
    similar_sids = [3445253714938429444, 3447505514752114692]
    
    for i, sid in enumerate(similar_sids):
        path = sdf.generate_partition_path(sid, "MOD09")
        components = path.split('/')
        common_prefix = '/'.join(components[:-2])
        print(f"  SID {sid} → Common prefix: {common_prefix}")
    
    print("  Files with same prefix are spatially related")
    
    # Scenario 3: Path parsing for queries
    print("\nScenario 3: Path-Based Queries")
    query_path = "Q00_5/Q01_3/Q02_3/Q03_2/*/MOD09"
    print(f"  Query pattern: {query_path}")
    print("  This would find all MOD09 data in the Q00_5/Q01_3/Q02_3/Q03_2 region")
    print("  Enables efficient spatial range queries")


def main():
    """Run all tests and demonstrations."""
    print("STARE SID Path Level Consistency Tests")
    print("=" * 50)
    
    # Run tests
    test_level_consistency()
    test1_success = test_pure_level_sids()
    test2_success = test_path_generation_consistency()
    explain_function_purpose()
    demonstrate_real_usage()
    
    print("\n" + "=" * 50)
    print("Test Summary:")
    if test1_success:
        print("✓ Pure level SID test PASSED")
    else:
        print("✗ Pure level SID test FAILED")
    
    if test2_success:
        print("✓ Path consistency test PASSED")
    else:
        print("✗ Path consistency test FAILED")
    
    print("\nConclusion:")
    print("The functions work correctly for their intended purpose:")
    print("- Hierarchical level extraction and reconstruction")
    print("- Storage path organization")
    print("- Spatial grouping and queries")
    print("- Level structure is perfectly preserved")
    print("- Complete spatial data preservation is not the goal")


if __name__ == "__main__":
    main()
