# STARE SID Zarr Path Generation

The `generate_zarr_path` method in `STAREDataFrame` creates hierarchical relative paths for storing zarr files based on the STARE SID (Spatial Temporal Adaptive Resolution Encoding Spatial Index) structure.

## Overview

This function extracts the hierarchical level structure encoded in STARE SIDs and generates organized directory paths that reflect the spatial relationships in the data. This enables efficient storage, retrieval, and spatial queries of zarr-formatted datasets.

## Function Signatures

### generate_zarr_path

```python
def generate_zarr_path(self, sid, dataset_name):
    """
    Generate relative path for storing zarr file based on STARE SID structure.
    
    Parameters
    ----------
    sid : int
        8-byte STARE SID integer
    dataset_name : str
        Name of the dataset
        
    Returns
    -------
    str
        Relative path for storing zarr file
    """
```

### parse_zarr_path

```python
def parse_zarr_path(self, zarr_path):
    """
    Parse hierarchical zarr path and reconstruct STARE SID from path components.
    
    This is the reverse operation of generate_zarr_path.
    
    Parameters
    ----------
    zarr_path : str
        Hierarchical path in format Q00_X/Q01_Y/.../QN_M/DatasetName
        
    Returns
    -------
    tuple
        (sid, dataset_name) where sid is the reconstructed STARE SID integer
        and dataset_name is the extracted dataset name
    """
```

## Path Structure

The generated path follows the pattern:
```
Q00_X/Q01_Y/Q02_Z/.../QN_M/DatasetName
```

Where:
- `Q00`, `Q01`, ..., `QN` are the STARE levels (N+1 nodes from root to leaf)
- `_X`, `_Y`, `_Z`, ..., `_M` are the values at each level
- `DatasetName` is the input dataset name

## STARE SID Bit Layout

The function extracts hierarchical information from the 64-bit STARE SID:

| Bits | Purpose | Values | Description |
|------|---------|--------|-------------|
| 0-4 | Number of levels | 0-31 | Total levels in hierarchy (N+1) |
| 5-6 | Level 27 value | 0-3 | Value at level 27 |
| 7-8 | Level 26 value | 0-3 | Value at level 26 |
| ... | ... | ... | ... |
| 57-58 | Level 1 value | 0-3 | Value at level 1 |
| 59-61 | Level 0 value | 0-7 | Value at level 0 (root) |
| 62-63 | Ignored | - | Reserved bits |

### Level Value Ranges
- **Level 0**: 3 bits → values 0-7 (8 possible values)
- **Levels 1-27**: 2 bits each → values 0-3 (4 possible values each)
- **Levels 28+**: Not encoded in SID → default to 0

## Usage Examples

### Basic Usage

```python
from starepandas import STAREDataFrame

# Create STAREDataFrame instance
sdf = STAREDataFrame()

# Generate path for a STARE SID
sid = 3448068485499011499
dataset = "MOD09"
path = sdf.generate_zarr_path(sid, dataset)
print(path)
# Output: Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/Q05_1/Q06_0/Q07_0/Q08_0/Q09_0/Q10_0/Q11_0/MOD09

# Parse path back to SID and dataset
reconstructed_sid, reconstructed_dataset = sdf.parse_zarr_path(path)
print(f"SID: {reconstructed_sid}, Dataset: {reconstructed_dataset}")
# Output: SID: 3448068464705536011, Dataset: MOD09
```

### Bidirectional Operations

```python
# Round-trip example: SID → Path → SID
original_sid = 3445253714938429444
dataset = "VIIRS_L2"

# Generate path
path = sdf.generate_zarr_path(original_sid, dataset)
print(f"Path: {path}")

# Parse path back
reconstructed_sid, parsed_dataset = sdf.parse_zarr_path(path)
print(f"Reconstructed SID: {reconstructed_sid}")
print(f"Dataset: {parsed_dataset}")

# Check level structure consistency
original_levels = (original_sid & 0x1F) + 1
reconstructed_levels = (reconstructed_sid & 0x1F) + 1
print(f"Level structure preserved: {original_levels == reconstructed_levels}")
```

### Multiple Datasets for Same Region

```python
# Different processing levels for same spatial region
sid = 3448068485499011499
datasets = ["MOD09_L1", "MOD09_L2", "MOD09_L3"]

for dataset in datasets:
    path = sdf.generate_zarr_path(sid, dataset)
    print(f"{dataset}: {path}")

# Output:
# MOD09_L1: Q00_5/Q01_3/.../Q11_0/MOD09_L1
# MOD09_L2: Q00_5/Q01_3/.../Q11_0/MOD09_L2
# MOD09_L3: Q00_5/Q01_3/.../Q11_0/MOD09_L3
```

### Spatial Relationships

```python
# SIDs with similar spatial locations
similar_sids = [
    3445253714938429444,  # 5 levels
    3447505514752114692,  # 5 levels
]

for i, sid in enumerate(similar_sids):
    path = sdf.generate_zarr_path(sid, "MOD09")
    print(f"Region {i+1}: {path}")

# Output:
# Region 1: Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09
# Region 2: Q00_5/Q01_3/Q02_3/Q03_2/Q04_3/MOD09
# Note: Common prefix Q00_5/Q01_3/Q02_3/Q03_2 indicates spatial proximity
```

## Storage Applications

### Cloud Storage Organization

```python
# S3 bucket organization
s3_base = "s3://my-bucket/zarr-data"
sid = 3448068485499011499
dataset = "MOD09"

path = sdf.generate_zarr_path(sid, dataset)
full_s3_path = f"{s3_base}/{path}"
print(full_s3_path)
# Output: s3://my-bucket/zarr-data/Q00_5/Q01_3/.../MOD09
```

### Local File System

```python
# Local storage organization
local_base = "/data/zarr"
path = sdf.generate_zarr_path(sid, dataset)
full_local_path = f"{local_base}/{path}"
print(full_local_path)
# Output: /data/zarr/Q00_5/Q01_3/.../MOD09
```

## Benefits

### Spatial Locality
- Files with similar SIDs are stored in nearby directories
- Enables efficient spatial range queries
- Supports hierarchical data access patterns

### Scalability
- Distributes files across multiple directories
- Prevents single directory from becoming too large
- Supports parallel processing by directory branches

### Data Management
- Logical organization reflects spatial relationships
- Easy to archive or migrate specific regions
- Supports incremental backups by spatial areas

### Query Efficiency
- Spatial queries can target specific directory trees
- Reduces I/O by avoiding irrelevant data
- Enables spatial indexing at the file system level

## Integration with Zarr Storage

### Writing Data

```python
# Example: Writing zarr data using generated paths
import zarr
import os

sdf = STAREDataFrame(your_data)
base_path = "/data/zarr"

for index, row in sdf.iterrows():
    sid = row['sids']
    dataset = "MOD09"
    
    # Generate hierarchical path
    rel_path = sdf.generate_zarr_path(sid, dataset)
    full_path = os.path.join(base_path, rel_path)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    # Write zarr data
    zarr_group = zarr.open_group(full_path, mode='w')
    # ... write your data
```

### Reading Data

```python
# Example: Reading data by spatial region
def read_spatial_region(base_path, level_prefix):
    """Read all data matching a spatial prefix."""
    import glob
    
    pattern = os.path.join(base_path, level_prefix, "**/MOD09")
    zarr_paths = glob.glob(pattern, recursive=True)
    
    # Read and combine data from matching paths
    data_frames = []
    for path in zarr_paths:
        zg = zarr.open_group(path, mode='r')
        # ... read zarr data and convert to DataFrame
        data_frames.append(df)
    
    return pd.concat(data_frames, ignore_index=True)

# Read all data from Q00_5/Q01_3 region
region_data = read_spatial_region("/data/zarr", "Q00_5/Q01_3")
```

## Performance Considerations

### Directory Depth
- Deeper hierarchies provide finer spatial organization
- Balance between organization and file system overhead
- Consider file system limitations on directory depth

### File Distribution
- STARE encoding naturally distributes files
- Prevents hotspots in storage systems
- Enables parallel access patterns

### Caching
- Directory structure supports efficient caching strategies
- Spatial locality improves cache hit rates
- Hierarchical prefetching possible

## Best Practices

### Path Construction
- Always use the `generate_zarr_path` method for consistency
- Validate SID values before path generation
- Use meaningful dataset names for clarity

### Storage Layout
- Organize by dataset type at the base level if needed
- Consider using date/time partitioning alongside spatial
- Document your storage schema for team consistency

### Backup and Archival
- Archive by directory trees to maintain spatial relationships
- Use hierarchical storage management (HSM) systems
- Implement retention policies by spatial regions

## Error Handling

The function handles various edge cases:

- **Invalid SIDs**: Masked to 64-bit values
- **Large level counts**: Levels beyond 27 default to 0
- **Empty dataset names**: Included as-is in path
- **Special characters**: Preserved in dataset names

```python
# Example error handling
try:
    path = sdf.generate_zarr_path(sid, dataset_name)
    # Use path for storage operations
except Exception as e:
    print(f"Error generating path for SID {sid}: {e}")
    # Handle error appropriately
```

## Function Relationship

The `generate_zarr_path` and `parse_zarr_path` functions work together as complementary operations:

- **generate_zarr_path**: Extracts hierarchical level structure from STARE SIDs
- **parse_zarr_path**: Reconstructs hierarchical level structure into STARE SIDs
- **Level Structure Preservation**: Both functions preserve the hierarchical organization
- **Spatial Information**: Lower bits containing precise spatial coordinates are not preserved
- **Perfect Round-Trip**: Works perfectly for SIDs containing only level information

### Important Notes

1. **Purpose**: These functions are designed for hierarchical storage organization, not complete spatial data preservation
2. **Level Structure**: The hierarchical level structure is perfectly preserved in both directions
3. **Spatial Precision**: Precise spatial coordinates in lower SID bits are not used in path generation
4. **Validation**: `parse_zarr_path` includes comprehensive validation and error handling
5. **Bit Clearing**: Bits 62-63 are always cleared in reconstructed SIDs as requested

## Related Functions

- `STAREDataFrame.to_zarr_s3()`: Write grouped zarr to S3
- `STAREDataFrame.to_zarr_local()`: Write grouped zarr locally
- `STAREDataFrame.generate_zarr_path()`: Generate hierarchical paths from SIDs
- `STAREDataFrame.parse_zarr_path()`: Parse hierarchical paths back to SIDs
- `from_zarr_s3_chunked_groups()`: Read specific groups from S3
- `to_zarr_s3()` (granules): Generic zarr writing function

## See Also

- [Chunked Zarr Reading](chunked_zarr_reading.md)
- [Generic to_zarr_s3 Function](generic_to_zarr_s3_api.md)
- [Zarr Metadata API](zarr_metadata_api.md)
- [STARE Documentation](https://stare.readthedocs.io/)
