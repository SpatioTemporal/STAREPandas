# Reading Chunked Parquet Data from S3

This document explains how to read Parquet data stored in a chunked format from S3, which is different from the grouped format expected by `STAREDataFrame.from_s3()`.

## Problem

When trying to read Parquet data from S3 using `STAREDataFrame.from_s3()`, you might get an empty DataFrame even though the data exists. This happens when the data is stored in a **chunked format** rather than the **grouped format** that `from_s3()` expects.

## Data Format Differences

### Chunked Parquet Format
- **Structure**: Single Parquet partition at root level
- **Storage**: All arrays stored as chunked arrays in the root directory
- **Efficiency**: More efficient for large datasets
- **Use case**: When data is stored as a single large dataset

### Grouped Parquet Format
- **Structure**: Root contains multiple group directories
- **Storage**: Each group directory contains arrays for that STARE group
- **Efficiency**: Better for partitioned data access
- **Use case**: When data is partitioned by STARE groups

## Solution: `from_s3()`

Use the `from_s3()` function to read chunked Parquet data:

```python
from starepandas.io.granules import from_s3

# Read chunked Parquet data
df = from_s3('s3://my-bucket/granule_data/')
```

## Examples

### Basic Usage

```python
from starepandas.io.granules import from_s3

# S3 path to chunked Parquet data
s3_path = "s3://zarrpods/MOD09.A2020032.1940.006.2020034015024/"

# Read the data
df = from_s3(s3_path)

print(f"Loaded data: {df.shape}")
print(f"Columns: {list(df.columns)}")
```

### With Custom Storage Options

```python
from starepandas.io.granules import from_s3

# Custom storage options
storage_options = {
    'key': 'your-access-key',
    'secret': 'your-secret-key',
    'client_kwargs': {'region_name': 'us-west-2'}
}

# Read with custom options
df = from_s3(s3_path, storage_options=storage_options)
```

### Using AWS Configuration

```python
from starepandas.io.granules import from_s3
from starepandas.staredataframe import _load_config_from_default_locations

# Load AWS configuration
_load_config_from_default_locations()

# Read data (will use loaded config)
df = from_s3(s3_path)
```

## Function Reference

### `from_s3(s3_path, storage_options=None)`

Read STAREDataFrame from S3 chunked Parquet store.

**Parameters:**
- `s3_path` (str): S3 path to the storage root directory containing chunked Parquet data
- `storage_options` (dict, optional): S3 storage options including credentials and region

**Returns:**
- `STAREDataFrame`: The reconstructed STAREDataFrame

**Raises:**
- `ValueError`: If S3 configuration is missing

## Troubleshooting

### Empty DataFrame from `from_s3()`

If `STAREDataFrame.from_s3()` returns an empty DataFrame:

1. **Check the data format**: Your data might be in chunked format
2. **Use `from_s3()`**: Try the chunked version instead
3. **Verify the path**: Make sure you're using the root path, not a group-specific path

### Missing S3 Configuration

If you get a "Missing S3 configuration" error:

1. **Set storage options explicitly**:
   ```python
   storage_options = {
       'key': 'your-key',
       'secret': 'your-secret',
       'client_kwargs': {'region_name': 'your-region'}
   }
   df = from_s3(s3_path, storage_options=storage_options)
   ```

2. **Use AWS configuration file**:
   ```python
   from starepandas.staredataframe import _load_config_from_default_locations
   _load_config_from_default_locations()
   ```

3. **Set environment variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your-key
   export AWS_SECRET_ACCESS_KEY=your-secret
   export AWS_DEFAULT_REGION=your-region
   ```

## Data Analysis Example

```python
from starepandas.io.granules import from_s3

# Read the data
df = from_s3(s3_path)

# Basic information
print(f"Data shape: {df.shape}")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

# Geographic coverage
if 'lat' in df.columns and 'lon' in df.columns:
    print(f"Latitude range: {df['lat'].min():.3f}° to {df['lat'].max():.3f}°")
    print(f"Longitude range: {df['lon'].min():.3f}° to {df['lon'].max():.3f}°")

# STARE analysis
if 'sids' in df.columns:
    print(f"Unique SIDs: {df['sids'].nunique():,}")
    print(f"SID density: {df['sids'].nunique() / len(df) * 100:.2f}%")

# Data quality
for col in df.columns:
    if col not in ['lat', 'lon', 'sids']:
        null_pct = df[col].isnull().sum() / len(df) * 100
        print(f"{col}: {null_pct:.1f}% missing values")
```

## Performance Considerations

- **Chunked format**: More efficient for reading entire datasets
- **Grouped format**: Better for reading specific STARE groups
- **Memory usage**: Chunked format loads all data into memory
- **Network efficiency**: Chunked format reduces S3 API calls

## Reading Specific Groups

For even more efficiency, you can read only specific STARE groups using `from_s3_groups()`:

```python
from starepandas.io.granules import from_s3_groups

# Read specific groups
group_ids = [3447505514752114692, 3445253714938429444]
df = from_s3_groups(s3_path, group_ids)

print(f"Loaded {len(df)} rows from {len(group_ids)} groups")
```

This function:
- Reads only the specified group directories
- Combines data from multiple groups into a single DataFrame
- Provides significant memory and processing time savings
- Ideal for spatial analysis and quality control

### Use Cases

1. **Spatial Analysis**: Read groups covering specific geographic regions
2. **Quality Control**: Validate data in specific areas
3. **Research**: Focus on specific atmospheric conditions or surface types
4. **Efficiency**: Reduce memory usage for large-scale analysis

## Related Functions

- `STAREDataFrame.from_s3()`: For grouped Parquet format
- `STAREDataFrame.to_s3()`: For writing grouped Parquet format
- `from_s3()`: For reading chunked Parquet format
- `from_s3_groups()`: For reading specific STARE groups

## See Also

- [Generic to_s3 Function](generic_to_s3_api.md)
- [Metadata API](metadata_api.md)
- [SSMIS HDF5 Support](ssmis_hdf5_support.md)
