# SSMIS HDF5 Support

The SSMIS class has been enhanced to support both NetCDF4 and HDF5 file formats. This allows for greater flexibility in handling SSMIS data files.

## Features

### Automatic File Type Detection
The SSMIS class automatically detects the file type based on:
- File extension (`.nc`, `.nc4` for NetCDF4; `.h5`, `.hdf5` for HDF5)
- File content (attempts to open as HDF5 first, then NetCDF4)

### Unified API
The same API works for both file types:
- `read_latlon()` - Read latitude and longitude data
- `read_timestamps()` - Read timestamp data
- `read_data()` - Read temperature data
- `to_df()` - Convert to DataFrame format

### S3 Support
Both local and S3-hosted files are supported for both formats.

### Context Manager Support
The class implements context manager protocol for proper file handling:

```python
with SSMIS(file_path) as ssmis:
    ssmis.read_latlon()
    ssmis.read_data()
    # File automatically closed when exiting context
```

## Usage Examples

### NetCDF4 Files
```python
from starepandas.io.granules.ssmis import SSMIS

# NetCDF4 file
with SSMIS("path/to/file.nc", scans=['S1', 'S2']) as ssmis:
    print(f"File type: {ssmis.file_type}")  # 'netcdf4'
    ssmis.read_latlon()
    ssmis.read_data()
    dfs = ssmis.to_df()
```

### HDF5 Files
```python
# HDF5 file
with SSMIS("path/to/file.h5", scans=['S1', 'S2', 'S3', 'S4']) as ssmis:
    print(f"File type: {ssmis.file_type}")  # 'hdf5'
    ssmis.read_latlon()
    ssmis.read_data()
    dfs = ssmis.to_df()
```

### S3 Files
```python
# S3-hosted file (works with both formats)
with SSMIS("s3://bucket/path/to/file.h5") as ssmis:
    print(f"File type: {ssmis.file_type}")
    ssmis.read_latlon()
    ssmis.read_data()
```

### Automatic Detection
```python
# File type automatically detected
with SSMIS("path/to/file.unknown") as ssmis:
    print(f"Detected type: {ssmis.file_type}")
    # Rest of the code works the same regardless of type
```

## File Structure Requirements

Both NetCDF4 and HDF5 files should have the same internal structure:

```
File
├── FileHeader (attributes or variable)
├── S1/
│   ├── Latitude
│   ├── Longitude
│   ├── ScanTime/
│   │   ├── Year
│   │   ├── Month
│   │   ├── DayOfMonth
│   │   ├── Hour
│   │   ├── Minute
│   │   ├── Second
│   │   └── MilliSecond
│   └── Tc (temperature data)
├── S2/
│   └── ... (same structure as S1)
├── S3/
│   └── ... (same structure as S1)
└── S4/
    └── ... (same structure as S1)
```

## Dependencies

The enhanced SSMIS class requires the following packages:

- `h5py` - For HDF5 file support
- `netCDF4` - For NetCDF4 file support
- `boto3` - For S3 file access
- `numpy` - For numerical operations
- `pystare` - For STARE index operations

### Installation
```bash
pip install h5py netCDF4 boto3 numpy pystare
```

## Error Handling

The class provides clear error messages for missing dependencies:

- `ImportError: h5py is required for HDF5 file access`
- `ImportError: netCDF4 is required for NetCDF4 file access`
- `ImportError: boto3 is required for S3 file access`

## Migration from Previous Version

The enhanced SSMIS class is backward compatible. Existing code using NetCDF4 files will continue to work without changes. The only difference is that the internal `netcdf` attribute is now called `dataset` and supports both file types.

### Before (NetCDF4 only)
```python
ssmis = SSMIS("file.nc")
ssmis.netcdf.groups['S1']['Latitude'][:]  # Direct access
```

### After (Both formats)
```python
ssmis = SSMIS("file.nc")  # or "file.h5"
if ssmis.file_type == 'hdf5':
    ssmis.dataset['S1']['Latitude'][:]
else:  # netcdf4
    ssmis.dataset.groups['S1']['Latitude'][:]
```

However, it's recommended to use the provided methods instead of direct dataset access:

```python
ssmis.read_latlon()  # Handles both formats automatically
ssmis.lat['S1']  # Access latitude data
```

## Performance Considerations

- HDF5 files may have slightly different performance characteristics than NetCDF4 files
- S3 access adds network latency regardless of file format
- The context manager ensures proper file cleanup and resource management

## Testing

To test the HDF5 support, you can use the provided example:

```bash
python examples/ssmis_hdf5_example.py
```

Make sure to update the file paths in the example to point to actual SSMIS files. 