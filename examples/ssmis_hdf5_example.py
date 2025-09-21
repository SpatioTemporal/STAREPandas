#!/usr/bin/env python3
"""
Example demonstrating the enhanced SSMIS class that supports both NetCDF4 and HDF5 files.

This example shows how to:
1. Load SSMIS data from NetCDF4 files
2. Load SSMIS data from HDF5 files
3. Handle both local and S3 files
4. Use the context manager for proper file handling
"""

import os
import sys
import numpy as np

# Add the parent directory to the path to import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules.ssmis import SSMIS


def example_netcdf4_file():
    """Example using a NetCDF4 SSMIS file."""
    print("=== NetCDF4 SSMIS File Example ===")

    # Example file path (replace with actual path)
    file_path = "path/to/your/ssmis_file.nc"

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid SSMIS NetCDF4 file.")
        return

    try:
        # Using context manager for automatic file cleanup
        with SSMIS(file_path, scans=['S1', 'S2']) as ssmis:
            print(f"File type detected: {ssmis.file_type}")
            print(f"Available scans: {ssmis.scans}")

            # Read latitude and longitude
            ssmis.read_latlon()
            print(f"Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"Longitude shape for S1: {ssmis.lon['S1'].shape}")

            # Read timestamps
            ssmis.read_timestamps()
            print(f"Timestamp shape for S1: {ssmis.timestamps['S1'].shape}")

            # Read data
            ssmis.read_data()
            print(f"Available data for S1: {list(ssmis.data['S1'].keys())}")
            print(f"Tc1 data shape: {ssmis.data['S1']['Tc1'].shape}")

            # Convert to DataFrame
            dfs = ssmis.to_df()
            print(f"DataFrame for S1 has {len(dfs['S1'])} rows")

    except Exception as e:
        print(f"Error reading NetCDF4 file: {e}")


def example_hdf5_file():
    """Example using an HDF5 SSMIS file."""
    print("\n=== HDF5 SSMIS File Example ===")

    # Example file path (replace with actual path)
    file_path = "path/to/your/ssmis_file.h5"

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid SSMIS HDF5 file.")
        return

    try:
        # Using context manager for automatic file cleanup
        with SSMIS(file_path, scans=['S1', 'S2', 'S3', 'S4']) as ssmis:
            print(f"File type detected: {ssmis.file_type}")
            print(f"Available scans: {ssmis.scans}")

            # Read latitude and longitude
            ssmis.read_latlon()
            print(f"Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"Longitude shape for S1: {ssmis.lon['S1'].shape}")

            # Read timestamps
            ssmis.read_timestamps()
            print(f"Timestamp shape for S1: {ssmis.timestamps['S1'].shape}")

            # Read data
            ssmis.read_data()
            print(f"Available data for S1: {list(ssmis.data['S1'].keys())}")
            print(f"Tc1 data shape: {ssmis.data['S1']['Tc1'].shape}")

            # Convert to DataFrame
            dfs = ssmis.to_df()
            print(f"DataFrame for S1 has {len(dfs['S1'])} rows")

    except Exception as e:
        print(f"Error reading HDF5 file: {e}")


def example_s3_file():
    """Example using an S3 SSMIS file (works with both NetCDF4 and HDF5)."""
    print("\n=== S3 SSMIS File Example ===")

    # Example S3 file path (replace with actual path)
    s3_file_path = "s3://your-bucket/path/to/ssmis_file.h5"

    try:
        # Using context manager for automatic file cleanup
        with SSMIS(s3_file_path, scans=['S1']) as ssmis:
            print(f"File type detected: {ssmis.file_type}")
            print(f"Available scans: {ssmis.scans}")

            # Read latitude and longitude
            ssmis.read_latlon()
            print(f"Latitude shape for S1: {ssmis.lat['S1'].shape}")
            print(f"Longitude shape for S1: {ssmis.lon['S1'].shape}")

            # Read timestamps
            ssmis.read_timestamps()
            print(f"Timestamp shape for S1: {ssmis.timestamps['S1'].shape}")

            # Read data
            ssmis.read_data()
            print(f"Available data for S1: {list(ssmis.data['S1'].keys())}")
            print(f"Tc1 data shape: {ssmis.data['S1']['Tc1'].shape}")

    except Exception as e:
        print(f"Error reading S3 file: {e}")


def example_auto_detection():
    """Example showing automatic file type detection."""
    print("\n=== Automatic File Type Detection Example ===")

    # The SSMIS class automatically detects file type based on extension
    # or by attempting to open the file

    file_paths = [
        "path/to/ssmis_file.nc",    # Will be detected as NetCDF4
        "path/to/ssmis_file.h5",    # Will be detected as HDF5
        "path/to/ssmis_file.hdf5",  # Will be detected as HDF5
        "path/to/ssmis_file.unknown"  # Will attempt to detect by content
    ]

    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                with SSMIS(file_path) as ssmis:
                    print(f"File: {file_path}")
                    print(f"Detected type: {ssmis.file_type}")
                    print(f"Available scans: {ssmis.scans}")
                    print()
            except Exception as e:
                print(f"Error with {file_path}: {e}")
        else:
            print(f"File not found: {file_path}")


def main():
    """Run all examples."""
    print("SSMIS HDF5/NetCDF4 Support Example")
    print("=" * 50)

    # Run examples
    example_netcdf4_file()
    example_hdf5_file()
    example_s3_file()
    example_auto_detection()

    print("\n" + "=" * 50)
    print("Example completed!")
    print("\nKey Features:")
    print("- Automatic file type detection (NetCDF4 vs HDF5)")
    print("- Support for local and S3 files")
    print("- Context manager for proper file handling")
    print("- Same API for both file types")
    print("- Error handling for missing dependencies")


if __name__ == "__main__":
    main()