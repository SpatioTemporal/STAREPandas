#!/usr/bin/env python3
"""
Example demonstrating the generic to_zarr_s3 function in starepandas.io.granules.

This example shows how to:
1. Convert a granule file directly to zarr format and store it in S3
2. Use different parameters for different granule types
3. Handle metadata and configuration
"""

import os
import sys
import datetime

# Add the parent directory to the path to import starepandas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starepandas.io.granules import to_zarr_s3


def example_modis_granule():
    """Example using a MODIS granule."""
    print("=== MODIS Granule Example ===")
    
    # Example file path (replace with actual path)
    file_path = "path/to/MOD05_L2.A2019336.0000.061.2019336211522.hdf"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid MODIS file.")
        return
    
    try:
        # Convert MODIS granule to zarr and store in S3
        s3_path = to_zarr_s3(
            file_path=file_path,
            s3_path="s3://my-bucket/modis_data/MOD05_L2_2019336",
            level=10,  # STARE level for partitioning
            chunk_size=250000,
            dataset="MOD05_L2",
            data_level="L2",
            metadata={
                "satellite": "Terra",
                "instrument": "MODIS",
                "product": "MOD05_L2",
                "processing_date": datetime.datetime.now().isoformat()
            }
        )
        
        print(f"✓ Successfully converted MODIS granule to zarr")
        print(f"✓ Data stored at: {s3_path}")
        
    except Exception as e:
        print(f"✗ Error processing MODIS granule: {e}")


def example_viirs_granule():
    """Example using a VIIRS granule."""
    print("\n=== VIIRS Granule Example ===")
    
    # Example file path (replace with actual path)
    file_path = "path/to/VNP02DNB.A2020219.0742.001.2020219125654.nc"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid VIIRS file.")
        return
    
    try:
        # Convert VIIRS granule to zarr and store in S3
        s3_path = to_zarr_s3(
            file_path=file_path,
            s3_path="s3://my-bucket/viirs_data/VNP02DNB_2020219",
            level=12,  # Higher resolution for VIIRS
            chunk_size=500000,  # Larger chunks for VIIRS data
            dataset="VNP02DNB",
            data_level="L1B",
            read_timestamp=True,  # Include timestamp data
            metadata={
                "satellite": "SNPP",
                "instrument": "VIIRS",
                "product": "VNP02DNB",
                "processing_date": datetime.datetime.now().isoformat()
            }
        )
        
        print(f"✓ Successfully converted VIIRS granule to zarr")
        print(f"✓ Data stored at: {s3_path}")
        
    except Exception as e:
        print(f"✗ Error processing VIIRS granule: {e}")


def example_ssmis_granule():
    """Example using an SSMIS granule."""
    print("\n=== SSMIS Granule Example ===")
    
    # Example file path (replace with actual path)
    file_path = "path/to/1C.F16.SSMIS.XCAL2016-V.20210110-S002714-E020909.088907.V05A.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid SSMIS file.")
        return
    
    try:
        # Convert SSMIS granule to zarr and store in S3
        s3_path = to_zarr_s3(
            file_path=file_path,
            s3_path="s3://my-bucket/ssmis_data/SSMIS_20210110",
            level=8,  # Lower resolution for SSMIS
            chunk_size=100000,  # Smaller chunks for SSMIS data
            dataset="SSMIS",
            data_level="L1C",
            add_sids=True,
            adapt_resolution=True,
            metadata={
                "satellite": "F16",
                "instrument": "SSMIS",
                "product": "1C",
                "processing_date": datetime.datetime.now().isoformat()
            }
        )
        
        print(f"✓ Successfully converted SSMIS granule to zarr")
        print(f"✓ Data stored at: {s3_path}")
        
    except Exception as e:
        print(f"✗ Error processing SSMIS granule: {e}")


def example_ssmis_specific_scan():
    """Example using a specific SSMIS scan."""
    print("\n=== SSMIS Specific Scan Example ===")
    
    # Example file path (replace with actual path)
    file_path = "path/to/1C.F16.SSMIS.XCAL2016-V.20210110-S002714-E020909.088907.V05A.HDF5"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid SSMIS file.")
        return
    
    try:
        # Convert specific SSMIS scan (S1) to zarr and store in S3
        s3_path = to_zarr_s3(
            file_path=file_path,
            s3_path="s3://my-bucket/ssmis_data/SSMIS_S1_20210110",
            level=8,
            scan="S1",  # Process only S1 scan
            chunk_size=100000,
            dataset="SSMIS",
            data_level="L1C",
            metadata={
                "satellite": "F16",
                "instrument": "SSMIS",
                "product": "1C",
                "scan": "S1",
                "processing_date": datetime.datetime.now().isoformat()
            }
        )
        
        print(f"✓ Successfully converted SSMIS S1 scan to zarr")
        print(f"✓ Data stored at: {s3_path}")
        
    except Exception as e:
        print(f"✗ Error processing SSMIS S1 scan: {e}")


def example_with_custom_storage_options():
    """Example with custom S3 storage options."""
    print("\n=== Custom Storage Options Example ===")
    
    # Example file path (replace with actual path)
    file_path = "path/to/granule_file.nc"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please update the file_path variable with a valid granule file.")
        return
    
    # Custom storage options
    storage_options = {
        'key': 'your-access-key',
        'secret': 'your-secret-key',
        'region_name': 'us-west-2',
        'endpoint_url': 'https://s3.us-west-2.amazonaws.com'
    }
    
    try:
        # Convert granule with custom storage options
        s3_path = to_zarr_s3(
            file_path=file_path,
            s3_path="s3://my-bucket/custom_data",
            level=10,
            storage_options=storage_options,
            dataset="CUSTOM",
            data_level="L1",
            raw_collected_time=datetime.datetime.utcnow(),
            metadata={
                "custom_field": "custom_value",
                "processing_date": datetime.datetime.now().isoformat()
            }
        )
        
        print(f"✓ Successfully converted granule with custom storage options")
        print(f"✓ Data stored at: {s3_path}")
        
    except Exception as e:
        print(f"✗ Error processing granule with custom options: {e}")


def main():
    """Run all examples."""
    print("Generic to_zarr_s3 Function Examples")
    print("=" * 50)
    
    # Run examples
    example_modis_granule()
    example_viirs_granule()
    example_ssmis_granule()
    example_ssmis_specific_scan()
    example_with_custom_storage_options()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nKey Features:")
    print("- Direct conversion from granule files to S3 zarr storage")
    print("- Automatic STARE indexing and partitioning")
    print("- Support for all granule types (MODIS, VIIRS, SSMIS, etc.)")
    print("- Support for multi-scan granules (e.g., SSMIS)")
    print("- Customizable metadata and storage options")
    print("- Unified API for different granule formats")
    
    print("\nUsage Notes:")
    print("- Replace file paths with actual granule files")
    print("- Configure S3 credentials before running")
    print("- Adjust STARE levels based on your data resolution needs")
    print("- Use appropriate chunk sizes for your data volume")
    print("- For multi-scan granules, use 'scan' parameter to process specific scans")


if __name__ == "__main__":
    main()
