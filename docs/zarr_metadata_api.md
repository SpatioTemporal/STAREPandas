# Zarr Metadata API

The `starepandas.io.granules` module provides functions to load and analyze metadata from the RDS database for zarr data stored in S3. These functions allow you to query information about datasets that have been processed and stored using the `to_zarr_s3` function.

## Overview

When you use the `to_zarr_s3` function to store granule data in S3, metadata is automatically written to an RDS PostgreSQL database. This metadata includes:

- Dataset information (name, data level)
- S3 storage details (bucket, group paths)
- STARE indexing information (resolution level, group IDs)
- Data statistics (number of rows, columns)
- Timestamps and additional metadata

## Functions

### `load_zarr_metadata()`

Load metadata from the RDS database with flexible filtering options.

**Parameters:**
- `dataset` (str, optional): Filter by dataset name (e.g., "MOD05_L2", "VNP02DNB", "SSMIS")
- `data_level` (str, optional): Filter by data level (e.g., "L1B", "L2", "L1C")
- `s3_bucket` (str, optional): Filter by S3 bucket name
- `resolution_level` (int, optional): Filter by STARE resolution level
- `start_date` (str/datetime, optional): Filter by start date (inclusive)
- `end_date` (str/datetime, optional): Filter by end date (inclusive)
- `grouped_id` (int, optional): Filter by specific grouped_id
- `limit` (int, optional): Limit the number of results returned
- `order_by` (str, optional): Column to order results by

**Returns:**
- `pandas.DataFrame`: DataFrame containing metadata with columns:
  - `Dataset`: Dataset name
  - `DataLevel`: Data level
  - `RawData Collected Time`: Timestamp when data was collected
  - `grouped_id`: STARE group ID
  - `S3 bucket`: S3 bucket name
  - `Resolution level`: STARE resolution level
  - `MetadataJson`: JSON metadata containing additional information
  - `group_path`: S3 path to the zarr group (from MetadataJson)
  - `num_rows`: Number of rows in the group (from MetadataJson)
  - `columns`: List of columns in the group (from MetadataJson)
  - `scan`: Scan name if applicable (from MetadataJson)

### `get_zarr_summary()`

Get summary statistics about zarr metadata stored in the database.

**Parameters:**
- `dataset` (str, optional): Filter by dataset name
- `data_level` (str, optional): Filter by data level
- `s3_bucket` (str, optional): Filter by S3 bucket name

**Returns:**
- `pandas.DataFrame`: Summary DataFrame with columns:
  - `Dataset`: Dataset name
  - `DataLevel`: Data level
  - `S3 bucket`: S3 bucket name
  - `Resolution level`: STARE resolution level
  - `count`: Number of groups
  - `total_rows`: Total number of rows across all groups
  - `date_range`: Date range of the data
  - `latest_date`: Most recent data collection date

## Usage Examples

### Basic Usage

```python
from starepandas.io.granules import load_zarr_metadata, get_zarr_summary

# Load all metadata
df = load_zarr_metadata()

# Get summary statistics
summary = get_zarr_summary()
```

### Filtering by Dataset

```python
# Load metadata for specific dataset
modis_df = load_zarr_metadata(dataset="MOD05_L2")

# Load metadata for multiple criteria
viirs_df = load_zarr_metadata(
    dataset="VNP02DNB",
    data_level="L1B"
)
```

### Date Range Filtering

```python
# Load metadata for specific date range
df = load_zarr_metadata(
    start_date="2023-01-01",
    end_date="2023-12-31"
)

# Load recent data
from datetime import datetime, timedelta
recent_df = load_zarr_metadata(
    start_date=datetime.now() - timedelta(days=30)
)
```

### S3 and Resolution Filtering

```python
# Load metadata for specific S3 bucket
df = load_zarr_metadata(s3_bucket="my-data-bucket")

# Load metadata for specific resolution level
df = load_zarr_metadata(resolution_level=10)
```

### Custom Ordering and Limiting

```python
# Load most recent 100 records
df = load_zarr_metadata(
    order_by="RawData Collected Time",
    limit=100
)

# Load by dataset name
df = load_zarr_metadata(
    order_by="Dataset",
    limit=50
)
```

### Summary Statistics

```python
# Get summary of all data
summary = get_zarr_summary()

# Get summary for specific dataset
modis_summary = get_zarr_summary(dataset="MOD05_L2")

# Get summary for specific S3 bucket
bucket_summary = get_zarr_summary(s3_bucket="my-data-bucket")
```

## Database Schema

The metadata is stored in a PostgreSQL table called `PodsMetadata` with the following structure:

```sql
CREATE TABLE "PodsMetadata" (
    "Dataset" TEXT,
    "DataLevel" TEXT,
    "RawData Collected Time" TIMESTAMP,
    grouped_id INTEGER,
    "S3 bucket" TEXT,
    "Resolution level" INTEGER,
    "MetadataJson" JSONB
);
```

### MetadataJson Structure

The `MetadataJson` field contains additional information in JSON format. When you call `load_zarr_metadata()`, this JSON is automatically parsed and expanded into separate columns:

```json
{
    "grouped_id_full": 123456789,
    "group_path": "s3://bucket/path/123456789",
    "num_rows": 1000000,
    "columns": ["lat", "lon", "sids", "data1", "data2"],
    "scan": "S1",
    "satellite": "Terra",
    "instrument": "MODIS"
}
```

**Note**: The `num_rows` and other metadata fields are stored in the `MetadataJson` column, not as separate database columns. The `load_zarr_metadata()` function automatically parses this JSON and creates separate columns for easier access.

## Configuration

Before using these functions, you must configure the RDS database connection:

```python
from starepandas import aws_configure

# Configure RDS connection
aws_configure(
    rds={
        'host': 'your-rds-host.amazonaws.com',
        'port': 5432,
        'username': 'your-username',
        'password': 'your-password',
        'database': 'postgres'  # Admin database
    }
)
```

Or load from a configuration file:

```python
from starepandas import load_aws_configure

# Load configuration from JSON file
load_aws_configure('path/to/config.json')
```

## Error Handling

The functions will raise appropriate errors if:

- RDS configuration is missing
- Database connection fails
- SQL query execution fails
- JSON parsing fails

Common error messages:

- `Missing RDS configuration`: RDS connection parameters not set
- `Error loading zarr metadata from database`: Database connection or query error
- `psycopg2 is required`: Missing PostgreSQL driver

## Performance Considerations

- Use appropriate filters to limit the amount of data returned
- Use `limit` parameter for large result sets
- Consider using `get_zarr_summary()` for overview statistics instead of loading all metadata
- Index the database on frequently queried columns (Dataset, DataLevel, S3 bucket, etc.)

## Integration with to_zarr_s3

These functions work seamlessly with the `to_zarr_s3` function:

```python
from starepandas.io.granules import to_zarr_s3, load_zarr_metadata

# Store data in S3
s3_path = to_zarr_s3(
    file_path="path/to/granule.hdf",
    s3_path="s3://bucket/data",
    level=10,
    dataset="MOD05_L2",
    data_level="L2"
)

# Query metadata
metadata = load_zarr_metadata(dataset="MOD05_L2")
```

This allows you to track and manage all your zarr datasets stored in S3 through a centralized metadata system.
