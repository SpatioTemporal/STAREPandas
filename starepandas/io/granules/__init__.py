import glob
import logging
import os
import re
import numpy as np
import pandas as pd
import json
import starepandas

# Lookup table: "INSTRUMENT_Sn" → across-track pixel_width
SCAN_PIXEL_WIDTHS = {
    "GMI_S1": 221, "GMI_S2": 221,
    "SSMIS_S1": 90, "SSMIS_S2": 90, "SSMIS_S3": 180, "SSMIS_S4": 180,
    "AMSR2_S1": 243, "AMSR2_S2": 243, "AMSR2_S3": 243, "AMSR2_S4": 243,
    "AMSR2_S5": 486, "AMSR2_S6": 486,
}

from .modis import Mod09GA, Mod05, Mod09, Mod03
from .viirsl2 import VNP02DNB, VNP03DNB, VNP03MOD, VNP03IMG, CLDMSKL2VIIRS, VNP09
from .ssmis import SSMIS
from .gmi import GMI
from .amsr2 import AMSR2
from .atms import ATMS
from .imergl3 import L3IMERG, DYAMONDv2

class UnsupportedFileError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'cannot handle {}'.format(file_path)
        super().__init__(self.message)


class SidecarNotFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'Could not find sidecar for {}'.format(file_path)
        super().__init__(self.message)


class CompanionNotFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'Could not find companion for {}'.format(file_path)
        super().__init__(self.message)


class MultipleCompanionsFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'More than one possible companion found for {}. Specify the prefix'.format(file_path)
        super().__init__(self.message)


def guess_companion_path(granule_path, folder=None, prefix=None):
    """
    Tries to find a companion to the granule.
    The assumption being that granule file names are composed of
    {Product}.{date}.{time}.{version}.{production_timestamp}.{extension}

    Parameters
    -----------
    granule_path: str
        The path of the granule to find the companion for
    folder: str
        the folder to look for companions
    prefix: str
        The prefix of the companion name; e.g. VJ102DNB

    Examples
    ---------
    # >>> granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    # >>> companion_path = guess_companion_path(granule_path, prefix='VNP03DNB')
    # >>> companion_name = companion_path.split('/')[-1]
    # >>> companion_name
    # 'VNP03DNB.A2020219.0742.001.2020219124651.nc'
    """

    if folder is None:
        folder = '/'.join(granule_path.split('/')[0:-1])
    name = granule_path.split('/')[-1]
    name_parts = name.split('.')
    date = name_parts[1]
    time = name_parts[2]
    if prefix:
        pattern = '{folder}/*{prefix}.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, prefix=prefix, date=date, time=time)
    else:
        pattern = '{folder}/*.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, date=date, time=time)
    matches = glob.glob(folder + '/*')
    companions = set(filter(re.compile(pattern).match, matches))
    companions = list(companions - set([granule_path]))
    if len(companions) < 1 or companions[0] == granule_path:
        raise CompanionNotFoundError(granule_path)
    if len(companions) > 1:
        raise MultipleCompanionsFoundError(granule_path)
    else:
        return companions[0]


granule_factory_library = {
    'MOD09GA|MYD09GA': Mod09GA,
    'MOD05|MYD05': Mod05,
    'MOD03|MYD03': Mod03,
    'MOD09|MYD09': Mod09,
    'VNP02DNB|VJ102DNB': VNP02DNB,
    'VNP03DNB|VJ103DNB': VNP03DNB,
    'VNP03MOD|VJ103MOD': VNP03MOD,
    'VNP03IMG|VJ103IMG': VNP03IMG,
    'VNP09': VNP09,
    'CLDMSKL2VIIRS': CLDMSKL2VIIRS,
    'SSMIS': SSMIS,
    'GMI': GMI,
    'AMSR2': AMSR2,
    'ATMS': ATMS,
    'L3IMERG': L3IMERG,
    'DYAMONDv2': DYAMONDv2
}



def granule_factory(file_path, sidecar_path=None, nom_res=None):

    """
    Returns a granule loader from the dictionary starepandas.io.granules.granule_factory_library.
    The keys in granule_factory_library are regex patterns against which file_path is matched.
    The values are the classes with constructors of signature (file_path,sidecar). For example:

    ```py`thon
        granule_factory_library = { 'MOD05|MYD05'       : Mod05,
                                    'MOD09|MYD09'       : Mod09,
                                    'VNP02DNB|VJ102DNB' : VNP02DNB,
                                    ...}
    ```

    To add a loader for a granule or dataset not presently supported by starepandas,
    one can add a class implementing the interface defined by starepandas.io.granules.Granule.
    This can be done by defining a class inheriting Granule or
    another Granule-derived class (like Modis, Mod09, VIIRSL2, etc.).
    To see the current list of loaders distributed with starepandas, examine granule_factor_library.

    An example of adding a new loader follows.

    ```
    class VNP02IMG(VIIRSL2):
        "Add loader for VNP02IMG, extending VIIRSL2, which extends Granule.
        VNP02IMG holds observations. Geolocations are in VNP03IMG, which has its own loader."

        def __init__(self, file_path, sidecar_path=None):
            super(VNP02IMG, self).__init__(file_path, sidecar_path)
            self.companion_prefix = 'VNP03IMG'

        def read_data(self):
            "Read the data of interest."
            for band in ['I04','I05']:
                IMG = self.netcdf.groups['observation_data'][band][:].data
                quality_flags = self.netcdf.groups['observation_data'][band+'_quality_flags'][:].data
                self.data[band+'_observations']  = IMG
                self.data[band+'_quality_flags'] = quality_flags

        def read_latlon(self):
            "Geolocations will be read from VNP03IMG using a separate loader."
            pass

        def read_sidecar_cover(self, sidecar_path=None):
            "Let VNP03IMG loader handle this."
            pass

        def read_sidecar_index(self, sidecar_path=None):
            "Let VNP03IMG loader handle this."
            pass

    # VNP02IMG is the short name for a Suomi National Polar-orbiting
    # Partnership (SNPP) NASA/VIIRS L1B observation product.
    # Add 'VJ102IMG' to the regex pattern for the Joint Polar-orbiting Satellite System (JPSS-1/NOAA20).

    starepandas.io.granules.granule_factory_library['VNP02IMG|VJ102IMG']=VNP02IMG
    ```

    Then to load a granule file into a starepandas dataframe you can do something like the following.

    ```
    granule_name="/home/jovyan/data/VNP02IMG.A2021182.0000.001.2021182064359.nc"
    vnp02 = starepandas.read_granule(granule_name, sidecar=False, read_latlon=False, add_stare=False)

    ```
    Please see the examples/user_defined_granule_loader.ipynb in the starepandas
    distribution, i.e. https://github.com/SpatioTemporal/STAREPandas/tree/master/examples .

    """

    for regex, granule in granule_factory_library.items():
        if re.search(regex, file_path, re.IGNORECASE):
            if nom_res:
                return granule(file_path, sidecar_path, nom_res=nom_res)
            else:
                return granule(file_path, sidecar_path)
    raise UnsupportedFileError(file_path)


def read_granule(file_path,
                 latlon=False,
                 sidecar=False,
                 sidecar_path=None,
                 add_sids=False,
                 adapt_resolution=True,
                 xy=False,
                 nom_res=None,
                 read_timestamp=False,
                 keep_na_sids=False,
                 datasets=None,
                 roi=None,
                 **kwargs):
    """ Reads a granule into a STAREDataFrame

    Parameters
    -----------
    file_path: str
        path of the granule
    latlon: bool
        toggle whether to read the latitude and longitude variables
    sidecar: bool
        toggle whether to read the sidecar file
    sidecar_path: str
        path of the sidecar file. If not provided, it is assumed to be ${file_path}_stare.nc
    add_sids: bool
        toggle whether to lookup stare indices
    adapt_resolution: bool
        toggle whether to adapt the resolution
    xy: bool
        toggle wheather to add array coordinates to the dataframe.
    nom_res: str
        optional; for multi-resolution products, specify which resolution to read
    read_timestamp:
        toggle whether to read the timestamp
    keep_na_sids:
        toggle whether to keep rows containing NA values for sids

    Returns
    --------
    df: starepandas.STAREDataFrame
        A dataframe holding the granule data

    Examples
    ----------
    # >>> fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    # >>> modis = starepandas.read_granule(fname, latlon=True, sidecar=True, nom_res='5km')
    """

    granule = granule_factory(file_path, sidecar_path, nom_res)

    if add_sids:
        latlon = True
        sidecar = False

    if read_timestamp:
        granule.read_timestamps()

    if latlon:
        if sidecar:
            granule.read_sidecar_latlon()
        else:
            granule.read_latlon()

    if sidecar:
        granule.read_sidecar_index(sidecar_path)
    elif add_sids:
        granule.add_sids(adapt_resolution)

    granule.read_data()

    df = granule.to_df(xy=xy)

    return df


def to_zarr_s3(file_path, s3_path, level, chunk_size=250000, storage_options=None,
               dataset=None, data_level=None, raw_collected_time=None, metadata=None,
               sidecar_path=None, add_sids=True, adapt_resolution=True, read_timestamp=False,
               keep_na_sids=False, nom_res=None, scan=None, **kwargs):
    """
    Generic function to convert a granule file to STAREDataFrame and write it to S3 in zarr format.
    
    This function combines the functionality of read_granule() and STAREDataFrame.to_zarr_s3()
    to provide a convenient way to process granule files and store them in S3 with STARE indexing.
    
    Parameters
    ----------
    file_path : str
        Path to the granule file to process
    s3_path : str
        S3 path where the zarr root directory will be created (e.g., "s3://bucket/granule_name")
    level : int
        STARE level for partitioning SIDs
    chunk_size : int, optional
        Size of chunks for zarr arrays (default: 250000)
    storage_options : dict, optional
        S3 storage options including credentials and region
    dataset : str, optional
        Dataset name to record in metadata table
    data_level : str, optional
        Data level string to record in metadata table
    raw_collected_time : datetime, optional
        Timestamp when raw data was collected; defaults to UTC now if not provided
    metadata : dict, optional
        Additional metadata to store in the JSON field
    sidecar_path : str, optional
        Path to the sidecar file. If not provided, it is assumed to be ${file_path}_stare.nc
    add_sids : bool, optional
        Whether to add STARE indices to the dataframe (default: True)
    adapt_resolution : bool, optional
        Whether to adapt the resolution when adding SIDs (default: True)
    read_timestamp : bool, optional
        Whether to read timestamp data (default: False)
    keep_na_sids : bool, optional
        Whether to keep rows containing NA values for SIDs (default: False)
    nom_res : str, optional
        For multi-resolution products, specify which resolution to read
    scan : str, optional
        For granules that return multiple scans (e.g., SSMIS), specify which scan to process.
        If None and multiple scans are available, will process all scans separately.
    **kwargs : dict
        Additional keyword arguments passed to read_granule()
        
    Returns
    -------
    str or list
        The S3 path(s) where data was written. Returns a list of paths if multiple scans were processed.
        
    Examples
    --------
    >>> # Convert a MODIS granule to zarr and store in S3
    >>> s3_path = to_zarr_s3(
    ...     file_path="path/to/MOD05_L2.A2019336.0000.061.2019336211522.hdf",
    ...     s3_path="s3://my-bucket/modis_data",
    ...     level=10,
    ...     dataset="MOD05_L2",
    ...     data_level="L2"
    ... )
    
    >>> # Convert a VIIRS granule with custom metadata
    >>> s3_path = to_zarr_s3(
    ...     file_path="path/to/VNP02DNB.A2020219.0742.001.2020219125654.nc",
    ...     s3_path="s3://my-bucket/viirs_data",
    ...     level=12,
    ...     dataset="VNP02DNB",
    ...     data_level="L1B",
    ...     metadata={"satellite": "SNPP", "instrument": "VIIRS"}
    ... )
    
    >>> # Convert a specific SSMIS scan
    >>> s3_path = to_zarr_s3(
    ...     file_path="path/to/ssmis_file.h5",
    ...     s3_path="s3://my-bucket/ssmis_data",
    ...     level=8,
    ...     scan="S1",
    ...     dataset="SSMIS",
    ...     data_level="L1C"
    ... )
    """
    # Read the granule
    result = read_granule(
        file_path=file_path,
        sidecar_path=sidecar_path,
        add_sids=add_sids,
        adapt_resolution=adapt_resolution,
        read_timestamp=read_timestamp,
        keep_na_sids=keep_na_sids,
        nom_res=nom_res,
        **kwargs
    )
    
    # Share a single DB connection across all scans to avoid repeated connection setup
    from starepandas.staredataframe import _ensure_rds_db_and_table
    conn = _ensure_rds_db_and_table('StarePodsMetadata')

    try:
        # Handle different return types from read_granule
        if isinstance(result, dict):
            # Multiple scans (e.g., SSMIS)
            if scan is not None:
                # Process specific scan
                if scan not in result:
                    raise ValueError(f"Scan '{scan}' not found. Available scans: {list(result.keys())}")
                df = result[scan]
                scan_s3_path = f"{s3_path}_{scan}" if scan else s3_path
                single_meta = metadata.copy() if metadata else {}
                scan_key = f"{dataset}_{scan}" if dataset else None
                pw = SCAN_PIXEL_WIDTHS.get(scan_key) if scan_key else None
                if pw is not None:
                    single_meta['pixel_width'] = pw
                return df.to_zarr_s3(
                    s3_path=scan_s3_path,
                    level=level,
                    chunk_size=chunk_size,
                    storage_options=storage_options,
                    dataset=dataset,
                    data_level=data_level,
                    raw_collected_time=raw_collected_time,
                    metadata=single_meta,
                    conn=conn
                )
            else:
                # Process all scans
                s3_paths = []
                for scan_name, df in result.items():
                    # Append scan name to dataset name, not S3 path
                    scan_dataset = f"{dataset}_{scan_name}" if dataset else f"data_{scan_name}"
                    scan_metadata = metadata.copy() if metadata else {}
                    scan_metadata.update({"scan": scan_name})
                    pw = SCAN_PIXEL_WIDTHS.get(scan_dataset)
                    if pw is not None:
                        scan_metadata['pixel_width'] = pw

                    scan_result = df.to_zarr_s3(
                        s3_path=s3_path,  # Keep original S3 path
                        level=level,
                        chunk_size=chunk_size,
                        storage_options=storage_options,
                        dataset=scan_dataset,  # Use scan-specific dataset name
                        data_level=data_level,
                        raw_collected_time=raw_collected_time,
                        metadata=scan_metadata,
                        conn=conn
                    )
                    s3_paths.append(scan_result)
                return s3_paths
        else:
            # Single DataFrame (e.g., MODIS, VIIRS)
            return result.to_zarr_s3(
                 s3_path=s3_path,
                 level=level,
                 chunk_size=chunk_size,
                 storage_options=storage_options,
                 dataset=dataset,
                 data_level=data_level,
                 raw_collected_time=raw_collected_time,
                 metadata=metadata,
                 conn=conn
             )
    finally:
        try:
            if conn is not None and not conn.closed:
                conn.close()
        except Exception:
            pass


def load_zarr_metadata(dataset=None, data_level=None, s3_bucket=None, 
                      resolution_level=None, start_date=None, end_date=None,
                      grouped_id=None, limit=None, order_by=None):
    """
    Load metadata from the RDS database for zarr data stored in S3.
    
    This function queries the PodsMetadata table to retrieve information about
    zarr datasets that have been stored in S3 using the to_zarr_s3 function.
    
    Parameters
    ----------
    dataset : str, optional
        Filter by dataset name (e.g., "MOD05_L2", "VNP02DNB", "SSMIS")
    data_level : str, optional
        Filter by data level (e.g., "L1B", "L2", "L1C")
    s3_bucket : str, optional
        Filter by S3 bucket name
    resolution_level : int, optional
        Filter by STARE resolution level
    start_date : str or datetime, optional
        Filter by start date (inclusive). Can be string in format 'YYYY-MM-DD' or datetime object
    end_date : str or datetime, optional
        Filter by end date (inclusive). Can be string in format 'YYYY-MM-DD' or datetime object
    grouped_id : int, optional
        Filter by specific grouped_id
    limit : int, optional
        Limit the number of results returned
    order_by : str, optional
        Column to order results by (e.g., "RawData Collected Time", "Dataset", "grouped_id")
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing metadata with columns:
        - Dataset: Dataset name
        - DataLevel: Data level
        - RawData Collected Time: Timestamp when data was collected
        - grouped_id: STARE group ID
        - S3 bucket: S3 bucket name
        - Resolution level: STARE resolution level
        - MetadataJson: JSON metadata containing additional information
        - group_path: S3 path to the zarr group (from MetadataJson)
        - num_rows: Number of rows in the group (from MetadataJson)
        - columns: List of columns in the group (from MetadataJson)
        - scan: Scan name if applicable (from MetadataJson)
        
    Examples
    --------
    >>> # Load all metadata
    >>> df = load_zarr_metadata()
    
    >>> # Load metadata for specific dataset
    >>> df = load_zarr_metadata(dataset="MOD05_L2")
    
    >>> # Load metadata for specific date range
    >>> df = load_zarr_metadata(
    ...     start_date="2023-01-01",
    ...     end_date="2023-12-31"
    ... )
    
    >>> # Load metadata for specific S3 bucket and resolution
    >>> df = load_zarr_metadata(
    ...     s3_bucket="my-data-bucket",
    ...     resolution_level=10
    ... )
    
    >>> # Load metadata with custom ordering and limit
    >>> df = load_zarr_metadata(
    ...     dataset="SSMIS",
    ...     order_by="RawData Collected Time",
    ...     limit=100
    ... )
    """
    # Import the RDS connection function from staredataframe
    from starepandas.staredataframe import _ensure_rds_db_and_table
    
    try:
        # Get database connection
        conn = _ensure_rds_db_and_table('StarePodsMetadata')
        
        # Build the SQL query
        query = 'SELECT * FROM "PodsMetadata"'
        conditions = []
        params = []
        
        # Add filters
        if dataset is not None:
            conditions.append('"Dataset" = %s')
            params.append(dataset)
            
        if data_level is not None:
            conditions.append('"DataLevel" = %s')
            params.append(data_level)
            
        if s3_bucket is not None:
            conditions.append('"S3 bucket" = %s')
            params.append(s3_bucket)
            
        if resolution_level is not None:
            conditions.append('"Resolution level" = %s')
            params.append(resolution_level)
            
        if grouped_id is not None:
            conditions.append('grouped_id = %s')
            params.append(grouped_id)
            
        if start_date is not None:
            conditions.append('"RawData Collected Time" >= %s')
            params.append(start_date)
            
        if end_date is not None:
            conditions.append('"RawData Collected Time" <= %s')
            params.append(end_date)
        
        # Add WHERE clause if conditions exist
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        # Add ORDER BY clause
        if order_by is not None:
            query += f' ORDER BY "{order_by}"'
        else:
            query += ' ORDER BY "RawData Collected Time" DESC'
        
        # Add LIMIT clause
        if limit is not None:
            query += f' LIMIT {limit}'
        
        # Execute query
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # Parse JSON metadata and expand into separate columns
        if not df.empty and 'MetadataJson' in df.columns:
            # Parse JSON metadata
            metadata_list = []
            for json_str in df['MetadataJson']:
                if json_str:
                    try:
                        if isinstance(json_str, str):
                            metadata_list.append(json.loads(json_str))
                        else:
                            # Already parsed JSON object
                            metadata_list.append(json_str)
                    except (json.JSONDecodeError, TypeError):
                        metadata_list.append({})
                else:
                    metadata_list.append({})
            
            # Create DataFrame from metadata
            metadata_df = pd.DataFrame(metadata_list)
            
            # Add metadata columns to main DataFrame
            for col in metadata_df.columns:
                if col not in df.columns:
                    df[col] = metadata_df[col]
        
        # Close connection
        conn.close()
        
        return df
        
    except Exception as e:
        # Try to close connection if it exists
        try:
            if 'conn' in locals():
                conn.close()
        except:
            pass
        raise RuntimeError(f"Error loading zarr metadata from database: {e}")


def get_zarr_summary(dataset=None, data_level=None, s3_bucket=None):
    """
    Get a summary of zarr metadata stored in the database.
    
    Parameters
    ----------
    dataset : str, optional
        Filter by dataset name
    data_level : str, optional
        Filter by data level
    s3_bucket : str, optional
        Filter by S3 bucket name
        
    Returns
    -------
    pandas.DataFrame
        Summary DataFrame with columns:
        - Dataset: Dataset name
        - DataLevel: Data level
        - S3 bucket: S3 bucket name
        - Resolution level: STARE resolution level
        - count: Number of groups
        - total_rows: Total number of rows across all groups (if available)
        - date_range: Date range of the data
        - latest_date: Most recent data collection date
        
    Examples
    --------
    >>> # Get summary of all data
    >>> summary = get_zarr_summary()
    
    >>> # Get summary for specific dataset
    >>> summary = get_zarr_summary(dataset="MOD05_L2")
    """
    # Load metadata
    df = load_zarr_metadata(dataset=dataset, data_level=data_level, s3_bucket=s3_bucket)
    
    if df.empty:
        return pd.DataFrame()
    
    # Group by key fields and aggregate
    agg_dict = {
        'grouped_id': 'count',
        'RawData Collected Time': ['min', 'max']
    }
    
    # Add num_rows aggregation if the column exists (from parsed JSON metadata)
    if 'num_rows' in df.columns:
        agg_dict['num_rows'] = 'sum'
    
    summary = df.groupby(['Dataset', 'DataLevel', 'S3 bucket', 'Resolution level']).agg(agg_dict).reset_index()
    
    # Flatten column names
    if 'num_rows' in df.columns:
        summary.columns = ['Dataset', 'DataLevel', 'S3 bucket', 'Resolution level', 
                          'count', 'total_rows', 'earliest_date', 'latest_date']
    else:
        summary.columns = ['Dataset', 'DataLevel', 'S3 bucket', 'Resolution level', 
                          'count', 'earliest_date', 'latest_date']
        # Add total_rows column with NaN values if num_rows not available
        summary['total_rows'] = pd.NA
    
    # Create date range string
    summary['date_range'] = summary['earliest_date'].astype(str) + ' to ' + summary['latest_date'].astype(str)
    
    # Sort by latest date
    summary = summary.sort_values('latest_date', ascending=False)
    
    return summary


def from_zarr_s3_chunked(s3_path, storage_options=None):
    """
    Read STAREDataFrame from S3 chunked zarr store (alternative format to grouped zarr).
    
    This function reads zarr data stored as a single chunked group, which is different
    from the grouped format expected by STAREDataFrame.from_zarr_s3().
    
    Parameters
    ----------
    s3_path : str
        S3 path to the zarr root directory containing chunked arrays
    storage_options : dict, optional
        S3 storage options including credentials and region
        
    Returns
    -------
    STAREDataFrame
        The reconstructed STAREDataFrame
        
    Examples
    --------
    >>> # Read chunked zarr data from S3
    >>> df = from_zarr_s3_chunked('s3://my-bucket/granule_data/')
    
    >>> # With custom storage options
    >>> df = from_zarr_s3_chunked(
    ...     's3://my-bucket/granule_data/',
    ...     storage_options={'key': '...', 'secret': '...', 'client_kwargs': {'region_name': 'us-west-2'}}
    ... )
    """
    import zarr
    import numpy as np
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS, _load_config_from_default_locations
    
    # Resolve storage options
    merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
    if not merged_opts:
        _load_config_from_default_locations()
        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
    if storage_options:
        merged_opts.update(storage_options)
    if not merged_opts:
        raise ValueError(
            "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
            "to set credentials/region, or pass storage_options to from_zarr_s3_chunked."
        )
    
    # Open zarr group
    zg = zarr.open_group(s3_path, mode="r", storage_options=merged_opts)
    
    # Read all arrays
    data = {}
    for key in zg.array_keys():
        arr = zg[key][:]
        if arr.dtype.kind == 'U':
            arr = arr.astype('O')
        data[key] = arr
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Convert to STAREDataFrame
    from starepandas import STAREDataFrame
    return STAREDataFrame(df)


def from_zarr_s3_chunked_groups(s3_path, group_sid_ids, storage_options=None):
    """
    Read specific STARE group SIDs from S3 chunked zarr store.
    
    This function efficiently reads only the data for specified STARE group SIDs
    from a chunked zarr store, avoiding the need to load the entire dataset.
    
    The function supports two approaches:
    1. If group_sid_ids are individual SIDs, it filters the chunked data
    2. If group_sid_ids are group directory names, it reads from those directories
    
    Parameters
    ----------
    s3_path : str
        S3 path to the zarr root directory containing chunked arrays
    group_sid_ids : list
        List of STARE group SID IDs to read (e.g., [3447505514752114692, 3445253714938429444])
    storage_options : dict, optional
        S3 storage options including credentials and region
        
    Returns
    -------
    STAREDataFrame
        The reconstructed STAREDataFrame containing only the specified groups
        
    Examples
    --------
    >>> # Read specific groups from chunked zarr data
    >>> group_ids = [3447505514752114692, 3445253714938429444]
    >>> df = from_zarr_s3_chunked_groups('s3://my-bucket/granule_data/', group_ids)
    
    >>> # With custom storage options
    >>> df = from_zarr_s3_chunked_groups(
    ...     's3://my-bucket/granule_data/',
    ...     group_ids,
    ...     storage_options={'key': '...', 'secret': '...', 'client_kwargs': {'region_name': 'us-west-2'}}
    ... )
    """
    import zarr
    import numpy as np
    import s3fs
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS, _load_config_from_default_locations
    
    # Validate input
    if not isinstance(group_sid_ids, (list, tuple, np.ndarray)):
        raise ValueError("group_sid_ids must be a list, tuple, or numpy array")
    
    if len(group_sid_ids) == 0:
        raise ValueError("group_sid_ids cannot be empty")
    
    # Convert to numpy array for efficient operations
    group_sid_ids = np.array(group_sid_ids, dtype=np.int64)
    
    # Resolve storage options
    merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
    if not merged_opts:
        _load_config_from_default_locations()
        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
    if storage_options:
        merged_opts.update(storage_options)
    if not merged_opts:
        raise ValueError(
            "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
            "to set credentials/region, or pass storage_options to from_zarr_s3_chunked_groups."
        )
    
    # First, try to read from group directories (more efficient)
    fs = s3fs.S3FileSystem(**merged_opts)
    all_data = []
    
    for group_id in group_sid_ids:
        group_path = s3_path + str(group_id)
        
        # Check if group directory exists
        if fs.exists(group_path):
            print(f"Reading from group directory: {group_id}")
            
            # Read arrays from this group directory
            group_data = {}
            try:
                # List contents of group directory
                contents = fs.ls(group_path)
                
                # Read each array
                for item in contents:
                    array_name = item.split('/')[-1]
                    # Check if it's a zarr array (either .zarray file or zarr.json with chunks)
                    if fs.exists(item + '/.zarray') or fs.exists(item + '/zarr.json'):
                        try:
                            # Use the S3 filesystem directly for zarr arrays
                            store = fs.get_mapper(item)
                            arr = zarr.open_array(store, mode='r')
                            group_data[array_name] = arr[:]
                        except Exception as e:
                            print(f"Warning: Could not read array {array_name}: {e}")
                
                if group_data:
                    # Create DataFrame for this group
                    group_df = pd.DataFrame(group_data)
                    all_data.append(group_df)
                    print(f"  - Read {len(group_df)} rows from group {group_id}")
                else:
                    print(f"  - No data found in group {group_id}")
                    
            except Exception as e:
                print(f"Warning: Could not read group {group_id}: {e}")
        else:
            print(f"Group directory not found: {group_id}")
    
    # If we found data in group directories, combine and return
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        from starepandas import STAREDataFrame
        return STAREDataFrame(df)
    
    # Fallback: try to filter the chunked data by SIDs
    print("Group directories not found or empty. Trying to filter chunked data by SIDs...")
    
    try:
        # Open zarr group
        zg = zarr.open_group(s3_path, mode="r", storage_options=merged_opts)
        
        # Check if sids array exists
        if 'sids' not in zg.array_keys():
            raise ValueError("No 'sids' array found in zarr store. Cannot filter by group SIDs.")
        
        # Read the sids array to find matching indices
        sids_array = zg['sids'][:]
        
        # Find indices where sids match any of the requested group SIDs
        mask = np.isin(sids_array, group_sid_ids)
        
        if not np.any(mask):
            print(f"Warning: No data found for the specified group SIDs: {group_sid_ids}")
            # Return empty DataFrame with correct structure
            from starepandas import STAREDataFrame
            return STAREDataFrame()
        
        # Get the indices of matching rows
        matching_indices = np.where(mask)[0]
        
        print(f"Found {len(matching_indices)} rows matching {len(group_sid_ids)} group SIDs")
        
        # Read only the matching rows from each array
        data = {}
        for key in zg.array_keys():
            arr = zg[key][matching_indices]
            if arr.dtype.kind == 'U':
                arr = arr.astype('O')
            data[key] = arr
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert to STAREDataFrame
        from starepandas import STAREDataFrame
        return STAREDataFrame(df)
        
    except Exception as e:
        print(f"Error filtering chunked data: {e}")
        from starepandas import STAREDataFrame
        return STAREDataFrame()


def generate_zarr_path(sid, dataset_name):
    """
    Generate relative path for storing zarr file based on STARE SID structure.
    
    This is a convenience function that creates a temporary STAREDataFrame instance
    and calls its generate_zarr_path method.
    
    Parameters
    ----------
    sid : int
        8-byte STARE SID integer
    dataset_name : str
        Name of the dataset
        
    Returns
    -------
    str
        Relative path for storing zarr file in format Q00_X/Q01_Y/.../QN_M/DatasetName
        
    Examples
    --------
    >>> from starepandas.io.granules import generate_zarr_path
    >>> path = generate_zarr_path(3445253714938429444, "MOD09")
    >>> print(path)
    Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09
    
    See Also
    --------
    STAREDataFrame.generate_zarr_path : The underlying method
    parse_zarr_path : Reverse operation to parse paths back to SIDs
    to_zarr_s3 : Generic zarr writing function
    """
    from starepandas import STAREDataFrame
    sdf = STAREDataFrame()
    return sdf.generate_zarr_path(sid, dataset_name)


def parse_zarr_path(zarr_path):
    """
    Parse hierarchical zarr path and reconstruct STARE SID from path components.
    
    This is a convenience function that creates a temporary STAREDataFrame instance
    and calls its parse_zarr_path method. This is the reverse operation of generate_zarr_path.
    
    Parameters
    ----------
    zarr_path : str
        Hierarchical path in format Q00_X/Q01_Y/.../QN_M/DatasetName
        
    Returns
    -------
    tuple
        (sid, dataset_name) where sid is the reconstructed STARE SID integer
        and dataset_name is the extracted dataset name
        
    Examples
    --------
    >>> from starepandas.io.granules import parse_zarr_path
    >>> sid, dataset = parse_zarr_path("Q00_5/Q01_3/Q02_3/Q03_2/Q04_2/MOD09")
    >>> print(f"SID: {sid}, Dataset: {dataset}")
    SID: 3445253714938429444, Dataset: MOD09
    
    See Also
    --------
    STAREDataFrame.parse_zarr_path : The underlying method
    generate_zarr_path : Reverse operation to generate paths from SIDs
    from_zarr_s3_chunked : Read chunked zarr from S3
    """
    from starepandas import STAREDataFrame
    sdf = STAREDataFrame()
    return sdf.parse_zarr_path(zarr_path)


def reconstruct_hdf5_from_zarr(
    zarr_path, dataset, output_hdf5_path,
    area_sids=None, bbox=None,
    storage_options=None, pixel_width=None,
    compression='gzip', compression_opts=4,
):
    """
    Reconstruct an HDF5 granule from zarr chunks stored on S3 or local disk.

    Reads only the zarr groups whose STARE partition SIDs intersect the
    requested area, concatenates them into a STAREDataFrame, then calls
    ``STAREDataFrame.to_hdf5()`` to write the original granule structure.

    Parameters
    ----------
    zarr_path : str
        Root zarr path.  Use ``"s3://bucket/prefix"`` for S3 or a local
        directory path for local storage.
    dataset : str
        Dataset / scan identifier used when the data were written
        (e.g. ``"GMI_S1"``).  Used to:

        * Locate the correct zarr groups via ``generate_zarr_path``.
        * Derive the HDF5 scan group name (``"S1"`` extracted from
          ``"GMI_S1"``).
        * Look up ``pixel_width`` from ``SCAN_PIXEL_WIDTHS`` when not
          provided explicitly.
    output_hdf5_path : str
        Destination HDF5 file path.
    area_sids : array-like of int, optional
        STARE SIDs covering the area of interest.  Exactly one of
        ``area_sids`` or ``bbox`` must be provided.
    bbox : tuple of float, optional
        Bounding box ``(lon_min, lat_min, lon_max, lat_max)``.  Converted
        internally to ``area_sids`` via ``pystare.cover_from_hull``.
    storage_options : dict, optional
        Passed to ``zarr.open_group`` for S3 authentication / configuration.
        If ``None`` and ``zarr_path`` starts with ``"s3://"``, the built-in
        ``_AWS_S3_STORAGE_OPTIONS`` are used.
    pixel_width : int, optional
        Explicit pixel_width override.  When ``None``, the function looks
        first in the zarr group attributes then in ``SCAN_PIXEL_WIDTHS``.
    compression : str, optional
        HDF5 compression filter (default ``'gzip'``).
    compression_opts : int, optional
        Compression level (default ``4``).

    Returns
    -------
    str
        ``output_hdf5_path``

    Raises
    ------
    ValueError
        If neither or both of ``area_sids`` / ``bbox`` are provided, if no
        matching zarr groups are found, or if ``pixel_width`` cannot be
        determined.
    """
    import zarr
    import pystare
    from starepandas import STAREDataFrame
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS, MAX_PARTITION_LEVEL

    # ── Validate inputs ──────────────────────────────────────────────────────
    if (area_sids is None) == (bbox is None):
        raise ValueError(
            "Provide exactly one of 'area_sids' or 'bbox', not both or neither."
        )

    # ── Build query SIDs ─────────────────────────────────────────────────────
    if bbox is not None:
        lon_min, lat_min, lon_max, lat_max = bbox
        lats = [lat_min, lat_min, lat_max, lat_max]
        lons = [lon_min, lon_max, lon_max, lon_min]
        area_sids = pystare.cover_from_hull(lats, lons, MAX_PARTITION_LEVEL)

    coerced = pystare.spatial_coerce_resolution(
        np.array(area_sids, dtype=np.int64), MAX_PARTITION_LEVEL
    )
    query_group_ids = set(int(s) for s in np.unique(coerced))

    # ── Collect matching zarr group paths ─────────────────────────────────────
    is_s3 = zarr_path.startswith('s3://')
    merged_opts = storage_options or (_AWS_S3_STORAGE_OPTIONS if is_s3 else {})

    group_paths = []       # (group_path, group_id) tuples
    zarr_pixel_width = None  # pixel_width read from zarr attrs

    if is_s3:
        from starepandas.io.granules import load_zarr_metadata
        meta_df = load_zarr_metadata(dataset=dataset)
        if meta_df is not None and not meta_df.empty:
            meta_df['grouped_id'] = meta_df['grouped_id'].astype(np.int64)

            # Detect the actual STARE level used for grouped_id in this dataset.
            # It may differ from MAX_PARTITION_LEVEL when data was ingested with a
            # different partitioning level.  Lower 5 bits of a STARE SID encode level.
            storage_levels = set(int(gid & 0x1f) for gid in meta_df['grouped_id'].dropna())
            if storage_levels == {MAX_PARTITION_LEVEL}:
                effective_query_ids = query_group_ids  # fast path: levels already match
            else:
                # Re-coerce the query area to each unique storage level so the
                # set-membership filter works regardless of how data was ingested.
                effective_query_ids: set = set()
                for slevel in storage_levels:
                    if bbox is not None:
                        lon_min_q, lat_min_q, lon_max_q, lat_max_q = bbox
                        lats_q = [lat_min_q, lat_min_q, lat_max_q, lat_max_q]
                        lons_q = [lon_min_q, lon_max_q, lon_max_q, lon_min_q]
                        sids_q = pystare.cover_from_hull(lats_q, lons_q, slevel)
                    else:
                        sids_q = area_sids
                    coerced_q = pystare.spatial_coerce_resolution(
                        np.array(sids_q, dtype=np.int64), slevel
                    )
                    effective_query_ids.update(int(s) for s in np.unique(coerced_q))

            matching = meta_df[meta_df['grouped_id'].isin(effective_query_ids)]
            for _, row in matching.iterrows():
                group_paths.append((row['group_path'], int(row['grouped_id'])))
        else:
            # Fallback: construct paths directly from query SIDs
            dummy = STAREDataFrame()
            for gid in query_group_ids:
                rel = dummy.generate_zarr_path(gid, dataset)
                group_paths.append((f"{zarr_path}/{rel}", gid))
    else:
        # Local: flat layout — each sub-directory name is the integer group SID
        if not os.path.isdir(zarr_path):
            raise ValueError(f"Local zarr path does not exist: {zarr_path}")
        for entry in os.listdir(zarr_path):
            entry_path = os.path.join(zarr_path, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                gid = int(entry)
            except ValueError:
                continue
            if gid in query_group_ids:
                group_paths.append((entry_path, gid))

    if not group_paths:
        raise ValueError(
            f"No zarr groups found for dataset '{dataset}' intersecting the "
            f"requested area.  Queried {len(query_group_ids)} partition SIDs."
        )

    # ── Read matching groups ──────────────────────────────────────────────────
    frames = []
    for gpath, _gid in group_paths:
        try:
            zg = zarr.open_group(gpath, mode='r', storage_options=merged_opts if is_s3 else {})
        except Exception as e:
            logging.warning("Skipping zarr group %s — could not open: %s", gpath, e)
            continue

        # Harvest pixel_width from zarr attrs (first group wins)
        if zarr_pixel_width is None and 'pixel_width' in zg.attrs:
            zarr_pixel_width = int(zg.attrs['pixel_width'])

        # Read all per-column arrays (skip the hidden helper array)
        col_data = {}
        for key in zg.array_keys():
            if key == '__row_positions__':
                continue
            col_data[key] = zg[key][:]

        if not col_data:
            continue

        row_positions = zg['__row_positions__'][:] if '__row_positions__' in zg else None
        df_chunk = pd.DataFrame(col_data)
        if row_positions is not None:
            df_chunk['__row_positions__'] = row_positions
        frames.append(df_chunk)

    if not frames:
        raise ValueError(
            f"All matched zarr groups were empty for dataset '{dataset}'."
        )

    # ── Concatenate and sort by original row order ────────────────────────────
    combined = pd.concat(frames, ignore_index=True)
    if '__row_positions__' in combined.columns:
        combined = combined.sort_values('__row_positions__').drop(
            columns=['__row_positions__']
        ).reset_index(drop=True)

    sdf = STAREDataFrame(combined)

    # ── Resolve pixel_width ───────────────────────────────────────────────────
    if pixel_width is not None:
        resolved_pw = pixel_width
        logging.debug("pixel_width=%d provided explicitly for dataset '%s'", resolved_pw, dataset)
    elif zarr_pixel_width is not None:
        resolved_pw = zarr_pixel_width
        logging.debug("pixel_width=%d read from zarr group attrs for dataset '%s'", resolved_pw, dataset)
    else:
        resolved_pw = SCAN_PIXEL_WIDTHS.get(dataset)
        if resolved_pw is not None:
            logging.debug("pixel_width=%d from SCAN_PIXEL_WIDTHS for dataset '%s'", resolved_pw, dataset)

    if resolved_pw is None:
        raise ValueError(
            f"Cannot determine pixel_width for dataset '{dataset}'. "
            "Pass pixel_width explicitly or ensure it was stored in zarr group attrs."
        )

    # ── Derive scan group name from dataset string ────────────────────────────
    m = re.search(r'_(S\d+)$', dataset)
    scan = m.group(1) if m else None
    if scan is None:
        raise ValueError(
            f"Cannot derive scan group name from dataset '{dataset}'. "
            "Expected a suffix like '_S1'.  Pass the dataset in 'INSTRUMENT_Sn' format."
        )

    # ── Write HDF5 ───────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(os.path.abspath(output_hdf5_path)), exist_ok=True)
    sdf.to_hdf5(
        output_hdf5_path,
        scan=scan,
        pixel_width=resolved_pw,
        compression=compression,
        compression_opts=compression_opts,
    )
    return output_hdf5_path
