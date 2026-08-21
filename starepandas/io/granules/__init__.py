import datetime
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


def to_s3(file_path, s3_path=None, level=10, chunk_size=250000, storage_options=None,
               dataset=None, data_level=None, raw_collected_time=None, metadata=None,
               sidecar_path=None, add_sids=True, adapt_resolution=True, read_timestamp=False,
               keep_na_sids=False, nom_res=None, scan=None, **kwargs):
    """
    Generic function to convert a granule file to STAREDataFrame and write it to S3 as Parquet partitions.
    
    This function combines the functionality of read_granule() and STAREDataFrame.to_s3()
    to provide a convenient way to process granule files and store them in S3 with STARE indexing.
    
    Parameters
    ----------
    file_path : str
        Path to the granule file to process
    s3_path : str
        S3 path where the storage root will be created (e.g., "s3://bucket/granule_name")
    level : int
        STARE level for partitioning SIDs
    chunk_size : int, optional
        Unused; retained for API compatibility (default: 250000)
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
    >>> # Convert a MODIS granule to Parquet partitions in S3
    >>> s3_path = to_s3(
    ...     file_path="path/to/MOD05_L2.A2019336.0000.061.2019336211522.hdf",
    ...     s3_path="s3://my-bucket/modis_data",
    ...     level=10,
    ...     dataset="MOD05_L2",
    ...     data_level="L2"
    ... )
    
    >>> # Convert a VIIRS granule with custom metadata
    >>> s3_path = to_s3(
    ...     file_path="path/to/VNP02DNB.A2020219.0742.001.2020219125654.nc",
    ...     s3_path="s3://my-bucket/viirs_data",
    ...     level=12,
    ...     dataset="VNP02DNB",
    ...     data_level="L1B",
    ...     metadata={"satellite": "SNPP", "instrument": "VIIRS"}
    ... )
    
    >>> # Convert a specific SSMIS scan
    >>> s3_path = to_s3(
    ...     file_path="path/to/ssmis_file.h5",
    ...     s3_path="s3://my-bucket/ssmis_data",
    ...     level=8,
    ...     scan="S1",
    ...     dataset="SSMIS",
    ...     data_level="L1C"
    ... )
    """
    # §C10 #2 fix: derive a deterministic per-granule timestamp from the
    # filename when the caller didn't supply one. Required so retries
    # (SQS visibility-timeout redelivery) produce the same row identity
    # and the §C10 #1 UNIQUE constraint can actually dedup.
    if raw_collected_time is None:
        from starepandas.io.granules._timestamps import derive_timestamp_from_path
        raw_collected_time = derive_timestamp_from_path(file_path)

    # Task 12: derive granule basename to splice into the partition path so
    # the S3 layout matches to_local — HTM tree → granule → dataset leaf.
    granule_basename = os.path.splitext(os.path.basename(file_path))[0]

    # Task 12: fall back to .config's default_s3_prefix when caller didn't
    # provide one. Loader populates _DEFAULT_S3_PREFIX during config load.
    if s3_path is None:
        from starepandas.staredataframe import (
            _DEFAULT_S3_PREFIX, _load_config_from_default_locations,
        )
        if not _DEFAULT_S3_PREFIX:
            _load_config_from_default_locations()
        from starepandas.staredataframe import _DEFAULT_S3_PREFIX as _resolved
        if not _resolved:
            raise ValueError(
                "s3_path not provided and no default_s3_prefix is set in "
                ".config. Either pass s3_path explicitly or add a line "
                "'default_s3_prefix=s3://your-bucket/storage' to .config."
            )
        s3_path = _resolved

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
                return df.to_s3(
                    s3_path=scan_s3_path,
                    level=level,
                    chunk_size=chunk_size,
                    storage_options=storage_options,
                    dataset=dataset,
                    data_level=data_level,
                    raw_collected_time=raw_collected_time,
                    metadata=single_meta,
                    conn=conn,
                    granule_name=granule_basename,
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

                    scan_result = df.to_s3(
                        s3_path=s3_path,  # Keep original S3 path
                        level=level,
                        chunk_size=chunk_size,
                        storage_options=storage_options,
                        dataset=scan_dataset,  # Use scan-specific dataset name
                        data_level=data_level,
                        raw_collected_time=raw_collected_time,
                        metadata=scan_metadata,
                        conn=conn,
                        granule_name=granule_basename,
                    )
                    s3_paths.append(scan_result)
                return s3_paths
        else:
            # Single DataFrame (e.g., MODIS, VIIRS)
            return result.to_s3(
                 s3_path=s3_path,
                 level=level,
                 chunk_size=chunk_size,
                 storage_options=storage_options,
                 dataset=dataset,
                 data_level=data_level,
                 raw_collected_time=raw_collected_time,
                 metadata=metadata,
                 conn=conn,
                 granule_name=granule_basename,
             )
    finally:
        try:
            if conn is not None and not conn.closed:
                conn.close()
        except Exception:
            pass


#: Upper bound on one chunk's temporal range. A chunk holds data from a
#: single granule pass (≈ one orbit ≈ 100 min), so 2 h is a safe ceiling.
#: Used to rewrite the period-overlap predicate into an index-friendly
#: ``t_start`` range (ADR-0002 Decision 3): because ``t_end ≤ t_start +
#: D_MAX``, every chunk overlapping ``[period_start, period_end]`` has
#: ``t_start BETWEEN period_start − D_MAX AND period_end`` — that range
#: prunes via the ``(t_start, t_end)`` index, and the residual exact
#: ``t_end ≥ period_start`` test filters the pruned slice. A bare
#: ``t_end ≥ …`` half cannot use the index and degrades to full scans as
#: the catalog grows.
D_MAX = datetime.timedelta(hours=2)


def _validate_period(period):
    """Normalize a ``(period_start, period_end)`` pair to naive-UTC datetimes.

    The catalog stores ``t_start``/``t_end`` as naive UTC, so tz-aware bounds
    are converted to UTC and stripped of their offset. Rejects missing bounds
    (``None``/``NaT`` — the period must be closed on both ends) and a
    reversed period. Returns ``(period_start, period_end)`` as
    ``datetime.datetime``.
    """
    period_start, period_end = period
    bounds = []
    for name, bound in (('start', period_start), ('end', period_end)):
        ts = pd.Timestamp(bound)
        if pd.isna(ts):
            raise ValueError(
                f"period {name} is missing ({bound!r}) — a period must be "
                "closed on both ends"
            )
        if ts.tzinfo is not None:
            # Catalog timestamps are naive UTC.
            ts = ts.tz_convert('UTC').tz_localize(None)
        bounds.append(ts.to_pydatetime())
    period_start, period_end = bounds
    if period_end < period_start:
        raise ValueError(
            f"period end ({period_end}) precedes period start ({period_start})"
        )
    return period_start, period_end


def _period_conditions(period, placeholder='%s', as_iso=False):
    """SQL conditions + params for "chunk temporal range overlaps period".

    The exact predicate is ``t_start ≤ period_end AND t_end ≥ period_start``;
    this returns its ADR-0002 Decision-3 index-friendly rewrite (see
    :data:`D_MAX`). Chunks with a null temporal range (ingested without
    per-point timestamps) never match a period.

    Parameters
    ----------
    period : tuple
        ``(period_start, period_end)`` — each a datetime, pandas Timestamp,
        or ISO-8601 / ``'YYYY-MM-DD'`` string; inclusive on both ends.
        tz-aware bounds are converted to the catalog's naive-UTC convention.
    placeholder : str
        SQL parameter placeholder — ``'%s'`` (psycopg2) or ``'?'`` (sqlite3).
    as_iso : bool
        Emit params as ISO-8601 strings for SQLite, where ``t_start`` /
        ``t_end`` are stored as ISO TEXT and compared lexicographically.

    Returns
    -------
    tuple of (list of str, list)
        Condition fragments (to AND into a WHERE clause) and their params.
    """
    period_start, period_end = _validate_period(period)

    conditions = [
        f't_start >= {placeholder}',
        f't_start <= {placeholder}',
        f't_end >= {placeholder}',
    ]
    params = [period_start - D_MAX, period_end, period_start]
    if as_iso:
        params = [p.isoformat() for p in params]
    return conditions, params


def _period_mask(df, period):
    """Client-side boolean mask for "chunk temporal range overlaps period".

    The exact closed-overlap predicate (``t_start ≤ period_end AND
    t_end ≥ period_start``) over parsed ``t_start``/``t_end`` columns — the
    in-memory counterpart of :func:`_period_conditions`' SQL rewrite, kept
    beside it so the two encodings of the overlap semantics cannot drift.
    Rows with a null range never match.
    """
    period_start, period_end = _validate_period(period)
    return (df['t_start'] <= period_end) & (df['t_end'] >= period_start)


def _require_podcodes(catalog):
    """Raise when a temporal-catalog frame carries rows with a null podcode.

    Shared contract of the pure pod-keyed functions (:func:`vcf_rollup`, the
    overlap analytics): rows without a pod code (pre-temporal rows from an
    in-place schema upgrade) cannot participate in pod-keyed analytics, and
    dropping them silently would undercount — so they raise instead (the
    thin loaders exclude them in SQL).
    """
    if catalog['podcode'].isna().any():
        raise ValueError(
            "catalog contains rows with a null podcode (pre-temporal rows "
            "from an in-place schema upgrade?) — re-ingest them or filter "
            "them out first; dropping them silently would undercount"
        )


def _podcode_prefix_condition(podcode_prefix, placeholder='%s'):
    """SQL condition + param for "chunk lies in the pod subtree" (both backends).

    Validates ``podcode_prefix`` against the pod-code grammar via the codec
    (:func:`starepandas.staredataframe.podcode_to_sid`, raising ``ValueError``
    on anything malformed) — the grammar contains no SQL wildcard characters,
    so the prefix is safe inside a ``LIKE`` pattern. The emitted
    ``podcode LIKE '<prefix>%'`` rides the ``idx_pods_podcode`` index.

    Returns
    -------
    tuple of (list of str, list)
        Condition fragments (to AND into a WHERE clause) and their params.
    """
    from starepandas.staredataframe import podcode_to_sid

    podcode_to_sid(podcode_prefix)
    return [f'podcode LIKE {placeholder}'], [f'{podcode_prefix}%']


#: SQL that extracts a chunk's storage path out of ``MetadataJson``. The path
#: is not a column of its own — it lives in the JSON blob — so each backend
#: needs its own accessor (Postgres jsonb ``->>`` vs SQLite JSON1).
_GROUP_PATH_SQL = {
    'postgres': '"MetadataJson"->>\'group_path\'',
    'sqlite': 'json_extract("MetadataJson", \'$.group_path\')',
}


def _path_prefix_condition(path_prefix, backend, placeholder='%s'):
    """SQL condition + param for "chunk was written under this storage root".

    One RDS catalog is shared by every ingest, so ``dataset``/``period``
    filters alone cannot tell one job's chunks from another's — two ingests of
    the same instrument covering the same hours are indistinguishable by those
    columns. The storage root can: it is the prefix a given ingest wrote to
    (e.g. ``"s3://zarrpods/gmi-demo-parquet"`` vs
    ``"s3://zarrpods/testing-s3/loadtest-jan"``). Works for local paths too,
    where ``group_path`` is the on-disk chunk path.

    Note the prefix is matched against a JSON field, so no index serves it —
    combine with ``period``/``dataset`` (which do) rather than relying on this
    alone to keep a scan small.

    Parameters
    ----------
    path_prefix : str
        Storage root; matched as a literal prefix of ``group_path``. A
        trailing separator is not required.
    backend : str
        ``'postgres'`` or ``'sqlite'`` — selects the JSON accessor.
    placeholder : str
        SQL parameter placeholder — ``'%s'`` (psycopg2) or ``'?'`` (sqlite3).

    Returns
    -------
    tuple of (list of str, list)
        Condition fragments (to AND into a WHERE clause) and their params.

    Raises
    ------
    ValueError
        If ``path_prefix`` is not a non-empty string, or ``backend`` is
        unknown.
    """
    if not isinstance(path_prefix, str) or not path_prefix:
        raise ValueError(f"path_prefix must be a non-empty string, got {path_prefix!r}")
    if backend not in _GROUP_PATH_SQL:
        raise ValueError(f"backend must be one of {sorted(_GROUP_PATH_SQL)}, "
                         f"got {backend!r}")

    # Unlike a pod code, a storage path may legitimately contain LIKE
    # wildcards ('_' is common in bucket and prefix names), so escape them
    # rather than trusting the grammar.
    escaped = (path_prefix.replace('\\', r'\\')
                          .replace('%', r'\%')
                          .replace('_', r'\_'))
    condition = f"{_GROUP_PATH_SQL[backend]} LIKE {placeholder} ESCAPE '\\'"
    return [condition], [f'{escaped}%']


def load_s3_metadata(dataset=None, dataset_prefix=None, data_level=None, s3_bucket=None,
                      resolution_level=None, start_date=None, end_date=None,
                      grouped_id=None, period=None, path_prefix=None, limit=None,
                      order_by=None):
    """
    Load metadata from the RDS database for Parquet partitions stored in S3.
    
    This function queries the PodsMetadata table to retrieve information about
    Parquet datasets that have been stored in S3 using the to_s3 function.
    
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
        **Granule-level** date filter (inclusive lower bound) on
        "RawData Collected Time" — the single collection time derived from
        the granule *filename*. Distinct from ``period``, which filters on
        the data-level temporal range. Can be 'YYYY-MM-DD' or datetime.
    end_date : str or datetime, optional
        Granule-level date filter (inclusive upper bound) on
        "RawData Collected Time"; see ``start_date``.
    grouped_id : int, optional
        Filter by specific grouped_id
    period : tuple of (str or datetime, str or datetime), optional
        **Data-level** time-period filter ``(period_start, period_end)``: a
        chunk matches when its temporal range overlaps the period —
        ``t_start <= period_end AND t_end >= period_start`` (both ends
        inclusive). ``t_start``/``t_end`` are the min/max of the chunk's
        per-point scan times, so this filters on when the data was actually
        collected. Chunks with a null temporal range never match. Executed
        as the index-friendly ``D_MAX`` rewrite (ADR-0002 Decision 3).
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
        - group_path: S3 path to the Parquet partition (from MetadataJson)
        - num_rows: Number of rows in the group (from MetadataJson)
        - columns: List of columns in the group (from MetadataJson)
        - scan: Scan name if applicable (from MetadataJson)
        
    Examples
    --------
    >>> # Load all metadata
    >>> df = load_s3_metadata()
    
    >>> # Load metadata for specific dataset
    >>> df = load_s3_metadata(dataset="MOD05_L2")
    
    >>> # Load metadata for specific date range
    >>> df = load_s3_metadata(
    ...     start_date="2023-01-01",
    ...     end_date="2023-12-31"
    ... )
    
    >>> # Load metadata for specific S3 bucket and resolution
    >>> df = load_s3_metadata(
    ...     s3_bucket="my-data-bucket",
    ...     resolution_level=10
    ... )
    
    >>> # Load metadata with custom ordering and limit
    >>> df = load_s3_metadata(
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

        if dataset_prefix is not None:
            conditions.append('"Dataset" LIKE %s')
            params.append(f"{dataset_prefix}_%")

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

        if path_prefix is not None:
            path_conds, path_params = _path_prefix_condition(
                path_prefix, 'postgres', placeholder='%s')
            conditions.extend(path_conds)
            params.extend(path_params)
        if period is not None:
            period_conds, period_params = _period_conditions(period, placeholder='%s')
            conditions.extend(period_conds)
            params.extend(period_params)

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
        raise RuntimeError(f"Error loading metadata from database: {e}")


def get_s3_summary(dataset=None, data_level=None, s3_bucket=None):
    """
    Get a summary of partition metadata stored in the database.
    
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
    >>> summary = get_s3_summary()
    
    >>> # Get summary for specific dataset
    >>> summary = get_s3_summary(dataset="MOD05_L2")
    """
    # Load metadata
    df = load_s3_metadata(dataset=dataset, data_level=data_level, s3_bucket=s3_bucket)
    
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


def from_legacy_zarr_s3(s3_path, storage_options=None):
    """
    Read STAREDataFrame from S3 chunked zarr store (alternative format to grouped zarr).
    
    This function reads zarr data stored as a single chunked group, which is different
    from the Parquet partitions read by STAREDataFrame.from_s3().
    
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
    >>> # Read legacy chunked zarr data from S3
    >>> df = from_legacy_zarr_s3('s3://my-bucket/granule_data/')

    >>> # With custom storage options
    >>> df = from_legacy_zarr_s3(
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
            "to set credentials/region, or pass storage_options to from_s3."
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


def from_legacy_zarr_s3_groups(s3_path, group_sid_ids, storage_options=None):
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
    >>> # Read specific groups from legacy chunked zarr data
    >>> group_ids = [3447505514752114692, 3445253714938429444]
    >>> df = from_legacy_zarr_s3_groups('s3://my-bucket/granule_data/', group_ids)

    >>> # With custom storage options
    >>> df = from_legacy_zarr_s3_groups(
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
            "to set credentials/region, or pass storage_options to from_s3_groups."
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


def generate_partition_path(sid, dataset_name=""):
    """
    Generate the pod code for a STARE SID (quaternary storage layout).

    Convenience wrapper around
    :func:`starepandas.staredataframe.sid_to_podcode`.  Returns the compact,
    dynamic-length pod code (``"q13211"``) — a single path segment.  See
    ``docs/quaternary_storage_plan.md`` §2.

    Parameters
    ----------
    sid : int
        8-byte STARE SID integer.
    dataset_name : str, optional
        Ignored — datasets are carried in the chunk filename, not the path.

    Returns
    -------
    str
        The pod code (e.g. ``"q13211"``).

    Examples
    --------
    >>> from starepandas.staredataframe import sid_to_podcode, podcode_to_sid
    >>> from starepandas.io.granules import generate_partition_path
    >>> generate_partition_path(podcode_to_sid("q13211"))
    'q13211'

    See Also
    --------
    sid_to_podcode : The underlying codec function.
    parse_partition_path : Reverse operation to parse pod codes back to SIDs.
    chunk_filename : Build a self-describing chunk filename.
    """
    from starepandas.staredataframe import sid_to_podcode
    return sid_to_podcode(sid)


def parse_partition_path(partition_path):
    """
    Reconstruct a STARE SID from a pod code (quaternary storage layout).

    Convenience wrapper around
    :func:`starepandas.staredataframe.podcode_to_sid`.  Reverse of
    :func:`generate_partition_path`.

    Parameters
    ----------
    partition_path : str
        A pod code (e.g. ``"q13211"``).  A ``/``-joined prefix is tolerated;
        only the final segment is parsed.

    Returns
    -------
    tuple
        ``(sid, None)`` — the dataset is no longer embedded in the path, so the
        second element (kept for tuple-shape compatibility) is always ``None``.

    Examples
    --------
    >>> from starepandas.io.granules import parse_partition_path, generate_partition_path
    >>> sid, _ = parse_partition_path("q13211")
    >>> generate_partition_path(sid)
    'q13211'

    See Also
    --------
    podcode_to_sid : The underlying codec function.
    generate_partition_path : Reverse operation to generate pod codes from SIDs.
    """
    from starepandas.staredataframe import podcode_to_sid
    podcode = partition_path.rstrip('/').split('/')[-1]
    return podcode_to_sid(podcode), None


def _podcode_query_prefixes(podcode):
    """S3 key prefixes that cover a query trixel's pod code in the flat layout.

    Returns the query code itself (a prefix match catches it and any
    finer-than-query descendants) plus, for each *coarser ancestor* code, the
    ``<ancestor>-`` form (the trailing ``-`` restricts the match to chunks
    stored exactly at that ancestor level rather than re-matching the query's
    own subtree — the dynamic-length mixed-level caveat, plan §4.3 / Q4).

    >>> _podcode_query_prefixes("q132110")
    ['q132110', 'q13-', 'q132-', 'q1321-', 'q13211-']
    """
    prefixes = [podcode]
    body = podcode[1:]
    # Ancestors start at the level-0 root, whose body is the 2-digit octant.
    for n in range(2, len(body)):
        prefixes.append('q' + body[:n] + '-')
    return prefixes


def reconstitute_hdf5_from_s3(
    s3_root, dataset, output_hdf5_path,
    area_sids=None, bbox=None,
    s3_prefix=None, granule_name=None,
    storage_options=None, pixel_width=None,
    compression='gzip', compression_opts=4,
    mode='w',
):
    """
    Reconstitute an HDF5 granule from Parquet partitions stored on S3 or local disk.

    Reads only the Parquet files whose STARE partition SIDs intersect the
    requested area, concatenates them into a STAREDataFrame, then calls
    ``STAREDataFrame.to_hdf5()`` to write the original granule structure.

    Parameters
    ----------
    s3_root : str
        Root path.  Use ``"s3://bucket/prefix"`` for S3 or a local
        directory path for local storage.
    dataset : str
        Dataset / scan identifier used when the data were written
        (e.g. ``"GMI_S1"``).  Used to:

        * locate the correct partition files via ``generate_partition_path``.
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
    s3_prefix : str, optional
        When ``s3_root`` is an S3 path, restrict matching to RDS metadata
        rows whose ``group_path`` starts with this prefix.  Use to scope a
        reconstitute call to one storage root.  Ignored for local roots.
    granule_name : str, optional
        Restrict to a single granule (its basename without extension). A
        prefix cannot express this: the flat S3 layout is
        ``<prefix>/<podcode>-<granule>-<dataset>.parquet``, so the granule
        name sits in the middle of the key. Required whenever a prefix holds
        more than one granule of the same dataset — otherwise they are
        reconstituted merged into one file. Ignored for local roots, which
        take the equivalent filter through
        :func:`reconstitute_hdf5_from_local`.
    storage_options : dict, optional
        Passed to ``s3fs.S3FileSystem`` for S3 authentication / configuration.
        If ``None`` and ``s3_root`` starts with ``"s3://"``, the built-in
        ``_AWS_S3_STORAGE_OPTIONS`` are used.
    pixel_width : int, optional
        Explicit pixel_width override.  When ``None``, the function looks
        first in the Parquet kv-metadata then in ``SCAN_PIXEL_WIDTHS``.
    compression : str, optional
        HDF5 compression filter (default ``'gzip'``).
    compression_opts : int, optional
        Compression level (default ``4``).
    mode : str, optional
        HDF5 file mode passed to ``STAREDataFrame.to_hdf5`` (``'w'`` or
        ``'a'``).  Default ``'w'``.

    Returns
    -------
    str
        ``output_hdf5_path``

    Raises
    ------
    ValueError
        If *both* ``area_sids`` and ``bbox`` are provided (passing *neither*
        is allowed — it reconstitutes the full granule, task-13 parity with
        :func:`reconstitute_hdf5_from_local`), if no matching partition files
        are found, or if ``pixel_width`` cannot be determined.
    """
    import pyarrow.parquet as pq
    import pystare
    from starepandas import STAREDataFrame
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS, MAX_PARTITION_LEVEL

    # ── Validate inputs ──────────────────────────────────────────────────────
    # Both-None means "no spatial filter — reconstitute the full granule"
    # (task 13 parity with the local reconstitute_hdf5_from_local). Either
    # set, but not both.
    if area_sids is not None and bbox is not None:
        raise ValueError(
            "Provide at most one of 'area_sids' or 'bbox', not both."
        )

    no_spatial_filter = (area_sids is None and bbox is None)

    # ── Build query SIDs (skipped when no_spatial_filter) ────────────────────
    if not no_spatial_filter:
        if bbox is not None:
            lon_min, lat_min, lon_max, lat_max = bbox
            lats = [lat_min, lat_min, lat_max, lat_max]
            lons = [lon_min, lon_max, lon_max, lon_min]
            area_sids = pystare.cover_from_hull(lats, lons, MAX_PARTITION_LEVEL)
        coerced = pystare.spatial_coerce_resolution(
            np.array(area_sids, dtype=np.int64), MAX_PARTITION_LEVEL
        )
        query_group_ids = set(int(s) for s in np.unique(coerced))
    else:
        query_group_ids = None   # signal: include every partition for this dataset

    # ── Collect matching partition paths ─────────────────────────────────────
    is_s3 = s3_root.startswith('s3://')
    merged_opts = storage_options or (_AWS_S3_STORAGE_OPTIONS if is_s3 else {})

    group_paths = []       # (group_path, group_id) tuples
    parquet_pixel_width = None  # pixel_width read from Parquet kv-metadata

    if is_s3:
        from starepandas.io.granules import load_s3_metadata
        meta_df = load_s3_metadata(dataset=dataset)
        if meta_df is not None and not meta_df.empty:
            meta_df['grouped_id'] = meta_df['grouped_id'].astype(np.int64)
            # Filter to a specific S3 prefix when provided (e.g. a single granule path)
            if s3_prefix is not None:
                meta_df = meta_df[meta_df['group_path'].str.startswith(s3_prefix)]
            # Scope to one granule. A prefix cannot do this on its own: the flat
            # S3 layout is <prefix>/<podcode>-<granule>-<dataset>.parquet, so the
            # granule name sits in the *middle* of the key. Without this, a
            # prefix holding two granules of the same instrument reconstitutes
            # both merged together — silently doubling the scan lines.
            if granule_name is not None:
                meta_df = meta_df[
                    meta_df['group_path'].str.contains(f'-{granule_name}-', regex=False)
                ]

            if no_spatial_filter:
                # Task 13 — no bbox/area_sids: take every partition for this dataset.
                matching = meta_df
            else:
                # Detect the actual STARE level used for grouped_id in this dataset.
                # It may differ from MAX_PARTITION_LEVEL when data was ingested with
                # a different partitioning level. Lower 5 bits of a SID encode level.
                storage_levels = set(int(gid & 0x1f) for gid in meta_df['grouped_id'].dropna())
                if storage_levels == {MAX_PARTITION_LEVEL}:
                    effective_query_ids = query_group_ids  # fast path: levels match
                else:
                    # Re-coerce the query area to each unique storage level so the
                    # set-membership filter works regardless of ingest level.
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
            # Fallback: no PodsMetadata rows — query the flat S3 layout directly
            # via pod-code key prefixes (docs/quaternary_storage_plan.md §4/Q4).
            # Only valid when we actually have a query area — no_spatial_filter
            # has no SIDs to enumerate prefixes from.
            if no_spatial_filter:
                raise ValueError(
                    f"No PodsMetadata rows for dataset '{dataset}' and no "
                    "spatial filter to enumerate partitions from. Pass bbox= "
                    "or area_sids=, or ensure the dataset has been ingested."
                )
            from starepandas.staredataframe import (
                parse_chunk_filename, podcode_to_sid, sid_to_podcode, CHUNK_SUFFIX,
            )
            import s3fs
            fs = s3fs.S3FileSystem(**merged_opts) if merged_opts else s3fs.S3FileSystem()
            root_no_scheme = (s3_root[len('s3://'):] if s3_root.startswith('s3://')
                              else s3_root).rstrip('/')
            seen = set()
            for gid in query_group_ids:
                for prefix in _podcode_query_prefixes(sid_to_podcode(gid)):
                    try:
                        keys = fs.glob(f"{root_no_scheme}/{prefix}*{CHUNK_SUFFIX}")
                    except Exception:
                        keys = []
                    for k in keys:
                        if k in seen:
                            continue
                        try:
                            pc, _g, ds = parse_chunk_filename(k)
                        except ValueError:
                            continue
                        if ds != dataset:
                            continue
                        seen.add(k)
                        group_paths.append((f"s3://{k}", podcode_to_sid(pc)))
    else:
        # Local: hierarchical pod-code tree — walk for chunk files, parse each
        # file's pod code + dataset from its (authoritative) filename, and keep
        # only those whose dataset matches and whose SID intersects the query.
        if not os.path.isdir(s3_root):
            raise ValueError(f"Local root path does not exist: {s3_root}")
        from starepandas.staredataframe import (
            parse_chunk_filename, podcode_to_sid, CHUNK_SUFFIX,
        )
        for root, _dirs, files in os.walk(s3_root):
            for fn in files:
                if not fn.endswith(CHUNK_SUFFIX):
                    continue
                try:
                    podcode, _g, ds = parse_chunk_filename(fn)
                except ValueError:
                    continue
                if ds != dataset:
                    continue
                try:
                    sid = podcode_to_sid(podcode)
                except ValueError:
                    continue
                filepath = os.path.join(root, fn)
                # Task 13: no_spatial_filter ⇒ include every partition found.
                if no_spatial_filter or sid in query_group_ids:
                    group_paths.append((filepath, sid))

    if not group_paths:
        if no_spatial_filter:
            raise ValueError(
                f"No Parquet partitions found for dataset '{dataset}' under "
                f"'{s3_root}' (no spatial filter applied)."
            )
        raise ValueError(
            f"No Parquet partitions found for dataset '{dataset}' intersecting "
            f"the requested area.  Queried {len(query_group_ids)} partition SIDs."
        )

    # ── Read matching partitions ─────────────────────────────────────────────
    if is_s3:
        import s3fs
        parquet_fs = s3fs.S3FileSystem(**merged_opts) if merged_opts else s3fs.S3FileSystem()
    else:
        parquet_fs = None

    frames = []
    for gpath, _gid in group_paths:
        try:
            if is_s3:
                read_path = gpath[len('s3://'):] if gpath.startswith('s3://') else gpath
                pq_file = pq.ParquetFile(read_path, filesystem=parquet_fs)
            else:
                pq_file = pq.ParquetFile(gpath)
        except Exception as e:
            logging.warning("Skipping Parquet partition %s — could not open: %s", gpath, e)
            continue

        # Harvest pixel_width from Parquet kv-metadata (first partition wins)
        if parquet_pixel_width is None:
            md = pq_file.schema_arrow.metadata or {}
            pw_bytes = md.get(b'pixel_width')
            if pw_bytes is not None:
                try:
                    parquet_pixel_width = int(pw_bytes.decode())
                except (ValueError, AttributeError):
                    pass

        df_chunk = pq_file.read().to_pandas()
        if df_chunk.empty:
            continue
        frames.append(df_chunk)

    if not frames:
        raise ValueError(
            f"All matched Parquet partitions were empty for dataset '{dataset}'."
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
    elif parquet_pixel_width is not None:
        resolved_pw = parquet_pixel_width
        logging.debug("pixel_width=%d read from Parquet kv-metadata for dataset '%s'", resolved_pw, dataset)
    else:
        resolved_pw = SCAN_PIXEL_WIDTHS.get(dataset)
        if resolved_pw is not None:
            logging.debug("pixel_width=%d from SCAN_PIXEL_WIDTHS for dataset '%s'", resolved_pw, dataset)

    if resolved_pw is None:
        raise ValueError(
            f"Cannot determine pixel_width for dataset '{dataset}'. "
            "Pass pixel_width explicitly or ensure it was stored in Parquet kv-metadata."
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
        mode=mode,
    )
    return output_hdf5_path


# ─────────────────────────────────────────────────────────────────────────────
# Local (filesystem + SQLite) pipeline
# ─────────────────────────────────────────────────────────────────────────────

def to_local(
    file_path, local_path, level, db_path,
    chunk_size=250000, dataset=None, data_level=None,
    raw_collected_time=None, metadata=None,
    sidecar_path=None, add_sids=True, adapt_resolution=True,
    read_timestamp=False, keep_na_sids=False, nom_res=None, scan=None,
    granule_name=None,
    **kwargs
):
    """
    Convert a granule file to Parquet on the **local filesystem** and record
    metadata to a SQLite database.

    This is the local-storage mirror of :func:`to_s3`.  The calling
    interface is identical except that ``local_path`` is a filesystem directory
    instead of an S3 URI, ``db_path`` is a SQLite ``.db`` file instead of a
    PostgreSQL connection, and no ``storage_options`` are required.

    Parameters
    ----------
    file_path : str
        Path to the granule HDF5 file.
    local_path : str
        Root directory where Parquet partitions will be written.  Sub-directories are
        created automatically.  The granule basename is inserted *inside* the
        directory tree (between the HTM-subtree leaf and the dataset segment),
        not appended to ``local_path``.
    level : int
        STARE level for spatial partitioning.
    db_path : str
        Path to the SQLite metadata database.  Created automatically if it
        does not exist.
    chunk_size : int, optional
        Unused; retained for API compatibility.
    dataset : str, optional
        Dataset name stored in metadata (e.g. ``"GMI"``).
    data_level : str, optional
        Data level string (e.g. ``"L1C"``).
    raw_collected_time : datetime, optional
        Override timestamp for the metadata row.
    metadata : dict, optional
        Extra key/value pairs merged into ``MetadataJson``.
    sidecar_path : str, optional
        Path to pre-computed STARE sidecar file.
    add_sids : bool, optional
        Compute STARE SIDs if not present (default True).
    adapt_resolution : bool, optional
        Adapt SID resolution to nominal instrument resolution (default True).
    read_timestamp : bool, optional
        Read time-stamp data from granule (default False).
    keep_na_sids : bool, optional
        Retain rows whose SIDs are NaN (default False).
    nom_res : str, optional
        Nominal resolution selector for multi-resolution products.
    scan : str, optional
        Specific scan to process (e.g. ``"S1"``); ``None`` means all scans.
    granule_name : str, optional
        Granule identifier inserted into the on-disk path between the HTM
        leaf and the dataset segment, and recorded in ``MetadataJson`` so
        :func:`reconstitute_hdf5_from_local` can filter by granule.
        Defaults to the basename of ``file_path`` without extension.
    **kwargs
        Forwarded to :func:`read_granule`.

    Returns
    -------
    str or list of str
        The local path(s) where data was written.
    """
    if granule_name is None:
        granule_name = os.path.splitext(os.path.basename(file_path))[0]

    # §C10 #2 (mirrors to_s3): derive a deterministic per-granule timestamp
    # from the filename so re-ingest hits the SQLite upsert instead of
    # inserting duplicate rows stamped with utcnow(). Unlike the S3 path,
    # an unrecognized filename falls back to ingest time rather than
    # failing: local ingest of custom/renamed granules worked before the
    # temporal columns and must keep working (it just isn't idempotent).
    if raw_collected_time is None:
        from starepandas.io.granules._timestamps import (
            CannotDeriveTimestampError, derive_timestamp_from_path,
        )
        try:
            raw_collected_time = derive_timestamp_from_path(file_path)
        except CannotDeriveTimestampError:
            logging.warning(
                "to_local: could not derive a collection timestamp from %r; "
                "falling back to ingest time — re-ingesting this granule "
                "will add duplicate catalog rows instead of refreshing.",
                os.path.basename(file_path),
            )

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

    if isinstance(result, dict):
        # Multi-scan granule (e.g. SSMIS)
        if scan is not None:
            if scan not in result:
                raise ValueError(f"Scan '{scan}' not found. Available: {list(result.keys())}")
            df = result[scan]
            scan_dataset = f"{dataset}_{scan}" if dataset else f"data_{scan}"
            single_meta = (metadata or {}).copy()
            single_meta.update({"scan": scan})
            pw = SCAN_PIXEL_WIDTHS.get(scan_dataset)
            if pw is not None:
                single_meta['pixel_width'] = pw
            return df.to_local(
                local_path=local_path,
                level=level,
                chunk_size=chunk_size,
                pixel_width=pw,
                db_path=db_path,
                dataset=scan_dataset,
                data_level=data_level,
                granule_name=granule_name,
                raw_collected_time=raw_collected_time,
            )
        else:
            local_paths = []
            for scan_name, df in result.items():
                scan_dataset = f"{dataset}_{scan_name}" if dataset else f"data_{scan_name}"
                scan_meta = (metadata or {}).copy()
                scan_meta.update({"scan": scan_name})
                pw = SCAN_PIXEL_WIDTHS.get(scan_dataset)
                if pw is not None:
                    scan_meta['pixel_width'] = pw
                path_out = df.to_local(
                    local_path=local_path,
                    level=level,
                    chunk_size=chunk_size,
                    pixel_width=pw,
                    db_path=db_path,
                    dataset=scan_dataset,
                    data_level=data_level,
                    granule_name=granule_name,
                    raw_collected_time=raw_collected_time,
                )
                local_paths.append(path_out)
            return local_paths
    else:
        # Single DataFrame
        return result.to_local(
            local_path=local_path,
            level=level,
            chunk_size=chunk_size,
            pixel_width=None,
            db_path=db_path,
            dataset=dataset,
            data_level=data_level,
            granule_name=granule_name,
            raw_collected_time=raw_collected_time,
        )


def load_local_metadata(
    db_path,
    dataset=None,
    dataset_prefix=None,
    resolution_level=None,
    start_date=None,
    end_date=None,
    grouped_id=None,
    period=None,
    path_prefix=None,
    limit=None,
    order_by=None,
):
    """
    Load metadata from the local SQLite database for Parquet partitions on disk.

    Local equivalent of :func:`load_s3_metadata`.  Returns a
    ``pandas.DataFrame`` with the same column structure so that downstream
    code (e.g. :func:`reconstitute_hdf5_from_local`) can be written once
    and work for both S3 and local backends.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.
    dataset : str, optional
        Exact dataset name filter.
    dataset_prefix : str, optional
        LIKE filter — matches datasets whose names start with
        ``"<dataset_prefix>_"``.
    resolution_level : int, optional
        Filter by STARE resolution level.
    start_date : str, optional
        **Granule-level** ISO-8601 lower-bound (inclusive) on
        ``"RawData Collected Time"`` — the filename-derived collection time.
        Distinct from ``period``, which filters the data-level range.
    end_date : str, optional
        Granule-level ISO-8601 upper-bound (inclusive) on
        ``"RawData Collected Time"``; see ``start_date``.
    grouped_id : int, optional
        Filter by exact grouped_id.
    period : tuple, optional
        **Data-level** time-period filter ``(period_start, period_end)``:
        includes a chunk when its temporal range ``[t_start, t_end]``
        overlaps the period (both ends inclusive). Chunks with a null
        temporal range never match. Same semantics as
        :func:`load_s3_metadata`'s ``period``.
    limit : int, optional
        Maximum rows to return.
    order_by : str, optional
        Column name to order by (default: ``"RawData Collected Time"`` DESC).

    Returns
    -------
    pandas.DataFrame
        Metadata rows with ``MetadataJson`` expanded into individual columns
        (``group_path``, ``num_rows``, ``columns``, ``pixel_width``, …).
    """
    import sqlite3

    from starepandas.staredataframe import _ensure_sqlite_db_and_table

    conn = _ensure_sqlite_db_and_table(db_path)
    try:
        query = 'SELECT * FROM "PodsMetadata"'
        conditions = []
        params = []

        if dataset is not None:
            conditions.append('"Dataset" = ?')
            params.append(dataset)

        if dataset_prefix is not None:
            conditions.append('"Dataset" LIKE ?')
            params.append(f"{dataset_prefix}_%")

        if resolution_level is not None:
            conditions.append('"Resolution level" = ?')
            params.append(resolution_level)

        if grouped_id is not None:
            conditions.append('grouped_id = ?')
            params.append(int(grouped_id))

        if start_date is not None:
            conditions.append('"RawData Collected Time" >= ?')
            params.append(str(start_date))

        if end_date is not None:
            conditions.append('"RawData Collected Time" <= ?')
            params.append(str(end_date))

        if path_prefix is not None:
            path_conds, path_params = _path_prefix_condition(
                path_prefix, 'sqlite', placeholder='?')
            conditions.extend(path_conds)
            params.extend(path_params)

        if period is not None:
            period_conds, period_params = _period_conditions(
                period, placeholder='?', as_iso=True)
            conditions.extend(period_conds)
            params.extend(period_params)

        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        if order_by is not None:
            query += f' ORDER BY "{order_by}"'
        else:
            query += ' ORDER BY "RawData Collected Time" DESC'

        if limit is not None:
            query += f' LIMIT {int(limit)}'

        cur = conn.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=columns)

    # Expand MetadataJson into individual columns (same pattern as load_s3_metadata)
    if not df.empty and 'MetadataJson' in df.columns:
        meta_list = []
        for json_str in df['MetadataJson']:
            if json_str:
                try:
                    meta_list.append(json.loads(json_str) if isinstance(json_str, str) else json_str)
                except (json.JSONDecodeError, TypeError):
                    meta_list.append({})
            else:
                meta_list.append({})
        meta_df = pd.DataFrame(meta_list)
        for col in meta_df.columns:
            if col not in df.columns:
                df[col] = meta_df[col]

    return df


#: Columns the overlap-analytics loaders project (ADR-0002 Decision 3) —
#: never ``MetadataJson``.
TEMPORAL_CATALOG_COLUMNS = ['podcode', 'Dataset', 't_start', 't_end']


def _finish_temporal_catalog(rows):
    """Rows → analytics-ready frame: fixed columns, parsed timestamps.

    ``format='ISO8601'`` so a catalog mixing whole-second stamps
    (``2025-01-01T04:36:55``) with fractional ones
    (``...:55.123456``) parses cleanly — pandas otherwise infers a single
    format from the first row and rejects the rest. ``None`` (null-range
    chunks) parses to ``NaT``.
    """
    df = pd.DataFrame(rows, columns=TEMPORAL_CATALOG_COLUMNS)
    df['t_start'] = pd.to_datetime(df['t_start'], format='ISO8601')
    df['t_end'] = pd.to_datetime(df['t_end'], format='ISO8601')
    return df


def load_s3_temporal_catalog(dataset=None, dataset_prefix=None, period=None,
                             podcode_prefix=None, path_prefix=None):
    """
    Thin-projection catalog load for the overlap analytics (cloud/RDS).

    Projects exactly ``podcode``, ``Dataset``, ``t_start``, ``t_end`` per
    ADR-0002 Decision 3 and prunes by ``period`` via the ``t_start`` index
    range (the ``D_MAX`` rewrite); since issue 06 the projection is answered
    index-only by ``idx_pods_temporal_covering`` (no heap access). Use
    :func:`load_s3_metadata` when the full metadata (group paths, etc.) is
    needed.

    Parameters
    ----------
    dataset : str, optional
        Exact dataset name filter.
    dataset_prefix : str, optional
        Matches datasets named ``"<dataset_prefix>_…"``.
    period : tuple, optional
        Data-level time-period filter; same semantics as
        :func:`load_s3_metadata`'s ``period``.
    podcode_prefix : str, optional
        Restrict to the pod subtree under this pod code (a coarser pod code
        is a prefix of its descendants' codes); see
        :func:`_podcode_prefix_condition`.
    path_prefix : str, optional
        Restrict to chunks written under this storage root (e.g.
        ``"s3://zarrpods/gmi-demo-parquet"``). The catalog is shared by every
        ingest, so this is the only filter that separates one job's chunks
        from another's when they cover the same instrument and hours; see
        :func:`_path_prefix_condition`.

    Returns
    -------
    pandas.DataFrame
        Columns ``podcode``, ``Dataset``, ``t_start``, ``t_end`` with the
        temporal columns parsed to timestamps. Rows without a pod code
        (pre-temporal catalogs upgraded in place but never re-ingested)
        cannot participate in pod-keyed analytics and are excluded.
    """
    from starepandas.staredataframe import _ensure_rds_db_and_table

    conn = _ensure_rds_db_and_table('StarePodsMetadata')
    try:
        query = 'SELECT podcode, "Dataset", t_start, t_end FROM "PodsMetadata"'
        conditions = ['podcode IS NOT NULL']
        params = []
        if dataset is not None:
            conditions.append('"Dataset" = %s')
            params.append(dataset)
        if dataset_prefix is not None:
            conditions.append('"Dataset" LIKE %s')
            params.append(f"{dataset_prefix}_%")
        if podcode_prefix is not None:
            prefix_conds, prefix_params = _podcode_prefix_condition(
                podcode_prefix, placeholder='%s')
            conditions.extend(prefix_conds)
            params.extend(prefix_params)
        if path_prefix is not None:
            path_conds, path_params = _path_prefix_condition(
                path_prefix, 'postgres', placeholder='%s')
            conditions.extend(path_conds)
            params.extend(path_params)
        if period is not None:
            period_conds, period_params = _period_conditions(period, placeholder='%s')
            conditions.extend(period_conds)
            params.extend(period_params)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    return _finish_temporal_catalog(rows)


def load_local_temporal_catalog(db_path, dataset=None, dataset_prefix=None,
                                period=None, podcode_prefix=None,
                                path_prefix=None):
    """
    Thin-projection catalog load for the overlap analytics (local/SQLite).

    Local equivalent of :func:`load_s3_temporal_catalog`; see there for the
    projection, period, and subtree semantics.

    Parameters
    ----------
    db_path : str
        Path to the SQLite metadata database.
    dataset, dataset_prefix, period, podcode_prefix, path_prefix : optional
        As in :func:`load_s3_temporal_catalog`.

    Returns
    -------
    pandas.DataFrame
        Columns ``podcode``, ``Dataset``, ``t_start``, ``t_end`` with the
        temporal columns parsed to timestamps. Rows without a pod code
        (pre-temporal catalogs upgraded in place but never re-ingested)
        cannot participate in pod-keyed analytics and are excluded.
    """
    from starepandas.staredataframe import _ensure_sqlite_db_and_table

    conn = _ensure_sqlite_db_and_table(db_path)
    try:
        query = 'SELECT podcode, "Dataset", t_start, t_end FROM "PodsMetadata"'
        conditions = ['podcode IS NOT NULL']
        params = []
        if dataset is not None:
            conditions.append('"Dataset" = ?')
            params.append(dataset)
        if dataset_prefix is not None:
            conditions.append('"Dataset" LIKE ?')
            params.append(f"{dataset_prefix}_%")
        if podcode_prefix is not None:
            prefix_conds, prefix_params = _podcode_prefix_condition(
                podcode_prefix, placeholder='?')
            conditions.extend(prefix_conds)
            params.extend(prefix_params)
        if path_prefix is not None:
            path_conds, path_params = _path_prefix_condition(
                path_prefix, 'sqlite', placeholder='?')
            conditions.extend(path_conds)
            params.extend(path_params)
        if period is not None:
            period_conds, period_params = _period_conditions(
                period, placeholder='?', as_iso=True)
            conditions.extend(period_conds)
            params.extend(period_params)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    return _finish_temporal_catalog(rows)


#: Columns of a VCF roll-up frame (issue 04 — the on-the-fly temporal
#: hierarchy). One row per pod at the requested level: the union temporal
#: range of all chunks beneath it plus the child count.
VCF_COLUMNS = ['podcode', 't_start', 't_end', 'n_chunks', 'n_without_range']


def _validate_vcf_args(level, subtree=None):
    """Validate a roll-up's ``level``/``subtree`` pair → pod-code prefix length.

    Single validation point for :func:`vcf_rollup` and the VCF loaders (which
    call it *before* the catalog round-trip, to fail fast). Rejects a
    ``subtree`` deeper than the requested ``level``: the result row would
    carry a coarse pod's code but a union range covering only the subtree's
    sliver — indistinguishable from the pod's true envelope.
    """
    from starepandas.staredataframe import (podcode_prefix_length,
                                            podcode_to_sid)

    n = podcode_prefix_length(level)
    if subtree is not None:
        podcode_to_sid(subtree)              # grammar check (raises ValueError)
        if len(subtree) > n:
            raise ValueError(
                f"subtree {subtree!r} (level {len(subtree) - 2}) is deeper "
                f"than the requested roll-up level {level}: a level-{level} "
                f"row would cover only that subtree's chunks while carrying "
                f"the coarser pod's code. Roll up at level >= "
                f"{len(subtree) - 2} instead."
            )
    return n


def vcf_rollup(catalog, level, subtree=None):
    """
    Roll a temporal catalog up the pod-code hierarchy, one VCF node per pod.

    Groups chunks by their level-``level`` ancestor pod — the first
    ``podcode_prefix_length(level)`` characters of the pod code, since a
    coarser pod code is a prefix of its descendants' codes — and returns, per
    pod, its **VCF (Virtual Collection File) node**: the union temporal range
    ``[min(t_start), max(t_end)]`` of the chunks beneath it plus the child
    count. Computed on the fly; nothing is materialized.

    Pure function over an already-loaded frame (no database access), so a
    Δt / level / subtree change never re-queries the catalog (ADR-0002
    Decision 2). Use :func:`load_s3_vcf` / :func:`load_local_vcf` for the
    catalog-backed equivalents.

    Parameters
    ----------
    catalog : pandas.DataFrame
        A temporal-catalog frame with ``podcode`` and *parsed* (datetime)
        ``t_start`` / ``t_end`` columns, as returned by
        :func:`load_s3_temporal_catalog` / :func:`load_local_temporal_catalog`.
        Every row must carry a pod code — rows without one (pre-temporal
        catalog remnants) would otherwise vanish from the counts silently,
        so they raise instead (the loaders exclude them in SQL).
    level : int
        Pod-code level to roll up to (0 = octant … ``MAX_PARTITION_LEVEL`` =
        leaf). A chunk cataloged at a level coarser than ``level`` groups
        under its own (shorter) pod code.
    subtree : str, optional
        Restrict the roll-up to the chunks under this pod code (validated
        against the pod-code grammar; must not be deeper than ``level`` —
        see :func:`_validate_vcf_args`).

    Returns
    -------
    pandas.DataFrame
        :data:`VCF_COLUMNS` — one row per pod, sorted by ``podcode``.
        ``n_chunks`` counts every chunk beneath the pod; ``n_without_range``
        notes how many of them lack a usable temporal range (either end
        missing — e.g. ingested without per-point timestamps). Range-less
        chunks never contribute either end to the union; a pod holding only
        range-less chunks gets a null union range.
    """
    n = _validate_vcf_args(level, subtree)
    df = catalog
    _require_podcodes(df)
    if subtree is not None:
        df = df[df['podcode'].str.startswith(subtree)]

    pods = df['podcode'].str[:n]
    pods.name = 'podcode'
    # A half-null range is no range: neither end may join the union.
    missing = df['t_start'].isna() | df['t_end'].isna()
    grouped = pd.DataFrame({
        't_start': df['t_start'].where(~missing),
        't_end': df['t_end'].where(~missing),
        'n_without_range': missing,
    }).groupby(pods, sort=True)
    vcf = grouped.agg({'t_start': 'min', 't_end': 'max',   # NaT-skipping
                       'n_without_range': 'sum'})
    vcf['n_chunks'] = grouped.size()
    vcf['n_without_range'] = vcf['n_without_range'].astype('int64')
    return vcf.reset_index()[VCF_COLUMNS]


def load_s3_vcf(level, subtree=None, dataset=None, dataset_prefix=None,
                period=None):
    """
    VCF temporal roll-up over the cloud (RDS) catalog.

    Loads the thin temporal-catalog projection (``subtree`` / ``dataset`` /
    ``period`` filters pushed into SQL, riding the ``idx_pods_podcode`` and
    ``idx_pods_temporal`` indexes) and rolls it up with the shared pure
    :func:`vcf_rollup` — both backends go through the same group-by code.

    Parameters
    ----------
    level : int
        Pod-code level to roll up to; see :func:`vcf_rollup`.
    subtree : str, optional
        Restrict to the pod subtree under this pod code.
    dataset, dataset_prefix, period : optional
        As in :func:`load_s3_temporal_catalog`.

    Returns
    -------
    pandas.DataFrame
        :data:`VCF_COLUMNS` — one row per pod at ``level``.
    """
    _validate_vcf_args(level, subtree)       # fail fast, before the DB hit
    catalog = load_s3_temporal_catalog(dataset=dataset,
                                       dataset_prefix=dataset_prefix,
                                       period=period, podcode_prefix=subtree)
    return vcf_rollup(catalog, level, subtree=subtree)


def load_local_vcf(db_path, level, subtree=None, dataset=None,
                   dataset_prefix=None, period=None):
    """
    VCF temporal roll-up over the local (SQLite) catalog.

    Local equivalent of :func:`load_s3_vcf`; see there and
    :func:`vcf_rollup` for semantics.

    Parameters
    ----------
    db_path : str
        Path to the SQLite metadata database.
    level : int
        Pod-code level to roll up to.
    subtree, dataset, dataset_prefix, period : optional
        As in :func:`load_s3_vcf`.

    Returns
    -------
    pandas.DataFrame
        :data:`VCF_COLUMNS` — one row per pod at ``level``.
    """
    _validate_vcf_args(level, subtree)       # fail fast, before the DB hit
    catalog = load_local_temporal_catalog(db_path, dataset=dataset,
                                          dataset_prefix=dataset_prefix,
                                          period=period,
                                          podcode_prefix=subtree)
    return vcf_rollup(catalog, level, subtree=subtree)


def reconstitute_hdf5_from_local(
    db_path,
    dataset,
    output_hdf5_path,
    area_sids=None,
    bbox=None,
    local_prefix=None,
    granule_name=None,
    pixel_width=None,
    compression='gzip',
    compression_opts=4,
    mode='w',
):
    """
    Reconstitute an HDF5 granule from Parquet partitions stored on the local
    filesystem.

    Local equivalent of :func:`reconstitute_hdf5_from_s3`.  Instead of
    querying S3 + RDS it queries the SQLite database written by
    :func:`to_local` and opens Parquet partitions on local disk.

    Parameters
    ----------
    db_path : str
        Path to the SQLite metadata database.
    dataset : str
        Dataset / scan identifier (e.g. ``"GMI_S1"``).
    output_hdf5_path : str
        Destination HDF5 file path.
    area_sids : array-like of int, optional
        STARE SIDs covering the area of interest.
    bbox : tuple of float, optional
        Bounding box ``(lon_min, lat_min, lon_max, lat_max)``.  Exactly one of
        ``area_sids`` or ``bbox`` must be given.
    local_prefix : str, optional
        Filter metadata to ``group_path`` entries that start with this prefix.
        Useful for scoping to a particular ``local_root``.  Note: under the
        current HTM-first layout the granule basename sits *inside* the path
        (between the HTM leaf and the dataset segment), so ``local_prefix``
        no longer scopes to a single granule — use ``granule_name`` for that.
    granule_name : str, optional
        Filter metadata to rows whose recorded ``granule_name`` matches.  This
        is the preferred per-granule filter under the HTM-first layout.
    pixel_width : int, optional
        Explicit pixel_width override.
    compression : str, optional
        HDF5 compression filter (default ``'gzip'``).
    compression_opts : int, optional
        Compression level (default ``4``).
    mode : str, optional
        h5py file open mode.  Use ``'w'`` to create/overwrite, ``'a'`` to
        append additional scan groups into an existing file (default ``'w'``).

    Returns
    -------
    str
        ``output_hdf5_path``
    """
    import pyarrow.parquet as pq
    import pystare
    from starepandas import STAREDataFrame
    from starepandas.staredataframe import MAX_PARTITION_LEVEL

    if area_sids is not None and bbox is not None:
        raise ValueError("Provide at most one of 'area_sids' or 'bbox', not both.")
    # When both are None → no spatial filter; all groups (filtered by local_prefix) are used.

    no_spatial_filter = area_sids is None and bbox is None

    # Build query SIDs when a spatial filter is requested
    if not no_spatial_filter:
        if bbox is not None:
            lon_min, lat_min, lon_max, lat_max = bbox
            lats = [lat_min, lat_min, lat_max, lat_max]
            lons = [lon_min, lon_max, lon_max, lon_min]
            area_sids = pystare.cover_from_hull(lats, lons, MAX_PARTITION_LEVEL)

        coerced = pystare.spatial_coerce_resolution(
            np.array(area_sids, dtype=np.int64), MAX_PARTITION_LEVEL
        )
        query_group_ids = set(int(s) for s in np.unique(coerced))

    # Load metadata from SQLite
    meta_df = load_local_metadata(db_path, dataset=dataset)
    if meta_df is None or meta_df.empty:
        raise ValueError(
            f"No metadata found for dataset '{dataset}' in SQLite database '{db_path}'."
        )

    meta_df['grouped_id'] = meta_df['grouped_id'].astype(np.int64)

    # Filter to a specific local prefix (e.g. one local_root)
    if local_prefix is not None:
        abs_prefix = os.path.abspath(local_prefix)
        meta_df = meta_df[meta_df['group_path'].str.startswith(abs_prefix)]

    # Filter by granule_name (preferred per-granule scoping under HTM-first layout)
    if granule_name is not None:
        if 'granule_name' not in meta_df.columns:
            raise ValueError(
                "granule_name filter requested but metadata lacks a 'granule_name' "
                "column. This metadata predates the granule_name field; re-ingest "
                "to use this filter."
            )
        meta_df = meta_df[meta_df['granule_name'] == granule_name]

    if no_spatial_filter:
        # No spatial filter — use all groups from local_prefix
        matching = meta_df
    else:
        # Adaptive STARE level detection (mirrors reconstitute_hdf5_from_s3)
        storage_levels = set(int(gid & 0x1f) for gid in meta_df['grouped_id'].dropna())
        if storage_levels == {MAX_PARTITION_LEVEL}:
            effective_query_ids = query_group_ids
        else:
            effective_query_ids: set = set()
            for slevel in storage_levels:
                if bbox is not None:
                    lon_min_q, lat_min_q, lon_max_q, lat_max_q = bbox
                    lats_q = [lat_min_q, lat_min_q, lat_max_q, lat_max_q]
                    lons_q = [lon_min_q, lon_max_q, lon_max_q, lon_min_q]
                    sids_q = pystare.cover_from_hull(lats_q, lons_q, int(slevel))
                else:
                    sids_q = area_sids
                coerced_q = pystare.spatial_coerce_resolution(
                    np.array(sids_q, dtype=np.int64), int(slevel)
                )
                effective_query_ids.update(int(s) for s in np.unique(coerced_q))

        matching = meta_df[meta_df['grouped_id'].isin(effective_query_ids)]

    if matching.empty:
        raise ValueError(
            f"No Parquet partitions found for dataset '{dataset}'"
            + (" intersecting the requested area." if not no_spatial_filter else " in the database.")
        )

    # Read matching Parquet partitions from local disk
    frames = []
    parquet_pixel_width = None
    for _, row in matching.iterrows():
        gpath = row['group_path']
        try:
            pq_file = pq.ParquetFile(gpath)
        except Exception as e:
            logging.warning("Skipping Parquet partition %s — could not open: %s", gpath, e)
            continue

        if parquet_pixel_width is None:
            md = pq_file.schema_arrow.metadata or {}
            pw_bytes = md.get(b'pixel_width')
            if pw_bytes is not None:
                try:
                    parquet_pixel_width = int(pw_bytes.decode())
                except (ValueError, AttributeError):
                    pass

        df_chunk = pq_file.read().to_pandas()
        if df_chunk.empty:
            continue
        frames.append(df_chunk)

    if not frames:
        raise ValueError(f"All matched Parquet partitions were empty for dataset '{dataset}'.")

    combined = pd.concat(frames, ignore_index=True)
    if '__row_positions__' in combined.columns:
        combined = combined.sort_values('__row_positions__').drop(
            columns=['__row_positions__']
        ).reset_index(drop=True)

    sdf = STAREDataFrame(combined)

    # Resolve pixel_width
    if pixel_width is not None:
        resolved_pw = pixel_width
    elif parquet_pixel_width is not None:
        resolved_pw = parquet_pixel_width
    else:
        resolved_pw = SCAN_PIXEL_WIDTHS.get(dataset)

    if resolved_pw is None:
        raise ValueError(
            f"Cannot determine pixel_width for dataset '{dataset}'. "
            "Pass pixel_width explicitly or ensure it was stored in Parquet kv-metadata."
        )

    # Derive HDF5 scan group name from dataset string
    m = re.search(r'_(S\d+)$', dataset)
    scan = m.group(1) if m else None
    if scan is None:
        raise ValueError(
            f"Cannot derive scan group name from dataset '{dataset}'. "
            "Expected a suffix like '_S1'.  Use 'INSTRUMENT_Sn' format."
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_hdf5_path)), exist_ok=True)
    sdf.to_hdf5(
        output_hdf5_path,
        scan=scan,
        pixel_width=resolved_pw,
        compression=compression,
        compression_opts=compression_opts,
        mode=mode,
    )
    return output_hdf5_path
