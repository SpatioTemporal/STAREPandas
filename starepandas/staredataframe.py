import bz2
import geopandas.plotting
import pystare
import pandas
import numpy as np
import starepandas
import netCDF4
import starepandas.tools.trixel_conversions
import starepandas.tools.temporal_conversions
import starepandas.io.pod
import multiprocessing
import pickle
import zarr
import s3fs
import os
import json
import datetime

import logging
import time
import copy

from pathlib import Path

_AWS_S3_STORAGE_OPTIONS = {}
_AWS_RDS_OPTIONS = {}

def aws_configure(key=None, secret=None, token=None, region_name=None, endpoint_url=None, client_kwargs=None,
                  rds=None, db_host=None, db_port=None, db_username=None, db_password=None, db_database=None,
                  **s3fs_kwargs):
    """
    Configure default AWS/S3 options for zarr S3 helpers and optional RDS Postgres metadata store.

    Parameters
    - key: AWS access key id
    - secret: AWS secret access key
    - token: AWS session token (optional)
    - region_name: AWS region (e.g., "us-west-2")
    - endpoint_url: Custom S3-compatible endpoint (optional)
    - client_kwargs: dict to merge into s3fs client_kwargs
    - rds: dict with keys {host, port, username, password, database} for RDS connection
    - db_host/db_port/db_username/db_password/db_database: overrides for rds dict
    - **s3fs_kwargs: any additional s3fs.S3FileSystem kwargs

    Notes
    - These options are used by to_zarr_s3/from_zarr_s3 when storage_options is not provided.
    - You can pass a ready-made 'client_kwargs' dict or individual fields like 'region_name'/'endpoint_url'.
    """
    global _AWS_S3_STORAGE_OPTIONS, _AWS_RDS_OPTIONS
    options = dict(s3fs_kwargs) if s3fs_kwargs else {}
    if key is not None:
        options['key'] = key
    if secret is not None:
        options['secret'] = secret
    if token is not None:
        options['token'] = token

    ck = dict(client_kwargs) if client_kwargs else {}
    if region_name is not None:
        ck['region_name'] = region_name
    if endpoint_url is not None:
        ck['endpoint_url'] = endpoint_url
    if ck:
        options['client_kwargs'] = ck

    _AWS_S3_STORAGE_OPTIONS = options

    # Configure RDS/PostgreSQL connection options
    rds_opts = {}
    if isinstance(rds, dict):
        rds_opts.update(rds)
    if db_host is not None:
        rds_opts['host'] = db_host
    if db_port is not None:
        rds_opts['port'] = int(db_port)
    if db_username is not None:
        rds_opts['username'] = db_username
    if db_password is not None:
        rds_opts['password'] = db_password
    if db_database is not None:
        rds_opts['database'] = db_database

    if rds_opts:
        _AWS_RDS_OPTIONS = rds_opts
    return _AWS_S3_STORAGE_OPTIONS

def load_aws_configure(config_path):
    """
    Load AWS/S3 configuration from a JSON file and set defaults for zarr S3 helpers.

    The JSON may contain either s3fs-style keys (key, secret, token, client_kwargs)
    or AWS-style keys (aws_access_key_id, aws_secret_access_key, aws_session_token, region_name, endpoint_url).

    It may also include an 'rds' block with {host, port, username, password, database}, or top-level aliases.
    """
    with open(config_path, 'r') as f:
        data = json.load(f)

    key = data.get('key') or data.get('aws_access_key_id')
    secret = data.get('secret') or data.get('aws_secret_access_key')
    token = data.get('token') or data.get('aws_session_token')

    client_kwargs = data.get('client_kwargs', {})
    region_name = data.get('region_name') or data.get('region')
    endpoint_url = data.get('endpoint_url')

    rds_block = data.get('rds') or {}
    for k in ['host', 'port', 'username', 'password', 'database']:
        if k in data and k not in rds_block:
            rds_block[k] = data[k]

    return aws_configure(
        key=key,
        secret=secret,
        token=token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        client_kwargs=client_kwargs,
        rds=rds_block,
        **{k: v for k, v in data.items() if k not in {
            'key', 'secret', 'token', 'client_kwargs',
            'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token',
            'region', 'region_name', 'endpoint_url', 'rds',
            'host', 'port', 'username', 'password', 'database'
        }}
    )

def _load_config_from_default_locations() -> bool:
    """Try to load configuration from default file locations.

    Order of precedence:
    - Env var STAREPANDAS_AWS_CONFIG
    - ./.config (current working directory)
    - <package_root>/.config (project root next to this module)
    - ~/.starepandas_aws_config.json
    - ~/.starepandas/aws.json
    Returns True if successfully loaded, else False.
    """
    candidates = []
    env_path = os.environ.get('STAREPANDAS_AWS_CONFIG')
    if env_path:
        candidates.append(env_path)
    candidates.append(os.path.join(os.getcwd(), '.config'))
    candidates.append(str(Path(__file__).resolve().parents[1] / '.config'))
    candidates.append(os.path.join(Path.home(), '.starepandas_aws_config.json'))
    candidates.append(os.path.join(Path.home(), '.starepandas', 'aws.json'))

    for cfg in candidates:
        try:
            if os.path.isfile(cfg):
                # Support simple .config key=value pairs as well as JSON
                try:
                    with open(cfg, 'r') as f:
                        txt = f.read().strip()
                    if txt.startswith('{'):
                        load_aws_configure(cfg)
                    else:
                        # Parse simple key=value lines
                        kv = {}
                        for line in txt.splitlines():
                            line = line.strip()
                            if not line or line.startswith('#'):
                                continue
                            if '=' in line:
                                k, v = line.split('=', 1)
                                kv[k.strip()] = v.strip()
                        # Map to expected fields
                        rds_block = {
                            'host': kv.get('rds_host') or kv.get('host'),
                            'port': int(kv.get('port', '5432')),
                            'username': kv.get('username') or kv.get('user'),
                            'password': kv.get('password'),
                            'database': kv.get('database') or 'postgres'
                        }
                        aws_configure(
                            key=kv.get('key'),
                            secret=kv.get('secret'),
                            region_name=kv.get('region_name') or kv.get('region'),
                            rds=rds_block
                        )
                except Exception:
                    # Fallback to JSON loader if parsing failed unexpectedly
                    load_aws_configure(cfg)
                return True
        except Exception:
            continue
    return False

def _ensure_rds_db_and_table(target_dbname='StarePodsMetadata'):
    """
    Ensure the RDS Postgres database and table exist, and return a connection to the target DB.
    Expects _AWS_RDS_OPTIONS with keys: host, port, username, password, database (admin DB to connect first).
    """
    if not _AWS_RDS_OPTIONS:
        # Attempt to auto-load default config file if available
        _load_config_from_default_locations()
    if not _AWS_RDS_OPTIONS:
        raise ValueError(
            "Missing RDS configuration. Call load_aws_configure(config_path) or aws_configure(..., rds=...) "
            "to set RDS connection parameters."
        )

    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError as e:
        raise ImportError("psycopg2 is required for RDS metadata operations. Install 'psycopg2-binary'.") from e

    host = _AWS_RDS_OPTIONS.get('host')
    port = int(_AWS_RDS_OPTIONS.get('port', 5432))
    user = _AWS_RDS_OPTIONS.get('username') or _AWS_RDS_OPTIONS.get('user')
    password = _AWS_RDS_OPTIONS.get('password')
    admin_db = _AWS_RDS_OPTIONS.get('database') or 'postgres'

    if not all([host, user, password]):
        raise ValueError("RDS configuration incomplete: require host, username, password (and optionally port, database).")

    # Connect to admin DB to ensure target DB exists
    admin_conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=admin_db)
    admin_conn.set_session(autocommit=True)
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (target_dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(target_dbname)))
    finally:
        admin_conn.close()

    # Connect to target DB and ensure table
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=target_dbname)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "PodsMetadata" (
                "Dataset" TEXT,
                "DataLevel" TEXT,
                "RawData Collected Time" TIMESTAMP,
                grouped_id INTEGER,
                "S3 bucket" TEXT,
                "Resolution level" INTEGER,
                "MetadataJson" JSONB
            )
            """
        )
        conn.commit()
    return conn

def _parse_s3_bucket(s3_path: str) -> str:
    if not s3_path.startswith('s3://'):
        return ''
    rest = s3_path[5:]
    return rest.split('/', 1)[0] if '/' in rest else rest

DEFAULT_SID_COLUMN_NAME = 'sids'
DEFAULT_TID_COLUMN_NAME = 'tids'
DEFAULT_TRIXEL_COLUMN_NAME = 'trixels'
DEFAULT_GEOMETRY_COLUMN_NAME = 'geometry'

def compress_sids_group(group):
    sids = group[1].to_numpy()  # zero element is group label, 1 element is the df
    if sids.dtype == np.dtype('O'):
        # If we receive a series of SID collections we merge all sids into a single 1D array
        # to_numpy() would have produced an array of lists in this case
        sids = np.concatenate(sids)
    sids = starepandas.compress_sids(sids)
    return tuple([group[0], sids])

def write_pod_pickle(g, fname, append=False, compress=None):
    """Write or append to a pickle."""
    logging.info('Writing to pickle: %s' % fname)
    if append:
        raise NotImplementedError('appending not implemented')
        with starepandas.io.pod.generic_open(fname)(fname, 'a+b') as f:
            pickle.dump(g, f)
    else:
        # Overwrite
        start = time.time()
        if compress == None:
            with open(fname, 'wb') as f:
                pickle.dump(g, f)
                logging.info('Writing chunk %s took %d seconds.' % (fname, time.time() - start))
        elif compress == 'bz2':
            with bz2.open(fname, 'wb') as f:
                pickle.dump(g, f)
                logging.info('Writing bz2 chunk %s took %d seconds.' % (fname, time.time() - start))
        else:
            raise ValueError('write_pod_pickle argument compress="%s" not understood.'%compress)
    return

def write_pod_hdf(g, fname, append=False):
    """Write or append to an HDF file."""
    # raise NotImplementedError
    # if append:
    #     pass
    # else:
    #     pass
    return

class STAREDataFrame(geopandas.GeoDataFrame):
    _metadata = ['_sid_column_name', '_trixel_column_name', '_geometry_column_name', '_tid_column_name']

    _sid_column_name = DEFAULT_SID_COLUMN_NAME
    _trixel_column_name = DEFAULT_TRIXEL_COLUMN_NAME
    _tid_column_name = DEFAULT_TID_COLUMN_NAME
    _geometry_column_name = DEFAULT_GEOMETRY_COLUMN_NAME

    def __init__(self, *args,
                 sids=None, add_sids=False, level=None,
                 trixels=None, add_trixels=False, n_partitions=1,
                 **kwargs):
        """
        A STAREDataFrame object is a pandas.DataFrame that has a special column
        with STARE indices and optionally a special column holding the trixel representation.
        In addition to the standard DataFrame constructor arguments,
        STARE also accepts the following keyword arguments:

        Parameters
        ----------
        sids : str or array-like
            If str, column to use as stare column. If array, will be set as 'stare' column on STAREDataFrame.
        add_sids : bool
            If true, STARE index values will be generated using a geometry column
        level: int
            If add_stare is True, then use level as the maximum STARE level
        trixels : str or array-like
            If str, column to use as trixel column. If array, will be set as 'trixel' column on STAREDataFrame.
        add_trixels : bool
            If true, trixels will be generated from the STARE column.

        Examples
        ---------
        # >>> cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
        # >>> latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
        # >>> longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
        # >>> data =  {'City': cities, 'Latitude': latitudes, 'Longitude': longitudes}
        # >>> sids = starepandas.sids_from_xy(longitudes, latitudes, level=5)
        # >>> sdf = starepandas.STAREDataFrame(data, sids=sids)
        """

        super().__init__(*args, **kwargs)

        if args and isinstance(args[0], (geopandas.GeoDataFrame, STAREDataFrame)):
            self._geometry_column_name = args[0]._geometry_column_name
            # self.set_crs(args[0].crs, inplace=True)

        if sids is not None:
            self.set_sids(sids, inplace=True)
        elif add_sids:
            if level is None:
                raise ValueError('Level has to be specified if SIDs are to be added')
            sids = self.make_sids(level=level, n_partitions=n_partitions)
            self.set_sids(sids, inplace=True)

        if trixels is not None:
            self.set_trixels(trixels, inplace=True)
        elif add_trixels:
            trixels = self.make_trixels(n_partitions=n_partitions)
            self.set_trixels(trixels, inplace=True)

    def __copy__(self):
        new_instance = super().__copy__()  # Call the parent class copy method
        # new_instance = self.copy()
        new_instance.__class__ = STAREDataFrame  # Ensure the correct class type
        return new_instance

    def __deepcopy__(self, memo=None):
        new_instance = super().__deepcopy__(memo)  # Call parent class deepcopy method
        # new_instance = self.copy()
        new_instance.__class__ = STAREDataFrame  # Ensure the correct class type

        # Copy the metadata attributes
        for key in self._metadata:
            setattr(new_instance, key, copy.deepcopy(getattr(self, key), memo))

        return new_instance

    def reset_index(self, inplace=False, drop=False):
        new_instance = super().reset_index(inplace=inplace, drop=drop)
        if not inplace:
            new_instance.__class__ = STAREDataFrame
            return new_instance

    def __getitem__(self, key):

        result = super().__getitem__(key)
        sid_col = self._sid_column_name

        if isinstance(result, (geopandas.GeoDataFrame, pandas.DataFrame, starepandas.STAREDataFrame)):
            result.__class__ = STAREDataFrame
            result._sid_column_name = sid_col
        elif isinstance(result, (geopandas.GeoSeries, pandas.Series)):
            # result.__class__ = starepandas.STARESeries
            pass
        else:
            pass
            # result.__class__ = geopandas.GeoDataFrame
        return result

    def __setattr__(self, attr, val):
        # have to special case geometry b/c pandas tries to use as column...
        if attr == "stare":
            object.__setattr__(self, attr, val)
        else:
            super().__setattr__(attr, val)

    def make_sids(self, level, convex=False, force_ccw=True, n_partitions=1):
        """
        Generates and returns the STARE representation of each feauture.

        Parameters
        -----------
        level: int; 0<=level<=27
            STARE level to use for the STARE lookup
        convex: bool
            Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
        force_ccw: bool
            Toggle if a counterclockwise orientation of the geometries should be enforced. Unfortunately, OGC and ESRI
            have oposing definitions. ([stackexchange](https://gis.stackexchange.com/questions/119150/order-of-polygon-vertices-in-general-gis-clockwise-or-counterclockwise.)).
            [ESRI](http://esri.github.io/geometry-api-java/doc/Polygon.html) defines exterior rings as clockwise, OGC as counterclockwise.
            We use the OGC definition, making it necessary to generally force CCW for polygons loaded from shapefules.
        n_partitions: int
            Number of partititions used to lookup STARE indices in parallel

        Returns
        ---------
        sids: numpy.ndarray
            array of (set of) STARE index values

        Examples
        ----------
        From points

        # >>> import starepandas, geopandas
        # >>> lats = [-72.609177, -72.648590, -72.591286]
        # >>> lons = [-41.255402, -42.054047, -41.625336]
        # >>> geoms = geopandas.points_from_xy(lons, lats)
        # >>> sdf = starepandas.STAREDataFrame(geometry=geoms)
        # >>> sdf.make_sids(level=6, convex=False)
        # 0    2299437706637111654
        # 1    2299435211084507366
        # 2    2299436587616075270
        # Name: sids, dtype: int64
        #
        # From polygons
        #
        # >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        # >>> sdf = starepandas.STAREDataFrame(gdf)
        # >>> sids = sdf.make_sids(level=5)
        """

        sids = starepandas.sids_from_geoseries(self.geometry, level=level, convex=convex,
                                               force_ccw=force_ccw, n_partitions=n_partitions)
        return sids

    def drop_na_sids(self, inplace=False):
        """Drop all rows that have NA values for the SIDs and cast the column to numpy.int64 """
        if inplace:
            self.dropna(subset=[self._sid_column_name], inplace=inplace)
            self[self._sid_column_name] = self[self._sid_column_name].astype(np.dtype('int64'))
        else:
            frame = self.__deepcopy__()
            frame = frame.dropna(subset=[frame._sid_column_name], inplace=inplace)
            frame[frame._sid_column_name] = frame[frame._sid_column_name].astype(np.dtype('int64'))
            return frame

    def make_tids(self, column='ts_start', end_column=None, forward_res=48, reverse_res=48):
        """
        Generates and returns the STARE representation of each feauture.

        Parameters
        -----------
        column: str
            column name containing datetime
        end_column: str
            optional. Column containing the end of the timestamp
        forward_res: int
            forward resolution
        reverse_res: int
            reverse resolution
        Returns
        ---------
        tids: numpy.ndarray
            array of (set of) STARE index values

        Examples
        ----------
        From points

        # >>> import starepandas, geopandas
        """
        # Autoadjust resolution
        start_col = self[column]
        if not pandas.api.types.is_datetime64_any_dtype(start_col.dtype):
            raise TypeError('dtype of column must be numpy.datetime64')

        tids = starepandas.tivs_from_timeseries(self[column],
                                                scale='utc',
                                                format='datetime64',
                                                forward_res=forward_res,
                                                reverse_res=reverse_res)
        return tids

    def set_sids(self, col, inplace=False):
        """ Set the StareDataFrame  spatial indices using either an existing column or
        the specified input. By default, yields a new object.
        The original tid column is replaced with the input.

        Parameters
        -------------
        col: array-like
            f stare sids or column name
        inplace: boolean
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        ---------
        df: STAREDataFrame
            the df with sids

        Examples
        --------
        # >>> import starepandas
        # >>> sdf = starepandas.STAREDataFrame()
        # >>> sids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        # >>> sdf.set_sids(sids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (list, np.ndarray, pandas.Series)):
            frame[frame._sid_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._sid_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def set_tids(self, col, inplace=False):
        """ Set the StareDataFrame temporal indices using either an existing column or
        the specified input. By default, yields a new object.
        The original tid column is replaced with the input.

        Parameters
        -------------
        col: array-like
            f stare tids or column name
        inplace: boolean
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        ---------
        df: STAREDataFrame
            the df with tids

        Examples
        --------
        # >>> import starepandas
        # >>> sdf = starepandas.STAREDataFrame()
        # >>> tids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        # >>> sdf.set_tids(tids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (list, np.ndarray, pandas.Series)):
            frame[frame._tid_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._tid_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def has_trixels(self):
        return self._trixel_column_name in self

    def has_sids(self):
        return self._sid_column_name in self

    def make_trixels(self, sid_column=None, n_partitions=1, wrap_lon=True, num_workers=None):
        """
        Returns a Polygon or Multipolygon GeoSeries
        containing the trixels referred by the STARE indices

        Parameters
        -----------
        sid_column: str
            Column to use as STARE column. Default: 'stare'
        n_partitions: int
            number of (dask) workers to use to generate trixels
        wrap_lon: bool
            toggle if trixels should be wraped around antimeridian.

        num_workers: int
            number of workers to use

        Returns
        -----------
        trixels_series: numpy.array
            array of polygons or multipolygons representing the trixels

        Examples
        --------
        # >>> import starepandas
        # >>> sids = [648518346341351428, 900719925474099204, 1170935903116328964]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> trixels = sdf.make_trixels()
        """

        if sid_column is None:
            sid_column = self._sid_column_name
        if sid_column not in list(self.columns):
            raise Exception('sids column does not exist')
        trixels_series = starepandas.tools.trixel_conversions.trixels_from_stareseries(self[sid_column],
                                                                                       n_partitions=n_partitions,
                                                                                       num_workers=num_workers,
                                                                                       wrap_lon=wrap_lon)
        return trixels_series

    def add_trixels(self, n_partitions=1, num_workers=None, inplace=False, wrap_lon=True):
        """Combination of make_trixels() and set_trixels()"""
        sid_column = self._sid_column_name
        trixels = self.make_trixels(sid_column=sid_column, n_partitions=n_partitions,
                                    num_workers=num_workers, wrap_lon=wrap_lon)

        return self.set_trixels(trixels, inplace=inplace)

    def set_trixels(self, col, inplace=False):
        """
        Set the trixel column

        Parameters
        ------------
        col: array-like or string
            If array like, will add the array as a new trixel column. If string, will set the df['col']
            as the trixel column. If None, will generate trixels from the STARE column.
        inplace: bool
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        -------
        df: DataFrame
            DataFrame or None


        Examples
        ---------
        # >>> import starepandas
        # >>> sids = [4611686018427387903, 4611686018427387903, 4611686018427387903]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> trixels = sdf.make_trixels()
        # >>> sdf.set_trixels(trixels, inplace=True)
        """

        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (pandas.Series, geopandas.GeoSeries, list, np.ndarray)):
            col = geopandas.geodataframe._ensure_geometry(col)
            frame[frame._trixel_column_name] = col
        elif isinstance(col, str) and col in self.columns:
            frame._trixel_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")
        # frame.set_geometry(col, inplace=True)

        if not inplace:
            return frame

    def trixel_vertices(self):
        """ Returns the vertices and centerpoints of the trixels.
        Requires stare column to be set. Vertices are a tuple of:

        1. the latitudes of the corners
        2. the longitudes of the corners
        3. the latitudes of the centers
        4. the longitudes of the centers

        Returns
        ---------
        vertices
            A vertices data structure

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_vertices()
        (array([29.9999996 , 45.00000069, 29.9999996 ]), array([-170.26439001,  -45.        ,   80.26439001]), array([80.264389]), array([135.]))
        """
        return starepandas.tools.trixel_conversions.to_vertices(self[self._sid_column_name])

    def trixel_centers(self, vertices=None):
        """ Returns the trixel centers.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        --------------
        vertices: vertices data structure
            If set, the centers are extracted from the vertices data structure.

        Returns
        ---------
        trixel_centers : numpy.array
            Trixel centers. First dimension are the SIDs, second dimension lon/lat.

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_centers()
        array([[134.9      ,  80.264389]])
        """

        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers(self[self._sid_column_name])

    def trixel_centers_ecef(self, vertices=None):
        """ Returns the trixel centers as ECEF vectors.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        --------------
        vertices: vertices data structure
            If set, the centers are extracted from the vertices data structure.

        Returns
        ---------
        trixel_centers : numpy.array
            Trixel centers. First dimension are the sids, second dimension are x/y/z.

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_centers_ecef()
        array([[-0.11957316,  0.11957316,  0.98559856]])
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers_ecef(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers_ecef(self[self._sid_column_name])

    def trixel_centerpoints(self, vertices=None):
        """ Returns the trixel centers as shapely points.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        ----------------
        vertices: tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ---------
        trixel_centerpoints: Geometery Array
            Series of shapely trixel center points

        Examples
        ---------
        # >>> sids = np.array([4458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> centers = df.trixel_centerpoints()
        # >>> print(centers[0])
        POINT (18.4 24.09)
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centerpoints(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centerpoints(self[self._sid_column_name])

    def trixel_corners(self, vertices=None, from_trixels=False):
        """ Returns corners of trixels as lon/lat.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If from_trixels is True and dataframe contains trixel column, corners are extracted from trixels.
        If not, corners are generated from stare column

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        from_trixels: bool
            If true and dataframe contains trixel column, corners are extracted from trixels.

        Returns
        ----------
        corners : numpy array
            Corners of the trixels in lon/lat representation. First dimension are the SIDs,
            second dimension the corners (1 through 3), third dimension lon/lat.

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_corners()
        array([[[-170.26439001,  29.9999996 ],
                [ -45.        ,  45.00000069],
                [  80.26439001,  29.9999996 ]]])
        """

        if vertices:
            corners = starepandas.tools.trixel_conversions.vertices2corners(vertices)
        elif from_trixels and self._trixel_column_name in self.columns:
            corners = []
            for trixel in self[self._trixel_column_name]:
                # Trixel is a polygon. Its first element is the outer ring.
                corners.append(tuple(trixel[0].boundary.coords)[0:3])
        else:
            corners = starepandas.tools.trixel_conversions.to_corners(self[self._sid_column_name])
        return corners

    def trixel_corners_ecef(self, vertices=None):
        """ Returns ECEF norm vectors of great circles constraining the trixels.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If not, corners are generated from stare column.

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ----------
        corners : numpy array
            Corners of the trixels in ECEF representation. First dimension are the sids, second
            dimension the great circles, third dimension x/y/z

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_corners_ecef()
        array([[[-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679],
                [ 0.14644661,  0.85355339,  0.49999999]]])
        """
        corners = self.trixel_corners(vertices)
        corners_ecef = starepandas.tools.trixel_conversions.corners2ecef(corners)
        return corners_ecef

    def trixel_grings(self, vertices=None):
        """ Returns corners of trixels as ECEF.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If not, corners are generated from stare column

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ----------
        corners : numpy array
            ECEF norm vectors of great circles constraining the trixels. First dimension are the sids, second
            dimension the great circles, third dimension x/y/z

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_grings()
        array([[[ 0.14644661,  0.85355339,  0.49999999],
                [-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679]]])
        """

        corners = self.trixel_corners_ecef(vertices)
        gring = starepandas.tools.trixel_conversions.corners2gring(corners)
        return gring

    def split_antimeridian(self, inplace=False, drop=False, trixel_column_name=None):
        """Splits trixels at the antimeridian

        This works on trixels that cross the meridian and whose longitudes have *not* been wrapped around the
        antimeridian. I.e. when creating the trixels use sdf.make_trixels(wrap_lon=False)


        Examples
        ----------
        # >>> cities = {'name': ['midway', 'Fiji', 'Baker', 'honolulu'],
        # ...           'lat': [28.2, -17.8,  0.2, 21.3282956],
        # ...           'lon': [-177.35, 178.1, -176.7, -157.9]}
        # >>> sdf = starepandas.STAREDataFrame(cities)
        # >>> sids = starepandas.sids_from_xy(sdf.lon, sdf.lat, level=1)
        # >>> sdf.set_sids(sids, inplace=True)
        # >>> trixels = sdf.make_trixels(wrap_lon=False)
        # >>> sdf.set_trixels(trixels, inplace=True)
        # >>> cites_split = sdf.split_antimeridian(inplace=False)
        # >>> max(max(cites_split.trixels[1].geoms[0].exterior.xy))
        # 180.0

        """
        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        if not trixel_column_name:
            trixel_column_name = df._trixel_column_name

        trixels = geopandas.GeoSeries(df[trixel_column_name])
        split = starepandas.tools.trixel_conversions.split_antimeridian_series(trixels, drop=drop)

        df[df._trixel_column_name] = split

        if not inplace:
            return df

    def plot(self, trixels=True, boundary=True, **kwargs):
        """ Generate a plot with matplotlib.
        Seminal method to
        `GeoDataFrame.plot() <https://geopandas.org/docs/reference/api/geopandas.GeoDataFrame.plot.html>`_
        All GeoDataFrame.plot() kwargs are available.

        Parameters
        ----------
        trixels: bool
            Toggle if trixels (rather than the SF geometry) is to be plotted
        boundary: bool
            Toggle if the ring is to be plotted as a linestring rather than the polygon. Only relevant if trixels==True

        Examples
        --------
        # >>> import starepandas
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> germany = world[world.name=='Germany']
        # >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=8, add_trixels=True, n_partitions=1)
        # >>> ax = germany.plot(trixels=True, boundary=True, color='y', zorder=0)
        """
        df = self.__deepcopy__()

        if trixels:
            if not self.has_trixels():
                raise AttributeError('No trixels set (expected in "{}" column)'.format(self._trixel_column_name))
            df.set_geometry(self._trixel_column_name, inplace=True)
            if boundary:
                df = df[df.geometry.is_empty == False]
                df = df.set_geometry(df.geometry.boundary)
        else:
            df.set_geometry(self._geometry_column_name, inplace=True)
        return geopandas.plotting.plot_dataframe(df, **kwargs)

    def to_scidb(self, connection):
        pass

    def stare_intersects(self, other, method='binsearch', n_partitions=1, num_workers=None):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that intersects `other`.
        An object is said to intersect `other` if its `ring` and `interior`
        intersects in any way with those of the other.

        Parameters
        -------------
        other: int or listlike
            The SID collection representing the spatial object to test if is intersected.
        method: str
            Method for STARE intersects test 'skiplist', 'binsearch' or 'nn'. Default: 'binsearch'.
        n_partitions: int
            number of dask dataframe partitions to use
        num_workers: int:
            number of dask workers to use

        Examples
        --------
        # >>> germany = [4251398048237748227, 4269412446747230211, 4278419646001971203,
        # ...            4539628424389459971, 4548635623644200963, 4566650022153682947]
        # >>> cities = {'name': ['berlin', 'madrid'], 'sid': [4258121269174388239, 4288120002905386575]}
        # >>> cities = starepandas.STAREDataFrame(cities, sids='sid')
        # >>> cities.stare_intersects(germany)
        0     True
        1    False
        dtype: bool
        """

        if isinstance(other, (int, np.int64)):
            # Other is a single STARE index value
            other = [other]
        elif isinstance(other, (np.ndarray, list)):
            # Other is a collection/set of STARE index values
            pass
        else:
            raise ValueError("Other must be array-like object or int64")

        intersects = starepandas.series_intersects(other=other,
                                                   series=self[self._sid_column_name],
                                                   method=method,
                                                   n_partitions=n_partitions,
                                                   num_workers=num_workers)
        return pandas.Series(intersects, index=self.index)

    def stare_disjoint(self, other, method='binsearch', n_partitions=1, num_workers=None):
        """  Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that is disjoint from `other`.
        This is the inverse operation of STAREDataFrame.stare_intersects()

        Parameters
        ------------
        other: array-like
            The STARE index collection representing the spatial object to test if is intersected.
        method: str
            Method for STARE intersects test 'skiplist', 'binsearch' or 'nn'. Default: 'binsearch'.
        n_partitions: int
            number of dask dataframe partitions to use
        num_workers: int:
            number of dask workers to use

        See also
        --------
        STAREDataFrame.stare_intersects : intersects test

        """
        return ~self.stare_intersects(other, method, n_partitions, num_workers)

    def stare_intersection(self, other):
        """Returns a ``STARESeries`` of the (STARE) spatial intersection of self with `other`.

        Parameters
        ------------
        other : Array-like
            The STARE index value collection representing the object to find the intersection with.

        Returns
        --------
        intersection : STARESeries
            A series of STARE index values representing the STARE interesection of each feature with other

        Examples
        ---------
        # >>> import shapely
        # >>> nodes1 = [[102, 33], [101, 35], [105, 34], [104, 33], [102, 33]]
        # >>> nodes2 = [[102, 34], [106, 35], [106, 33], [102, 33.5], [102, 34]]
        # >>> polygon1 = shapely.geometry.Polygon(nodes1)
        # >>> polygon2 = shapely.geometry.Polygon(nodes2)
        # >>> sids1 = starepandas.sids_from_polygon(polygon1, level=5, force_ccw=True)
        # >>> sids2 = starepandas.sids_from_polygon(polygon2, level=5, force_ccw=True)
        #
        # >>> df = starepandas.STAREDataFrame(sids=[sids1])
        # >>> df.stare_intersection(sids2).iloc[0]
        # array([694117292568477701, 701435641962954757, 701998591916376069])
        """
        data = []
        for srange in self[self._sid_column_name]:
            data.append(pystare.intersection(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, by=None, num_workers=1, geom=False, aggfunc="first", **kwargs):
        """
        Dissolves a dataframe subject to a field. I.e. grouping by a field/column.
        Seminal method to [GeoDataFrame.dissolve()](https://geopandas.org/en/stable/docs/user_guide/aggregation_with_dissolve.html)

        stare_dissolve() can be thought of as doing three things:
        - it dissolves all the SIDs within a given group together into a single set o SIDs (this means a) removing duplicate SIDs b) replacing 4 child SIDs with the parent SID), and
        - it aggregates all the rows of data in a group using groupby.aggregate, and
        - it combines those two results.

        Parameters
        -------------
        by: str
            column to use the dissolve on. If None, dissolve all rows.
        num_workers: int
            workers to use for the dissolve
        geom: bool
            Toggle if the geometry column is to be dissolved. Geom column Will be dropped if set to False.
        aggfunc: str
            aggregation function. E.g. 'first', 'sum', 'mean'.

        Examples
        --------
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> west = world[world['continent'].isin(['Europe', 'North America'])]
        # >>> west = starepandas.STAREDataFrame(west, add_sids=True, level=4, add_trixels=False)
        # >>> west.stare_dissolve(by='continent', aggfunc='sum') # doctest: +SKIP
        #                                                            stare  ...  gdp_md_est
        # continent                                                         ...
        # Europe         [648518346341351428, 900719925474099204, 10448...  ...  25284877.0
        # North America  [1170935903116328964, 1173187702930014212, 117...  ...  23505137.0
        """
        if by is None:
            sids = self[self._sid_column_name].to_numpy()
            if sids.dtype == np.dtype('O'):
                # If we receive a series of SID collections we merge all sids into a single 1D array
                # to_numpy() would have produced an array of lists in this case
                sids = np.concatenate(sids)
            sids = starepandas.compress_sids(sids)
            return sids
        else:
            data = self.drop(columns=[self._sid_column_name, self._trixel_column_name], errors='ignore')
            if geom:
                aggregated_data = data.dissolve(by=by, aggfunc=aggfunc, **kwargs)
            else:
                data = data.drop(columns=[self._geometry_column_name], errors='ignore')
                aggregated_data = data.groupby(by=by, **kwargs).agg(aggfunc)

        sids_groups = self.groupby(group_keys=True, by=by)[self._sid_column_name]

        if num_workers == 1:
            dissolved = []
            for group in sids_groups:
                dissolved.append(compress_sids_group(group))
        else:
            with multiprocessing.Pool(processes=num_workers) as pool:
                dissolved = pool.map(compress_sids_group, [group for group in sids_groups])

        sdf = STAREDataFrame(dissolved, columns=[by, self._sid_column_name])
        sdf.set_index(by, inplace=True)
        sdf.set_sids(self._sid_column_name, inplace=True)

        aggregated = sdf.join(aggregated_data)
        aggregated.__class__ = STAREDataFrame
        return aggregated

    def spatial_level(self):
        """
        Returns the spatial level of each feature
        """
        sids = self[self._sid_column_name]
        return pystare.spatial_resolution(sids)

    def trixel_area(self, r=None):
        """
        Returns the approximate area of the trixel

        Parameters
        -------------
        r: float or int
             earth radius
        """
        sids = self[self._sid_column_name]
        solid_angel = pystare.to_area(sids)
        if r is None:
            return solid_angel
        else:
            return solid_angel * r ** 2

    def to_sids_level(self, level, inplace=False, clear_to_level=False):
        """
        Changes level of STARE index values to level; optionally clears location bits up to level.
        Caution: This method is not intended for use on features represented by sets of sids.

        Parameters
        ------------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).
        level: int
            STARE level to change to.
        clear_to_level: bool
            Toggle if the location bits below level should be cleared

        Returns
        -------------
        if not inplace, returns stare index values, otherwise None

        Examples
        --------
        # >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> sdf.to_sids_level(level=6, clear_to_level=False)
        #                   sids
        # 0  2299437706637111718
        # 1  2299435211084507590
        # 2  2299566194809236966
        """

        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids = df[df._sid_column_name]
        if pandas.api.types.is_integer_dtype(sids):
            # We have column of single SIDs and can send whole column to pystare
            sids = sids.astype(np.dtype('int64'))
            sids = pystare.spatial_coerce_resolution(sids, level)

            if clear_to_level:
                # pystare_terminator_mask uses << operator, which requires us to cast to numpy array first
                sids = pystare.spatial_clear_to_resolution(np.array(sids))
        else:
            pass

        df[df._sid_column_name] = sids
        if not inplace:
            return df

    def clear_to_level(self, inplace=False):
        """
        Clears location bits to level

        Parameters
        -----------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Examples
        ----------
        # >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> sdf.clear_to_level(inplace=False)
        #                   sids
        # 0  2299437254470270985
        # 1  2299435055447015433
        # 2  2299564797819093001

        """
        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids = df[df._sid_column_name]
        sids = pystare.spatial_clear_to_resolution(np.array(sids))

        df[df._sid_column_name] = sids
        if not inplace:
            return df

    def to_sids_singlelevel(self, level=None, inplace=False):
        """
        Changes the STARE index values to single level representation (in contrary to multiresolution).

        Parameters
        -----------
        level: int
            level to change the sids to
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Returns
        ------------
        if not inplace, returns stare index values, otherwise None

        Examples
        ---------
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> germany  = world[world.name=='Germany']
        # >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=6, add_trixels=False)
        # >>> len(germany.sids.iloc[0])
        # 43
        # >>> germany_singleres = germany.to_sids_singlelevel()
        # >>> len(germany_singleres.sids.iloc[0])
        # 46
        """

        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids_col = df[df._sid_column_name]

        new_sids_col = []
        for sids in sids_col:
            if level:
                r = level
            else:
                r = int(pystare.spatial_resolution(sids).max())
            sids = pystare.expand_intervals(sids, level=r, multi_resolution=False)
            new_sids_col.append(sids)

        df[df._sid_column_name] = new_sids_col
        if not inplace:
            return df

    def hex(self):
        """
        Returns the hex16 representation of the stare column

        Examples
        ---------
        # >>> sdf = starepandas.STAREDataFrame(sids=[2251799813685252, 4503599627370500])
        # >>> sdf.hex()
        # ['0x0008000000000004', '0x0010000000000004']
        #
        # >>> sdf = starepandas.STAREDataFrame(sids=[[2251799813685252, 4503599627370500],
        # ...                                        [4604930618986332164, 4607182418800017412]])
        # >>> sdf.hex()
        # [['0x0008000000000004', '0x0010000000000004'], ['0x3fe8000000000004', '0x3ff0000000000004']]
        """

        sids = []
        for row in self[self._sid_column_name]:
            try:
                # Ducktyping collection of sids
                sids.append(list(map(pystare.int2hex, row)))
            except TypeError:
                sids.append(pystare.int2hex(row))
        return sids

    def write_pods_spatial(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                           compress=None
                           ):
        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{chunk_name}' if path_format is None else path_format
        pods_written = []

        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        for group in grouped.groups:
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:
                continue
            g = grouped.get_group(group)
            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            # Original
            # g.to_pickle('{pod_root}/{pod}/{chunk_name}'.format(pod_root=pod_root, pod=pod, chunk_name=chunk_name))

            # New MLR 2022-1117-1
            # Note: with the following approach we could update a headr that includes extent information.
            #
            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()

            fname = path_format.format(pod_path_format=dname, chunk_name=chunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        return pods_written

    def write_pods_granule(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                           compress=None
                           ):
        start0 = time.time()
        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{tchunk_name}-{chunk_name}' if path_format is None else path_format

        pods_written = []

        start = time.time()
        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        logging.info('Grouping chunk %s took %d seconds.' % (chunk_name, time.time() - start))

        for group in grouped.groups:

            # Future            
            # self.write_pods_granule_group(self,(group,pod_path_format,pod_root,chunk_name))            
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:  # This cannot be right. group is a dictionary.
                continue

            start = time.time()
            g = grouped.get_group(group)
            logging.info('Get group %s took %d seconds.' % (group, time.time() - start))

            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()
                pass

            # One might cheat and use the fact that ts_start and ts_end are for the granule, so index to [0]
            start = time.time()
            t_mnmx = (self['ts_start'].min(), self['ts_end'].max())
            logging.info('Get group %s min/max took %d seconds.' % (group, time.time() - start))

            start = time.time()
            dt_mnmx = [t.to_pydatetime() for t in t_mnmx]
            ds_tid = pystare.tiv_from_datetime2(dt_mnmx)
            logging.info('Get group %s min/max tiv took %d seconds.' % (group, time.time() - start))

            # ds_tpod = pystare.make_tpod_tuple(ds_tid,temporal_resolution)
            # tpod        = pystare.hex16(ds_tpod[0])
            tchunk_name = pystare.hex16(ds_tid)
            fname = path_format.format(pod_path_format=dname, chunk_name=chunk_name, tchunk_name=tchunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        logging.info('write_pods_granule chunk %s took %d seconds total.' % (chunk_name, time.time() - start0))
        return pods_written

    def write_pods_tpod(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                        temporal_chunking_resolution=16, compress=None
                        ):
        """
        Parameters
        ----------
        pod_root: str
        level: int
        chunk_name: str
        hex: bool
            toggle hex
        path_format
        append: bool
        temporal_chunking_resolution: int
            defaults to 16 (28 days)

        Returns
        -------

        """

        """
        TODO: Add temporal partitioning. Currently broken.
        """
        raise NotImplementedError

        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{tpod_name}-{tchunk_name}-{chunk_name}' if path_format is None else path_format
        pods_written = []

        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        for group in grouped.groups:
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:  # cannot be right. group is a dictionary
                continue
            g = grouped.get_group(group)
            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()
                pass

            t_mnmx = self.ts_start.min(), self.ts_end.max()
            dt_mnmx = [t.to_pydatetime() for t in t_mnmx]
            ds_tid = pystare.tiv_from_datetime2(dt_mnmx)

            tpod_name = pystare.format_tpod(pystare.make_tpod_tuple(ds_tid, temporal_chunking_resolution))

            # ds_tpod = pystare.make_tpod_tuple(ds_tid,temporal_resolution)
            # tpod        = pystare.hex16(ds_tpod[0])
            tchunk_name = pystare.hex16(ds_tid)
            fname = path_format.format(pod_path_format=dname,
                                       tpod_name=tpod_name,
                                       tchunk_name=tchunk_name,
                                       chunk_name=chunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        return pods_written

    ### Just stashing this here for the moment.

    ### if temporal_chunking:
    ###     # link other pods to this one? sigh... no chunking really.
    ###     tpods = pystare.pods_in_query(ds_tid,temporal_resolution)
    ###     for tp_ in tpods:
    ###         tp=pystare.hex16(tp_[0])
    ###         if tp != tpod:
    ###             dst_name = path_format.format(pod_root=pod_root
    ###                                    , pod=pod
    ###                                    , chunk_name=chunk_name
    ###                                    , tpod=tp
    ###                                    , tchunk_name=tchunk_name
    ###                                    )
    ###             os.symlink(fname,dst_name) # creates dst_name symlinking to fname (the src)

    def write_pods(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                   temporal_chunking=None, compress=None):
        """ Writes dataframe into a STAREPods hierarchy.

        Appends the dataframe to the pod (pickle), if it exists.

        Returns list of pods written.

        Parameters
        --------------
        pod_root: str
            Root directory of STAREPods
        level: int
            level of STAREPods
        chunk_name: str
            name of the pod
        hex: bool
            toggle pods being hex vs int
        path_format: str
            defines how pods are to be named 
            default: '{pod_root}/{pod}/{chunk_name}'
        append: bool
            toggle appending to existing pods (default: False)
            Not implemented.
        temporal_chunking: dict
            toggle writing into temporal pods (default: None)
            Supported options...
            - {'partitioning':'granule'}
            - {'partitioning':'pod','resolution':16 } # 16 => month chunk (28 days)
        """

        if temporal_chunking is None:
            return self.write_pods_spatial(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                           path_format=path_format, append=append, compress=compress)
        elif temporal_chunking['partitioning'] == 'granule':
            return self.write_pods_granule(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                           path_format=path_format, append=append, compress=compress)
        elif temporal_chunking['partitioning'] == 'pod':
            return self.write_pods_tpod(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                        path_format=path_format, append=append, compress=compress,
                                        temporal_chunking_resolution=temporal_chunking['resolution'])
        else:
            raise (Exception('Pod configuration not supported. temporal_chunking = %s' % (temporal_chunking)))

    @property
    def _constructor(self):
        return STAREDataFrame

    def to_array(self, column, shape=None, pivot=False):
        """Converts the 'column' to a numpy array.

        Either a shape argument has to be provided or the dataframe has to contain a column x and y
        holding the original array coordinates.

        If the dataframe has x/y columns, the column can also be pivoted. I.e. rather than
        reshaping according to the shape, pivoted along the x/y columns.
        This may be relevant if the dataframe's row order has changed.

        Parameters
        ----------
        column: str
            column name to be converted to an array
        shape: tuple
            x and y shape of the array. x*y has to equal the length of the dataframe
        pivot: bool
            if true, rather than simple reshaping, the dataframe is pivoted along the x and y column

        Examples
        ----------
        # >>> df = starepandas.STAREDataFrame({'x': [0, 0, 1, 1],
        # ...                                  'y': [1, 0, 0, 1],
        # ...                                  'a': [1, 2, 3, 4]})
        # >>> df.to_array('a', pivot=False)
        # array([[1, 2],
        #        [3, 4]])
        #
        # >>> df.to_array('a', pivot=True)
        # array([[2, 1],
        #        [3, 4]])
        #
        # See also
        --------
        STAREDataFrame.to_arrays

        """
        if shape is None:
            shape = (max(self['x']) + 1, max(self['y']) + 1)

        if pivot:
            array = self.pivot(index='x', columns='y', values=column).to_numpy()
        else:
            array = self[column].to_numpy().reshape(shape)
        return array

    def to_sids_array(self, shape=None, pivot=False):
        return self.to_array(self._sid_column_name, shape, pivot)

    def to_arrays(self, shape=None, pivot=False):
        """ Converts a STAREDataFrame into a dictionary of arrays; one array per column/field.

        This may be useful to write data back to granules.
        Either a shape argument has to be provided or the dataframe has to contain a column x and y
        holding the original array coordinates.
        If no shape is provided, the shape is assumed to be (max(x)+1, max(y)+1).

        If the dataframe has x/y columns, the column can also be pivoted. I.e. rather than
        reshaping according to the shape, pivoted along the x/y columns.
        This may be relevant if the dataframe's row order has changed.

        Parameters
        ----------
        shape: tuple
            x and y shape of the array. x*y has to equal the length of the dataframe
        pivot: bool
            if true, rather than simple reshaping, the dataframe is pivoted along the x and y column

        See also
        ---------
        STAREDataFrame.to_array
        """

        arrays = {}

        for column in self.columns:
            if column in ['x', 'y']:
                continue
            arrays[column] = self.to_array(column, shape=shape, pivot=pivot)

        return arrays

    def to_zarr_s3(self, s3_path, level, chunk_size=250000, storage_options=None,
                   dataset=None, data_level=None, raw_collected_time=None, metadata=None):
        """
        Partition STAREDataFrame by SIDs at specified level and write to S3 in grouped layout.
        
        Layout: s3_path/<grouped_id>/[one array per column + __row_positions__]
        
        Parameters
        ----------
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
            
        Returns
        -------
        str
            The S3 path where data was written
        """
        # Resolve storage options: use per-call options over configured defaults
        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if not merged_opts:
            # Attempt to auto-load default config file if available
            _load_config_from_default_locations()
            merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if storage_options:
            merged_opts.update(storage_options)
        if not merged_opts:
            raise ValueError(
                "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
                "to set credentials/region, or pass storage_options to to_zarr_s3."
            )

        # Ensure grouping by coerced SIDs and preserve encounter order of groups
        coerced = self.to_sids_level(level=level, clear_to_level=True)
        grouped = self.groupby(coerced[self._sid_column_name], sort=False)

        # Record original row order so we can reconstruct the exact order on read
        original_positions = pandas.Series(np.arange(len(self), dtype=np.int64), index=self.index)

        # Prepare RDS connection and metadata defaults
        conn = _ensure_rds_db_and_table('StarePodsMetadata')
        bucket_name = _parse_s3_bucket(s3_path)
        ts = raw_collected_time if raw_collected_time is not None else datetime.datetime.utcnow()
        base_meta = dict(metadata or {})

        # Write each group to its own zarr group under s3_path/<group_id>
        for group_id, gdf in grouped:
            # Skip invalid groups if any
            if isinstance(group_id, (int, np.integer)) and group_id < 0:
                continue

            group_path = f"{s3_path}/{group_id}"
            zg = zarr.open_group(group_path, mode="w", storage_options=merged_opts)

            # Per-group arrays for each column
            for col in self.columns:
                values = gdf[col].to_numpy()
                if values.dtype == np.dtype('O'):
                    values = values.astype('U')
                zg.empty(name=col, shape=(len(values),), dtype=values.dtype, chunks=(min(chunk_size, max(1, len(values))),))[:] = values

            # Hidden helper to preserve original order
            row_pos = original_positions.loc[gdf.index].to_numpy(dtype=np.int64)
            zg.empty(name="__row_positions__", shape=(len(row_pos),), dtype=row_pos.dtype, chunks=(min(chunk_size, max(1, len(row_pos))),))[:] = row_pos

            # Insert metadata row into RDS for this group
            try:
                # best-effort to fit grouped_id into 4-byte signed int, also store full in json
                full_gid = int(group_id)
                gid32 = full_gid & 0x7fffffff
                meta_row = dict(base_meta)
                meta_row.update({
                    'grouped_id_full': full_gid,
                    'group_path': group_path,
                    'num_rows': int(len(gdf)),
                    'columns': list(self.columns),
                })
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO "PodsMetadata" ("Dataset", "DataLevel", "RawData Collected Time", grouped_id, "S3 bucket", "Resolution level", "MetadataJson")\
                         VALUES (%s, %s, %s, %s, %s, %s, %s)',
                        (
                            dataset, data_level, ts, gid32, bucket_name, int(level), json.dumps(meta_row)
                        )
                    )
            except Exception:
                # Do not fail writing data due to metadata issues
                pass

        try:
            conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return s3_path
    
    def to_zarr_local(self, local_path, level, chunk_size=250000):
        """
        Partition STAREDataFrame by SIDs at specified level and write to local storage in grouped layout.
        
        Layout: local_path/<grouped_id>/[one array per column + __row_positions__]
        
        Parameters
        ----------
        local_path : str
            Local path where the zarr root directory will be created
        level : int
            STARE level for partitioning SIDs
        chunk_size : int, optional
            Size of chunks for zarr arrays (default: 250000)
            
        Returns
        -------
        str
            The local path where data was written
        """
        # Ensure root directory exists
        os.makedirs(local_path, exist_ok=True)

        # Group by SIDs at the specified level, preserving encounter order
        coerced = self.to_sids_level(level=level, clear_to_level=True)
        grouped = self.groupby(coerced[self._sid_column_name], sort=False)

        # Record original row order
        original_positions = pandas.Series(np.arange(len(self), dtype=np.int64), index=self.index)

        # Write each group to its own zarr group under local_path/<group_id>
        for group_id, gdf in grouped:
            if isinstance(group_id, (int, np.integer)) and group_id < 0:
                continue

            group_dir = os.path.join(local_path, str(group_id))
            zg = zarr.open_group(group_dir, mode="w")

            for col in self.columns:
                values = gdf[col].to_numpy()
                if values.dtype == np.dtype('O'):
                    values = values.astype('U')
                zg.empty(name=col, shape=(len(values),), dtype=values.dtype, chunks=(min(chunk_size, max(1, len(values))),))[:] = values

            row_pos = original_positions.loc[gdf.index].to_numpy(dtype=np.int64)
            zg.empty(name="__row_positions__", shape=(len(row_pos),), dtype=row_pos.dtype, chunks=(min(chunk_size, max(1, len(row_pos))),))[:] = row_pos

        return local_path
    
    @classmethod
    def from_zarr_s3(cls, s3_path, storage_options=None):
        """
        Read STAREDataFrame from S3 grouped zarr store written by to_zarr_s3.
        
        Parameters
        ----------
        s3_path : str
            S3 path to the zarr root directory
        storage_options : dict, optional
            S3 storage options including credentials and region
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame in original row order
        """
        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if not merged_opts:
            _load_config_from_default_locations()
            merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if storage_options:
            merged_opts.update(storage_options)
        if not merged_opts:
            raise ValueError(
                "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
                "to set credentials/region, or pass storage_options to from_zarr_s3."
            )
        fs = s3fs.S3FileSystem(**merged_opts)

        # Discover immediate child prefixes that are zarr groups (contain .zgroup)
        try:
            entries = fs.ls(s3_path)
        except Exception:
            entries = []

        group_dirs = []
        for entry in entries:
            # Ensure path is a directory-like and has a .zgroup
            candidate = entry.rstrip('/')
            if fs.exists(candidate + '/.zgroup'):
                group_dirs.append(candidate)

        # Read each group's arrays into a DataFrame and collect
        frames = []
        for gpath in group_dirs:
            zg = zarr.open_group(gpath, mode="r", storage_options=merged_opts)
            cols = [name for name in zg.array_keys() if name != "__row_positions__"]
            data = {}
            for name in cols:
                arr = zg[name][:]
                if arr.dtype.kind == 'U':
                    arr = arr.astype('O')
                data[name] = arr
            df_part = pandas.DataFrame(data)
            row_pos = zg["__row_positions__"][:].astype(np.int64)
            df_part["__row_pos__"] = row_pos
            frames.append(df_part)

        if not frames:
            return cls()

        df = pandas.concat(frames, ignore_index=True)
        df.sort_values("__row_pos__", inplace=True)
        df.drop(columns=["__row_pos__"], inplace=True)

        return cls(df)
    
    @classmethod
    def from_zarr_local(cls, local_path):
        """
        Read STAREDataFrame from local grouped zarr store written by to_zarr_local.
        
        Parameters
        ----------
        local_path : str
            Local path to the zarr root directory
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame in original row order
        """
        if not os.path.isdir(local_path):
            # Nothing to read
            return cls()

        # Discover child directories that contain a .zgroup file
        group_dirs = []
        for name in os.listdir(local_path):
            candidate = os.path.join(local_path, name)
            if os.path.isdir(candidate) and os.path.exists(os.path.join(candidate, '.zgroup')):
                group_dirs.append(candidate)

        frames = []
        for gdir in group_dirs:
            zg = zarr.open_group(gdir, mode="r")
            cols = [n for n in zg.array_keys() if n != "__row_positions__"]
            data = {}
            for n in cols:
                arr = zg[n][:]
                if arr.dtype.kind == 'U':
                    arr = arr.astype('O')
                data[n] = arr
            df_part = pandas.DataFrame(data)
            row_pos = zg["__row_positions__"][:].astype(np.int64)
            df_part["__row_pos__"] = row_pos
            frames.append(df_part)

        if not frames:
            return cls()

        df = pandas.concat(frames, ignore_index=True)
        df.sort_values("__row_pos__", inplace=True)
        df.drop(columns=["__row_pos__"], inplace=True)

        return cls(df)

    def to_pickle_s3(self, s3_path, storage_options=None, compress=None):
        """
        Write STAREDataFrame to S3 as pickle file.
        
        Parameters
        ----------
        s3_path : str
            S3 path where the pickle file will be written (e.g., "s3://bucket/granule_name.pkl")
        storage_options : dict, optional
            S3 storage options including credentials and region
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        str
            The S3 path where data was written
        """
        import s3fs
        
        # Create S3 filesystem
        fs = s3fs.S3FileSystem(**storage_options or {})
        
        # Write pickle to S3
        with fs.open(s3_path, 'wb') as f:
            if compress == 'bz2':
                import bz2
                with bz2.open(f, 'wb') as bz2f:
                    pickle.dump(self, bz2f)
            else:
                pickle.dump(self, f)
        
        return s3_path
    
    def to_pickle_local(self, local_path, compress=None):
        """
        Write STAREDataFrame to local storage as pickle file.
        
        Parameters
        ----------
        local_path : str
            Local path where the pickle file will be written
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        str
            The local path where data was written
        """
        # Write pickle to local filesystem
        if compress == 'bz2':
            import bz2
            with bz2.open(local_path, 'wb') as f:
                pickle.dump(self, f)
        else:
            with open(local_path, 'wb') as f:
                pickle.dump(self, f)
        
        return local_path
    
    @classmethod
    def from_pickle_s3(cls, s3_path, storage_options=None, compress=None):
        """
        Read STAREDataFrame from S3 pickle file.
        
        Parameters
        ----------
        s3_path : str
            S3 path to the pickle file
        storage_options : dict, optional
            S3 storage options including credentials and region
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame
        """
        import s3fs
        
        # Create S3 filesystem
        fs = s3fs.S3FileSystem(**storage_options or {})
        
        # Read pickle from S3
        with fs.open(s3_path, 'rb') as f:
            if compress == 'bz2':
                import bz2
                with bz2.open(f, 'rb') as bz2f:
                    df = pickle.load(bz2f)
            else:
                df = pickle.load(f)
        
        return df
    
    @classmethod
    def from_pickle_local(cls, local_path, compress=None):
        """
        Read STAREDataFrame from local pickle file.
        
        Parameters
        ----------
        local_path : str
            Local path to the pickle file
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame
        """
        # Read pickle from local filesystem
        if compress == 'bz2':
            import bz2
            with bz2.open(local_path, 'rb') as f:
                df = pickle.load(f)
        else:
            with open(local_path, 'rb') as f:
                df = pickle.load(f)
        
        return df

    def to_sidecar(self, file_name, cover=False, shuffle=True, zlib=True):
        """ Writes STARE Sidecar

        """
        sids = self.to_array(self._sid_column_name)
        # lat = self.to_array(self['lat'])
        # lon = self.to_array(self['lon'])

        i = sids.shape[0]
        j = sids.shape[1]
        with netCDF4.Dataset(file_name, 'w', format="NETCDF4") as root_group:
            root_group.createDimension('i', i)
            root_group.createDimension('j', j)

            sids_netcdf = root_group.createVariable(varname='STARE_index',
                                                    datatype='u8',
                                                    dimensions=('i', 'j'),
                                                    chunksizes=[i, j],
                                                    shuffle=shuffle,
                                                    zlib=zlib)
            sids_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) index'
            sids_netcdf[:, :] = sids
            if cover:
                sids_cover = self.stare_dissolve()
                l: int = sids_cover.size
                root_group.createDimension('l', l)
                cover_netcdf = root_group.createVariable(varname='STARE_cover',
                                                         datatype='u8',
                                                         dimensions='l',
                                                         chunksizes=[l],
                                                         shuffle=shuffle,
                                                         zlib=zlib)
                cover_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) cover'
                cover_netcdf[:] = sids_cover

        def to_postgis(self, name, con, schema=None, if_exists="fail", index=False, index_label=None, chunksize=None, dtype=None):
            """
            This overwrites the geopandas.GeoDataFrame.to_postgis() method.
            Parameters
            ----------
            name
            con
            schema
            if_exists
            index
            index_label
            chunksize
            dtype

            Returns
            -------
            None

            """
            starepandas.io.postgis.write(gdf=self, engine=con, table_name=name)

def _dataframe_set_sids(self, col, inplace=False):
    # We create a function here so that we can take conventional DataFrames and convert them to sdfs
    if inplace:
        raise ValueError("Can't do inplace setting when converting from (Geo)DataFrame to STAREDataFrame")
    sdf = STAREDataFrame(self)
    # this will copy so that BlockManager gets copied
    return sdf.set_sids(col, inplace=False)

geopandas.GeoDataFrame.set_sids = _dataframe_set_sids
pandas.DataFrame.set_sids = _dataframe_set_sids
