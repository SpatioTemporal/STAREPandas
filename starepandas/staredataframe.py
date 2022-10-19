import geopandas.plotting
import pystare
import pandas
import numpy
import starepandas
import netCDF4
import starepandas.tools.trixel_conversions
import multiprocessing

DEFAULT_SID_COLUMN_NAME = 'sids'
DEFAULT_TID_COLUMN_NAME = 'tids'
DEFAULT_TRIXEL_COLUMN_NAME = 'trixels'


def compress_sids_group(group):
    sids = group[1].to_numpy()  # zero element is group label, 1 element is the df
    if sids.dtype == numpy.dtype('O'):
        # If we receive a series of SID collections we merge all sids into a single 1D array
        # to_numpy() would have produced an array of lists in this case
        sids = numpy.concatenate(sids)
    sids = starepandas.compress_sids(sids)
    return tuple([group[0], sids])


class STAREDataFrame(geopandas.GeoDataFrame):
    _metadata = ['_sid_column_name', '_trixel_column_name', '_geometry_column_name', '_crs']

    _sid_column_name = DEFAULT_SID_COLUMN_NAME
    _trixel_column_name = DEFAULT_TRIXEL_COLUMN_NAME
    _tid_column_name = DEFAULT_TID_COLUMN_NAME

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
        >>> cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
        >>> latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
        >>> longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
        >>> data =  {'City': cities, 'Latitude': latitudes, 'Longitude': longitudes}
        >>> sids = starepandas.sids_from_xy(longitudes, latitudes, level=5)
        >>> sdf = starepandas.STAREDataFrame(data, sids=sids)
        """

        super(STAREDataFrame, self).__init__(*args, **kwargs)

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

    def __getitem__(self, key):

        result = super(STAREDataFrame, self).__getitem__(key)
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
            super(STAREDataFrame, self).__setattr__(attr, val)

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
            Toggle if a counterclockwise orientation of the geometries should be enforced
        n_partitions: int
            Number of workers used to lookup STARE indices in parallel

        Returns
        ---------
        sids: numpy.ndarray
            array of (set of) STARE index values

        Examples
        ----------
        From points

        >>> import starepandas, geopandas
        >>> lats = [-72.609177, -72.648590, -72.591286]
        >>> lons = [-41.255402, -42.054047, -41.625336]
        >>> geoms = geopandas.points_from_xy(lons, lats)
        >>> sdf = starepandas.STAREDataFrame(geometry=geoms)
        >>> sdf.make_sids(level=6, convex=False)
        0    2299437706637111654
        1    2299435211084507366
        2    2299436587616075270
        Name: sids, dtype: int64

        From polygons

        >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        >>> sdf = starepandas.STAREDataFrame(gdf)
        >>> sids = sdf.make_sids(level=5)
        """

        sids = starepandas.sids_from_geoseries(self.geometry, level=level, convex=convex,
                                               force_ccw=force_ccw, n_partitions=n_partitions)
        return sids

    def set_sids(self, col, inplace=False):
        """ Set the StareDataFrame stare indices using either an existing column or
        the specified input. By default, yields a new object.
        The original stare column is replaced with the input.

        Parameters
        -------------
        col: array-like
            f stare indices or column name
        inplace: boolean
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        ---------
        df: STAREDataFrame
            the df with sids

        Examples
        --------
        >>> import starepandas
        >>> sdf = starepandas.STAREDataFrame()
        >>> sids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        >>> sdf.set_sids(sids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.copy()

        if isinstance(col, (list, numpy.ndarray, pandas.Series)):
            frame[frame._sid_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._sid_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def drop_na_sids(self, inplace=False):
        """Drop all rows that have NA values for the SIDs and cast the column to numpy.int64 """
        if inplace:
            self.dropna(subset=[self._sid_column_name], inplace=inplace)
            self[self._sid_column_name] = self[self._sid_column_name].astype(numpy.dtype('int64'))
        else:
            frame = self.copy()
            frame = frame.dropna(subset=[frame._sid_column_name], inplace=inplace)
            frame[frame._sid_column_name] = frame[frame._sid_column_name].astype(numpy.dtype('int64'))
            return frame

    def make_tids(self):
        pass

    def set_tids(self):
        pass

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
        >>> import starepandas
        >>> sids = [648518346341351428, 900719925474099204, 1170935903116328964]
        >>> sdf = starepandas.STAREDataFrame(sids=sids)
        >>> trixels = sdf.make_trixels()
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
        >>> import starepandas
        >>> sids = [4611686018427387903, 4611686018427387903, 4611686018427387903]
        >>> sdf = starepandas.STAREDataFrame(sids=sids)
        >>> trixels = sdf.make_trixels()
        >>> sdf.set_trixels(trixels, inplace=True)
        """

        if inplace:
            frame = self
        else:
            frame = self.copy()

        if isinstance(col, (pandas.Series, geopandas.GeoSeries, list, numpy.ndarray)):
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_vertices()
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_centers()
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_centers_ecef()
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
        >>> sids = numpy.array([4458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> centers = df.trixel_centerpoints()
        >>> print(centers[0])
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_corners()
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_corners_ecef()
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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(sids=sids)
        >>> df.trixel_grings()
        array([[[ 0.14644661,  0.85355339,  0.49999999],
                [-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679]]])
        """

        corners = self.trixel_corners_ecef(vertices)
        gring = starepandas.tools.trixel_conversions.corners2gring(corners)
        return gring

    def split_antimeridian(self, inplace=False, n_workers=1, drop=False):
        """Splits trixels at the antimeridian

        This works on trixels that cross the meridian and whose longitudes have *not* been wrapped around the
        antimeridian. I.e. when creating the trixels use sdf.make_trixels(wrap_lon=False)


        Examples
        ----------
        >>> cities = {'name': ['midway', 'Fiji', 'Baker', 'honolulu'],
        ...           'lat': [28.2, -17.8,  0.2, 21.3282956],
        ...           'lon': [-177.35, 178.1, -176.7, -157.9]}
        >>> sdf = starepandas.STAREDataFrame(cities)
        >>> sids = starepandas.sids_from_xy(sdf.lon, sdf.lat, level=1)
        >>> sdf.set_sids(sids, inplace=True)
        >>> trixels = sdf.make_trixels(wrap_lon=False)
        >>> sdf.set_trixels(trixels, inplace=True)
        >>> cites_split = sdf.split_antimeridian(inplace=False)
        >>> max(max(cites_split.trixels[1].geoms[0].exterior.xy))
        180.0

        """
        if inplace:
            df = self
        else:
            df = self.copy()

        trixels = geopandas.GeoSeries(df[df._trixel_column_name])

        split = []
        for row in trixels:
            if row.geom_type == 'Polygon':
                # We need to catch single Polygons
                row = [row]
            row = starepandas.tools.trixel_conversions.split_antimeridian(row, drop=drop)
            split.append(row)
        split = geopandas.GeoSeries(split, index=df.index)

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
        >>> import starepandas
        >>> import geopandas
        >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        >>> germany = world[world.name=='Germany']
        >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=8, add_trixels=True, n_partitions=1)
        >>> ax = germany.plot(trixels=True, boundary=True, color='y', zorder=0)
        """
        df = self.copy()

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
        >>> germany = [4251398048237748227, 4269412446747230211, 4278419646001971203,
        ...            4539628424389459971, 4548635623644200963, 4566650022153682947]
        >>> cities = {'name': ['berlin', 'madrid'], 'sid': [4258121269174388239, 4288120002905386575]}
        >>> cities = starepandas.STAREDataFrame(cities, sids='sid')
        >>> cities.stare_intersects(germany)
        0     True
        1    False
        dtype: bool
        """

        if isinstance(other, (int, numpy.int64)):
            # Other is a single STARE index value
            other = [other]
        elif isinstance(other, (numpy.ndarray, list)):
            # Other is a collection/set of STARE index values
            pass
        else:
            raise ValueError("Other must be array-like object or int64")

        intersects = starepandas.series_intersects(other=other,
                                                   series=self[self._sid_column_name],
                                                   method=method,
                                                   n_partitions=n_partitions,
                                                   num_workers=num_workers)
        return pandas.Series(intersects)

    def stare_disjoint(self, other, method='binsearch', n_workers=1):
        """  Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that is disjoint from `other`.
        This is the inverse operation of STAREDataFrame.stare_intersects()

        Parameters
        ------------
        other: array-like
            The STARE index collection representing the spatial object to test if is intersected.
        method: str
            Method for STARE intersects test 'skiplist', 'binsearch' or 'nn'. Default: 'binsearch'.
        n_workers: int
            number of workers to be used for intersects tests

        See also
        --------
        STAREDataFrame.stare_intersects : intersects test

        """
        return ~self.stare_intersects(other, method, n_workers)

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
        >>> import shapely
        >>> nodes1 = [[102, 33], [101, 35], [105, 34], [104, 33], [102, 33]]
        >>> nodes2 = [[102, 34], [106, 35], [106, 33], [102, 33.5], [102, 34]]
        >>> polygon1 = shapely.geometry.Polygon(nodes1)
        >>> polygon2 = shapely.geometry.Polygon(nodes2)
        >>> sids1 = starepandas.sids_from_polygon(polygon1, level=5, force_ccw=True)
        >>> sids2 = starepandas.sids_from_polygon(polygon2, level=5, force_ccw=True)

        >>> df = starepandas.STAREDataFrame(sids=[sids1])
        >>> df.stare_intersection(sids2).iloc[0]
        array([694117292568477701, 701435641962954757, 701998591916376069])
        """
        data = []
        for srange in self[self._sid_column_name]:
            data.append(pystare.intersection(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, by=None, compress_sids=True, num_workers=1,
                       geom=False, aggfunc="first", **kwargs):
        """
        Dissolves a dataframe subject to a field. I.e. grouping by a field/column.
        Seminal method to GeoDataFrame.dissolve()

        Parameters
        -------------
        by: str
            column to use the dissolve on. If None, dissolve all rows.
        compress_sids: bool
            Toggle if STARE index values get dissolved. If not, sids will be appended.
            If not dissolved, there may be repetitive sids and sids that could get merged into the parent sid.
        num_workers: int
            workers to use for the dissolve
        geom: bool
            Toggle if the geometry column is to be dissolved. Geom column Will be dropped if set to False.
        aggfunc: str
            aggregation function. E.g. 'first', 'sum', 'mean'.

        Examples
        --------
        >>> import geopandas
        >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        >>> west = world[world['continent'].isin(['Europe', 'North America'])]
        >>> west = starepandas.STAREDataFrame(west, add_sids=True, level=4, add_trixels=False)
        >>> west.stare_dissolve(by='continent', aggfunc='sum') # doctest: +SKIP
                                                                   stare  ...  gdp_md_est
        continent                                                         ...
        Europe         [648518346341351428, 900719925474099204, 10448...  ...  25284877.0
        North America  [1170935903116328964, 1173187702930014212, 117...  ...  23505137.0
        """
        if by is None:
            sids = self[self._sid_column_name].to_numpy()
            if sids.dtype == numpy.dtype('O'):
                # If we receive a series of SID collections we merge all sids into a single 1D array
                # to_numpy() would have produced an array of lists in this case
                sids = numpy.concatenate(sids)
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

    def to_stare_level(self, level, inplace=False, clear_to_level=False):
        """
        Changes level of STARE index values to level; optionally clears location bits up to level.
        Caution: This method is not intended for use on feautures represented by sets of sids.

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
        >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        >>> sdf = starepandas.STAREDataFrame(sids=sids)
        >>> sdf.to_stare_level(level=6, clear_to_level=False)
                          sids
        0  2299437706637111718
        1  2299435211084507590
        2  2299566194809236966
        """

        if inplace:
            df = self
        else:
            df = self.copy()

        sids = df[df._sid_column_name].astype(numpy.dtype('int64'))
        sids = pystare.spatial_coerce_resolution(sids, level)
        if clear_to_level:
            # pystare_terminator_mask uses << operator, which requires us to cast to numpy array first
            sids = pystare.spatial_clear_to_resolution(numpy.array(sids))

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
        >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        >>> sdf = starepandas.STAREDataFrame(sids=sids)
        >>> sdf.clear_to_level(inplace=False)
                          sids
        0  2299437254470270985
        1  2299435055447015433
        2  2299564797819093001

        """
        if inplace:
            df = self
        else:
            df = self.copy()
        sids = df[df._sid_column_name]
        sids = pystare.spatial_clear_to_resolution(numpy.array(sids))

        df[df._sid_column_name] = sids
        if not inplace:
            return df

    def to_stare_singllevel(self, level=None, inplace=False):
        """
        Changes the STARE index values to single level representation (in contrary to multiresolution).

        Parameters
        -----------
        level: int
            level to change thre
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Returns
        ------------
        if not inplace, returns stare index values, otherwise None

        Examples
        ---------
        >>> import geopandas
        >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        >>> germany  = world[world.name=='Germany']
        >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=6, add_trixels=False)
        >>> len(germany.sids.iloc[0])
        43
        >>> germany_singleres = germany.to_stare_singllevel()
        >>> len(germany_singleres.sids.iloc[0])
        46
        """

        if inplace:
            df = self
        else:
            df = self.copy()

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
        >>> sdf = starepandas.STAREDataFrame(sids=[2251799813685252, 4503599627370500])
        >>> sdf.hex()
        ['0x0008000000000004', '0x0010000000000004']

        >>> sdf = starepandas.STAREDataFrame(sids=[[2251799813685252, 4503599627370500],
        ...                                        [4604930618986332164, 4607182418800017412]])
        >>> sdf.hex()
        [['0x0008000000000004', '0x0010000000000004'], ['0x3fe8000000000004', '0x3ff0000000000004']]
        """

        sids = []
        for row in self[self._sid_column_name]:
            try:
                # Ducktyping collection of sids
                sids.append(list(map(pystare.int2hex, row)))
            except TypeError:
                sids.append(pystare.int2hex(row))
        return sids

    def write_pods(self, pod_root, level, chunk_name, hex=True):
        """ Writes dataframe into a STAREPods hierarchy

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
        """
        grouped = self.groupby(self.to_stare_level(level=level, clear_to_level=True)[self._sid_column_name])
        for group in grouped.groups:
            g = grouped.get_group(group)
            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group
            g.to_pickle('{pod_root}/{pod}/{chunk_name}'.format(pod_root=pod_root, pod=pod, chunk_name=chunk_name))

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
        >>> df = starepandas.STAREDataFrame({'x': [0, 0, 1, 1],
        ...                                  'y': [1, 0, 0, 1],
        ...                                  'a': [1, 2, 3, 4]})
        >>> df.to_array('a', pivot=False)
        array([[1, 2],
               [3, 4]])

        >>> df.to_array('a', pivot=True)
        array([[2, 1],
               [3, 4]])

        See also
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


def _dataframe_set_sids(self, col, inplace=False):
    # We create a function here so that we can take conventional DataFrames and convert them to sdfs
    if inplace:
        raise ValueError(
            "Can't do inplace setting when converting from (Geo)DataFrame to STAREDataFrame"
        )
    sdf = STAREDataFrame(self)
    # this will copy so that BlockManager gets copied
    return sdf.set_sids(col, inplace=False)


geopandas.GeoDataFrame.set_sids = _dataframe_set_sids
pandas.DataFrame.set_sids = _dataframe_set_sids
