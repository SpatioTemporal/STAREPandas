import geopandas
import geopandas.plotting
import pystare
import pandas
import numpy
import starepandas
import starepandas.tools.trixel_conversions

DEFAULT_STARE_COLUMN_NAME = 'stare'
DEFAULT_TRIXEL_COLUMN_NAME = 'trixels'


class STAREDataFrame(geopandas.GeoDataFrame):
    """
    A STAREDataFrame object is a pandas.DataFrame that has a special column
    with STARE indices and optionally a special column holding the trixel representation.
    In addition to the standard DataFrame constructor arguments,
    STARE also accepts the following keyword arguments:

    Parameters
    ----------
    stare : str or array-like
        If str, column to use as stare column. If array, will be set as 'stare' column on STAREDataFrame.
    add_stare : bool
        If true, STARE index values will be generated using a geometry column
    level: bool
        If add_stare is True, then use level as the maximum STARE level. Warning: default is max level (27).
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
    >>> sids = starepandas.stare_from_xy(longitudes, latitudes, level=5)
    >>> sdf = starepandas.STAREDataFrame(data, stare=sids)
    """

    _metadata = ['_stare_column_name', '_trixel_column_name', '_geometry_column_name', '_crs']

    _stare_column_name = DEFAULT_STARE_COLUMN_NAME
    _trixel_column_name = DEFAULT_TRIXEL_COLUMN_NAME

    def __init__(self, *args,
                 stare=None, add_stare=False, level=0,
                 trixels=None, add_trixels=False, n_workers=1,
                 **kwargs):

        super(STAREDataFrame, self).__init__(*args, **kwargs)

        if args and isinstance(args[0], STAREDataFrame):
            self._geometry_column_name = args[0]._geometry_column_name

        if stare is not None:
            self.set_stare(stare, inplace=True)
        elif add_stare:
            stare = self.make_stare(level=level, n_workers=n_workers)
            self.set_stare(stare, inplace=True)

        if trixels is not None:
            self.set_trixels(trixels, inplace=True)
        elif add_trixels:
            trixels = self.make_trixels(n_workers=n_workers)
            self.set_trixels(trixels, inplace=True)

    def __getitem__(self, key):

        result = super(STAREDataFrame, self).__getitem__(key)
        stare_col = self._stare_column_name

        if isinstance(result, (geopandas.GeoDataFrame, pandas.DataFrame, starepandas.STAREDataFrame)):# and stare_col in result:
            result.__class__ = STAREDataFrame
            result._stare_column_name = stare_col
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

    def make_stare(self, level=0, convex=False, force_ccw=True, n_workers=1):
        """
        Generates and returns the STARE representation of each feauture.

        :param level: STARE level to use for the STARE lookup
        :param convex: Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
        :param force_ccw: Toggle if a counter clockwise orientation of the geometries should be enforced
        :param n_workers: Number of workers used to lookup STARE indices in parallel
        :return: list/array of (set of) STARE index values
        :rtype: numpy.ndarray

        Examples
        ----------
        From points

        >>> import starepandas, geopandas
        >>> lats = [-72.609177, -72.648590, -72.591286]
        >>> lons = [-41.255402, -42.054047, -41.625336]
        >>> geoms = geopandas.points_from_xy(lons, lats)
        >>> sdf = starepandas.STAREDataFrame(geometry=geoms)
        >>> sdf.make_stare(level=6, convex=False)
        [2299437706637111654, 2299435211084507366, 2299436587616075270]

        From polygons

        >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        >>> sdf = starepandas.STAREDataFrame(gdf)
        >>> sids = sdf.make_stare(level=5)
        """

        stare = starepandas.stare_from_geoseries(self.geometry,
                                                 level=level, convex=convex, force_ccw=force_ccw, n_workers=n_workers)
        return stare

    def set_stare(self, col=None, inplace=False):
        """ Set the StareDataFrame stare indices using either an existing column or
        the specified input. By default yields a new object.
        The original stare column is replaced with the input.

        :param col:  f stare indices or column name
        :type col: array-like
        :param inplace: Modify the StareDataFrame in place (do not create a new object)
        :type  inplace: Bool
        :return: df
        :rtype: StareDataFrame

        Examples
        --------
        >>> import starepandas
        >>> sdf = starepandas.STAREDataFrame()
        >>> sids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        >>> sdf.set_stare(sids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.copy()

        if col is None:
            col = self.make_stare()

        if isinstance(col, (list, numpy.ndarray, pandas.Series)):
            frame[frame._stare_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._stare_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def make_trixels(self, stare_column=None, n_workers=1):
        """
        Returns a Polygon or Multipolygon GeoSeries
        containing the trixels referred by the STARE indices

        :param stare_column: Column to use as STARE column. Default: 'stare'
        :type stare_column: string
        :param n_workers: number of (dask) workers to use to generate trixels
        :type n_workers: integer
        :return: returns array of polygons or multipolygons representing the trixels
        :rtype:

        Examples
        --------
        >>> import starepandas
        >>> sids = [648518346341351428, 900719925474099204, 1170935903116328964]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> trixels = sdf.make_trixels()
        """

        if stare_column is None:
            stare_column = self._stare_column_name
        trixels_series = starepandas.tools.trixel_conversions.trixels_from_stareseries(self[stare_column], n_workers=n_workers)
        return trixels_series

    def set_trixels(self, col=None, inplace=False):
        """
        Set the trixel column

        :param col: If array like, will add the array as a new trixel column. If string, will set the df['col'] as the trixel column. If None, will generate trixels from the STARE column.
        :type col: Array-like, string, or None
        :param inplace: Modify the StareDataFrame in place (do not create a new object)
        :type inplace: bool
        :return: DataFrame or None
        :rtype: DataFrame or None

        Examples
        ---------
        >>> import starepandas
        >>> sids = [4611686018427387903, 4611686018427387903, 4611686018427387903]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> trixels = sdf.make_trixels()
        >>> sdf.set_trixels(trixels, inplace=True)
        """

        if inplace:
            frame = self
        else:
            frame = self.copy()

        if col is None:
            col = self.make_trixels()

        if isinstance(col, (pandas.Series, list, numpy.ndarray)):
            frame[frame._trixel_column_name] = col
        elif isinstance(col, str) and col in self.columns:
            frame._trixel_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

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
        vertices : tuple
            A vertices data structure

        Examples
        ---------
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> df.trixel_vertices()
        (array([29.9999996 , 45.00000069, 29.9999996 ]),
        array([189.73560999, 315.        ,  80.26439001]),
        array([80.264389]),
        array([135.]))
        """
        return starepandas.tools.trixel_conversions.to_vertices(self[self._stare_column_name])

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
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> df.trixel_centers()
        array([[135.      ,  80.264389]])
        """

        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers(self[self._stare_column_name])

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
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> df.trixel_centers_ecef()
        array([[-0.11957316,  0.11957316,  0.98559856]])
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers_ecef(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers_ecef(self[self._stare_column_name])

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
        >>> sids = numpy.array([3458764513820540928])
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> centers = df.trixel_centerpoints()
        >>> print(centers[0])
        POINT (135 80.26438899520529)
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centerpoints(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centerpoints(self[self._stare_column_name])

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
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> df.trixel_corners()
        array([[[189.73560999,  29.9999996 ],
                [315.        ,  45.00000069],
                [ 80.26439001,  29.9999996 ]]])
        """

        if vertices:
            corners = starepandas.tools.trixel_conversions.vertices2corners(vertices)
        elif from_trixels and self._trixel_column_name in self.columns:
            corners = []
            for trixel in self[self._trixel_column_name]:
                # Trixel is a polygon. Its first element is the outer ring.
                corners.append(tuple(trixel[0].boundary.coords)[0:3])
        else:
            corners = starepandas.tools.trixel_conversions.to_corners(self[self._stare_column_name])

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
        >>> df = starepandas.STAREDataFrame(stare=sids)
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
        >>> df = starepandas.STAREDataFrame(stare=sids)
        >>> df.trixel_grings()
        array([[[ 0.14644661,  0.85355339,  0.49999999],
                [-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679]]])
        """

        corners = self.trixel_corners_ecef(vertices)
        gring = starepandas.tools.trixel_conversions.corners2gring(corners)
        return gring

    def plot(self, *args, trixels=True, boundary=False, **kwargs):
        """ Generate a plot with matplotlib.
        Seminal method to
        `GeoDataFrame.plot() <https://geopandas.org/docs/reference/api/geopandas.GeoDataFrame.plot.html>`_
        All GeoDataFrame.plot() kwargs are available.

        :param trixels: Toggle if trixels (rather than the SF geometry) is to be plotted
        :type trixels: bool
        :param boundary: Toggle if the ring is to be plotted as a linestring rather than the polygon
        :type boundary: bool
        :return: ax

        Examples
        --------
        >>> import starepandas
        >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        >>> germany = world[world.name=='Germany']
        >>> germany = starepandas.STAREDataFrame(germany, add_stare=True, level=8, add_trixels=True, n_workers=1)
        >>> ax = germany.plot(trixels=True, boundary=True, color='y', zorder=0)
        """
        if trixels:
            boundary = True
            df = self.set_geometry(self._trixel_column_name, inplace=False)
        else:
            df = self.copy()
        if boundary:
            df = df[df.geometry.is_empty==False]
            df = df.set_geometry(df.geometry.boundary)
        return geopandas.plotting.plot_dataframe(df, *args, **kwargs)

    def to_scidb(self, connection):
        pass

    def stare_intersects(self, other, method='binsearch', n_workers=1):
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
        n_workers: int
            number of workers to be used for intersects tests

        Examples
        --------
        >>> germany = [4251398048237748227, 4269412446747230211, 4278419646001971203, 4539628424389459971, 4548635623644200963, 4566650022153682947]
        >>> cities = {'name': ['berlin', 'madrid'], 'sid': [4258121269174388239, 4288120002905386575]}
        >>> cities = starepandas.STAREDataFrame(cities, stare='sid')
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
                                                   series=self[self._stare_column_name],
                                                   method=method,
                                                   n_workers=n_workers)
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
        >>> sids1 = starepandas.stare_from_polygon(polygon1, level=5, force_ccw=True)
        >>> sids2 = starepandas.stare_from_polygon(polygon2, level=5, force_ccw=True)

        >>> df = starepandas.STAREDataFrame(stare=[sids1])
        >>> df.stare_intersection(sids2).iloc[0]
        array([694117292568477701, 701435641962954757, 701998591916376069])
        """
        data = []
        for srange in self[self._stare_column_name]:
            data.append(pystare.intersection(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, by=None, dissolve_sids=True, n_workers=1,
                       n_chunks=1, geom=False, aggfunc="first", **kwargs):
        """
        Dissolves a dataframe subject to a field. I.e. grouping by a field/column.
        Seminal method to GeoDataFrame.dissolve()

        Parameters
        -------------
        by: str
            column to use the dissolve on. If None, dissolve all rows.
        dissolve_sids: bool
            Toggle if STARE index values get dissolved. If not, sids will be appended.
            If not dissolved, there may be repetitive sids and sids that could get merged into the parent sid.
        n_workers: int
            workers to use for the dissolve
        n_chunks: int
            Performance optimization; number of chunks to use for the stare dissolve.
        geom: bool
            Toggle if the geometry column is to be dissolved.
        aggfunc: str
            aggregation function. E.g. 'first', 'sum', 'mean'.

        Examples
        --------
        >>> import geopandas
        >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        >>> west = world[world['continent'].isin(['Europe', 'North America'])]
        >>> west = starepandas.STAREDataFrame(west, add_stare=True, level=4, add_trixels=False)
        >>> west.stare_dissolve(by='continent', aggfunc='sum') # doctest: +SKIP
                                                                   stare  ...  gdp_md_est
        continent                                                         ...
        Europe         [648518346341351428, 900719925474099204, 10448...  ...  25284877.0
        North America  [1170935903116328964, 1173187702930014212, 117...  ...  23505137.0
        """
        if by is None:
            sids = starepandas.merge_stare(self[self._stare_column_name], dissolve_sids, n_workers, n_chunks)
            return sids
        else:
            data = self.drop(columns=['stare', 'trixels'], errors='ignore')
            if geom:
                aggregated_data = data.dissolve_stare(by=by, aggfunc=aggfunc, **kwargs)
            else:
                aggregated_data = data.groupby(by=by, **kwargs).agg(
                    aggfunc)

        sids = self.groupby(group_keys=True, by=by)[self._stare_column_name].agg(starepandas.merge_stare,
                                                                                 dissolve_sids,
                                                                                 n_workers, n_chunks)
        sdf = starepandas.STAREDataFrame(sids, stare=self._stare_column_name)

        aggregated = sdf.join(aggregated_data)
        return aggregated

    def to_stare_resolution(self, resolution, inplace=False, clear_to_resolution=False):
        """
        Changes resolution of STARE index values to resolution; optionally clears location bits up to resolution.
        Caution: This methods is not intended for use on feautures represented by sets of sids.

        Parameters
        ------------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).
        resolution: int
            STARE level/resolution to change to.
        clear_to_resolution: bool
            Toggle if the location bits below resolutions should be cleared

        Returns
        -------------
        if not inplace, returns stare index values, otherwise None

        Examples
        --------
        >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> sdf.to_stare_resolution(resolution=6, clear_to_resolution=False)
        0    2299437706637111718
        1    2299435211084507590
        2    2299566194809236966
        Name: stare, dtype: int64

        """

        if inplace:
            sids = self[self._stare_column_name]
        else:
            sids = self[self._stare_column_name].copy()

        sids = pystare.spatial_coerce_resolution(sids, resolution)
        if clear_to_resolution:
            # pystare_terminator_mask uses << operator, which requires us to cast to numpy array first
            sids = pystare.spatial_clear_to_resolution(numpy.array(sids))

        if inplace:
            self[self._stare_column_name] = sids
        else:
            return sids

    def clear_to_resolution(self, inplace=False):
        """
        Clears location bits to resolution

        Parameters
        -----------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Examples
        ----------
        >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> sdf.clear_to_resolution(inplace=False)
        array([2299437254470270985, 2299435055447015433, 2299564797819093001])
        """
        if inplace:
            sids = self[self._stare_column_name]
        else:
            sids = self[self._stare_column_name].copy()
        sids = pystare.spatial_clear_to_resolution(numpy.array(sids))

        if inplace:
            self[self._stare_column_name] = sids
        else:
            return sids

    def to_stare_singleres(self, resolution=None, inplace=False):
        """
        Changes the STARE index values to single resolution representation (in contrary to multiresolution).

        Parameters
        -----------
        resolution: int
            resolution to change thre
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
        >>> germany = starepandas.STAREDataFrame(germany, add_stare=True, level=6, add_trixels=False)
        >>> len(germany.stare.iloc[0])
        43
        >>> sids_singleres = germany.to_stare_singleres()
        >>> len(sids_singleres[0])
        46
        """

        if inplace:
            sids_col = self[self._stare_column_name]
        else:
            sids_col = self[self._stare_column_name].copy()

        new_sids_col = []
        for sids in sids_col:
            if resolution:
                r = resolution
            else:
                r = int(pystare.spatial_resolution(sids).max())
            sids = pystare.expand_intervals(sids, level=r, multi_resolution=False)
            new_sids_col.append(sids)

        if inplace:
            self[self._stare_column_name] = new_sids_col
        else:
            return new_sids_col

    def hex(self):
        """
        Returns the hex16 representation of the stare column

        Examples
        ---------
        >>> sdf = starepandas.STAREDataFrame(stare=[2251799813685252, 4503599627370500])
        >>> sdf.hex()
        ['0x0008000000000004', '0x0010000000000004']

        >>> sdf = starepandas.STAREDataFrame(stare=[[2251799813685252, 4503599627370500], [4604930618986332164, 4607182418800017412]])
        >>> sdf.hex()
        [['0x0008000000000004', '0x0010000000000004'], ['0x3fe8000000000004', '0x3ff0000000000004']]
        """

        sids = []
        for row in self[self._stare_column_name]:
            try:
                # Ducktyping collection of sids
                sids.append(list(map(pystare.int2hex, row)))
            except TypeError:
                sids.append(pystare.int2hex(row))
        return sids

    def write_pods(self, pod_root, level, chunk_name, hex=True):
        """ Writes dataframe into a starepods hierarchy

        Parameters
        --------------
        pod_root: str
            Root directory of starepods
        level: str
            level of starepods
        chunk_name: str
            name of the pod
        hex: bool
            toggle pods being hex vs int
        """
        grouped = self.groupby(self.to_stare_resolution(resolution=level, clear_to_resolution=True))
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


def _dataframe_set_stare(self, col, inplace=False):
    if inplace:
        raise ValueError(
            "Can't do inplace setting when converting from (Geo)DataFrame to STAREDataFrame"
        )
    sdf = STAREDataFrame(self)
    # this will copy so that BlockManager gets copied    
    return sdf.set_stare(col, inplace=False)


geopandas.GeoDataFrame.set_stare = _dataframe_set_stare
pandas.DataFrame.set_stare = _dataframe_set_stare
