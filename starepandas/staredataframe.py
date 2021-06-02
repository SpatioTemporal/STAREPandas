import geopandas
import geopandas.plotting
import pystare
import pandas
import numpy
import starepandas

DEFAULT_STARE_COLUMN_NAME = 'stare'
DEFAULT_TRIXEL_COLUMN_NAME = 'trixels'


class STAREDataFrame(geopandas.GeoDataFrame):
    """
    A STAREDataFrame object is a pandas.DataFrame that has a column
    with STARE indices. In addition to the standard DataFrame constructor arguments,
    STARE also accepts the following keyword arguments:

    Parameters
    ----------
    stare : value (optional)
        a
    add_stare :
        a
    trixels :
        a
    add_trixels :
        a

    Examples
    --------
    >>>
    """

    _metadata = ['_stare_column_name', '_trixel_column_name', '_geometry_column_name', '_crs']

    _stare_column_name = DEFAULT_STARE_COLUMN_NAME
    _trixel_column_name = DEFAULT_TRIXEL_COLUMN_NAME

    def __init__(self, *args,
                 stare=None, add_stare=False, level=-1,
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
        if isinstance(result, (geopandas.GeoDataFrame, pandas.DataFrame)) and stare_col in result:
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

    def make_stare(self, level=-1, convex=False, force_ccw=True, n_workers=1):
        """Generates and returns the STARE representation of each feauture.

        :param level: STARE level to use for the STARE lookup
        :param convex: Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
        :param force_ccw: Toggle if a counter clockwise orientation of the geometries should be enforced
        :param n_workers: Number of workers used to lookup STARE indices in parallel
        :return: list/array of (set of) STARE index values
        :rtype: numpy.ndarray

        :Example:
        From points
        >>> import starepandas, geopandas
        >>> lats = [-72.609177, -72.648590, -72.591286]
        >>> lons = [-41.255402, -42.054047, -41.625336]
        >>> geoms = geopandas.points_from_xy(lons, lats)
        >>> sdf = starepandas.STAREDataFrame(geometry=geoms)
        >>> sdf.make_stare(level=6, convex=False)
        From polygons
        >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        >>> sdf = starepandas.STAREDataFrame(gdf)
        >>> sdf.make_stare(level=5), inplace=True)
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

        :Example:
        >>> import starepandas
        >>> sdf = starepandas.STAREDataFrame()
        >>> sdf.set_stare([[4611686018427387903, 4611686018427387903, 4611686018427387903])
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
        """Returns a Polygon or Multipolygon GeoSeries 
        containing the trixels referred by the STARE indices

        :param stare_column: Column to use as STARE column. Default: 'stare'
        :type stare_column: string
        :param n_workers: number of (dask) workers to use to generate trixels
        :type n_workers: integer
        :return: returns array of polygons or multipolygons representing the trixels
        :rtype:

        >>> import starepandas
        >>> sids = [4611686018427387903, 4611686018427387903, 4611686018427387903]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> sdf.make_trixels()

        """
        if stare_column is None:
            stare_column = self._stare_column_name
        trixels_series = starepandas.trixels_from_stareseries(self[stare_column], n_workers=n_workers)
        return trixels_series

    def set_trixels(self, col=None, inplace=False):
        """

        :param col:
        :type col:
        :param inplace:
        :type inplace:
        :return:
        :rtype:
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

    def plot(self, *args, trixels=False, boundary=False, **kwargs):
        if trixels:
            boundary = True
            df = self.set_geometry(self._trixel_column_name, inplace=False)
        else:
            df = self.copy()
        if boundary:
            df = df[df.geometry.is_empty == False]
            df = df.set_geometry(df.geometry.boundary)
        return geopandas.plotting.plot_dataframe(df, *args, **kwargs)

    def to_scidb(self, connection):
        pass

    def stare_intersects(self, other, method=1, n_workers=1):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that intersects `other`.
        An object is said to intersect `other` if its `boundary` and `interior`
        intersects in any way with those of the other.
        Parameters
        ----------
        other: Collection of STARE indices 
            The STARE index collection representing the spatial object to test if is
            intersected.
        method: Method for STARE intersects test {skiplist': 0, 'binsearch': 1, 'nn': 2}. Default: 1
        n_workers: number of workers to be used for intersects tests
        """

        if isinstance(other, (int, numpy.int64)):
            # Other is a single STARE index value
            other = [other]
        elif isinstance(other, (numpy.ndarray, list)):
            # Other is a collection/set of STARE index values
            pass
        else:
            raise ValueError("Other must be array-like object or int64")

        intersects = starepandas.series_intersects(other=other, series=self[self._stare_column_name], method=method,
                                                   n_workers=n_workers)
        return pandas.Series(intersects)

    def stare_disjoint(self, other, method=1):
        return self.stare_intersects(other, method) is False

    def stare_intersection(self, other):
        """Returns a ``STARESeries`` of the spatial intersection of self with `other`.

        :param other: The STARE index value collection representing the object to find the intersection with.
        :type other: Array like or int
        :return:
        :rtype:
        """
        data = []
        for srange in self[self._stare_column_name]:
            data.append(pystare.intersect(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, dissolve_sids=True, n_workers=1, n_chunks=1, geom=False, by=None, aggfunc="first",
                       as_index=True, level=None, sort=True, observed=False, dropna=True):
        """ Dissolves a dataframe subject to a parameter

        :param dissolve_sids: Toggle if STARE index values get dissolved. If not, sids will be appended
        :type dissolve_sids: bool
        :param n_workers: workers to use for the dissolve
        :type n_workers: int
        :param n_chunks: Performance optimization; number of chunks to use for the stare dissolve.
        :type n_chunks: int
        :param geom: Toggle if the geometry column is to be dissolved
        :type geom: bool
        :param by: column to use the dissolve on
        :type by: str
        :param aggfunc: aggregation function
        :type aggfunc: str
        :param as_index: Toggle if 'by' column is to be the new index
        :type as_index: bool
        :param level:
        :type level:
        :param sort:
        :type sort:
        :param observed:
        :type observed:
        :param dropna:
        :type dropna:
        :return:
        :rtype:
        """
        if by is None:
            sids = starepandas.merge_stare(self[self._stare_column_name], dissolve_sids, n_workers, n_chunks)
            return sids
        else:
            groupby_kwargs = dict(by=by, sort=sort, observed=observed, dropna=dropna)
            dissolve_kwargs = groupby_kwargs
            dissolve_kwargs['aggfunc'] = aggfunc
            dissolve_kwargs['level'] = level
            data = self.drop(columns=['stare', 'trixels'], errors='ignore')
            if geom:
                aggregated_data = data.dissolve(by=by, aggfunc=aggfunc, as_index=as_index)
            else:
                aggregated_data = data.groupby(by=by, level=level, sort=sort, observed=observed, dropna=dropna).agg(
                    aggfunc)

        sids = self.groupby(group_keys=True, by=by)[self._stare_column_name].agg(starepandas.merge_stare, dissolve_sids,
                                                                                 n_workers, n_chunks)
        sdf = starepandas.STAREDataFrame(sids, stare=self._stare_column_name)

        aggregated = sdf.join(aggregated_data)
        return aggregated

    def to_stare_resolution(self, resolution, inplace=False, clear_to_resolution=False):
        """ Forces resolution of STARE index values to resolution; optionally clears location bits up to resolution
        :param inplace: If True, modifies the DataFrame in place (do not create a new object).
        :type inplace: bool
        :param resolution
        :type int
        :param clear_to_resolution: Toggle if the location bits below resolutions should be cleared
        :type clear_to_resolution: bool
        :return: if not inplace, returns stare index values, otherwise None
        :rtype None or ndarray

        :Example:
        >>> import starepandas
        >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        >>> sdf = starepandas.STAREDataFrame(stare=sids)
        >>> sdf.to_stare_resolution(resolution=6, clear_to_resolution=False)
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

    def to_stare_singleres(self, inplace=False):
        """ Changes the STARE index values to single resolution representation.
        :param inplace: If True, modifies the DataFrame in place (do not create a new object).
        :type bool:
        :return: if not inplace, returns stare index values, otherwise None
        :rtype None or ndarray
        """
        if inplace:
            sids = self[self._stare_column_name]
        else:
            sids = self[self._stare_column_name].copy()

        sids = sids.apply(lambda row: pystare.intersect(row, row, False))

        if inplace:
            self[self._stare_column_name] = sids
        else:
            return sids

    def write_pods(self, pod_root, level, pod_name):
        """ Writes dataframe into a starepods hierarchy

        :param pod_root: Root directory of starepods
        :type pod_root: str
        :param level: level of starepodspod
        :type level: int
        :param pod_name: name of the pod
        :type pod_name: str
        """
        grouped = self.groupby(self.to_stare_resolution(resolution=level, clear_to_resolution=True))
        for group in grouped.groups:
            g = grouped.get_group(group)
            g.to_pickle('{pod_root}/{pod}/{pod_name}'.format(pod_root=pod_root, pod=group, pod_name=pod_name))

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
