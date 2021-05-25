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
    Examples
    --------
    Constructing GeoDataFrame from a dictionary."""

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
            stare = self.stare(level=level, n_workers=n_workers)
            self.set_stare(stare, inplace=True)

        if trixels is not None:
            self.set_trixels(trixels, inplace=True)
        elif add_trixels:
            trixels = self.trixels(n_workers=n_workers)
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

    def stare(self, level=-1, nonconvex=True, force_ccw=True, n_workers=1):
        stare = starepandas.stare_from_geoseries(self.geometry, level, nonconvex, force_ccw, n_workers)
        return stare

    def set_stare(self, col=None, inplace=False):
        """
        Set the StareDataFrame stare indices using either an existing column or
        the specified input. By default yields a new object.
        The original stare column is replaced with the input.
        Parameters
        ----------
        col : array-like of stare indices or column name
        inplace : boolean, default False
            Modify the StareDataFrame  in place (do not create a new object)
        Examples
        --------
        >> df = df.set_stare([[4611686018427387903, 4611686018427387903, 4611686018427387903])
        Returns
        -------
        StareDataFrame
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.copy()

        if col is None:
            col = self.stare()

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

    def trixels(self, stare_column=None, n_workers=1):
        """Returns a Polygon or Multipolygon GeoSeries 
        containing the trixels referred by the STARE indices"""
        if stare_column is None:
            stare_column = self._stare_column_name
        trixels_series = starepandas.trixels_from_stareseries(self[stare_column], n_workers=n_workers)
        return trixels_series

    def set_trixels(self, col=None, inplace=False):
        if inplace:
            frame = self
        else:
            frame = self.copy()

        if col is None:
            col = self.trixels()

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
            df = df[df.geometry.is_empty is False]
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
        Parameters
        ----------
        other: Collection of STARE indices.
            The STARE index collection representing the object to find the
            intersection with.
        """
        data = []
        for srange in self[self._stare_column_name]:
            data.append(pystare.intersect(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, dissolve_sids=True, n_workers=1, n_chunks=1, geom=False, by=None, aggfunc="first",
                       as_index=True, level=None, sort=True, observed=False, dropna=True):
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
