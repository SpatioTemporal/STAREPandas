import geopandas
import pystare
import pandas
import numpy
import dask.dataframe
import shapely.geometry
import starepandas


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
    
    _stare_column_name = 'stare'
    _trixel_column_name = 'trixels'
    
    def __init__(self, *args, **kwargs):
        stare = kwargs.pop("stare", None)
        super(STAREDataFrame, self).__init__(*args, **kwargs)
        
        # We need this to solve https://github.com/geopandas/geopandas/issues/1179
        # Should verify that it does not cause a performance hit
        self._data = self._data.copy()
        
        # We carry over the geometry column name
        if isinstance(*args, geopandas.GeoDataFrame):
            self._geometry_column_name = args[0]._geometry_column_name
        
        if stare is not None:
            self.set_stare(stare, inplace=True)
            
            
    def __getitem__(self, key):
        result = super(STAREDataFrame, self).__getitem__(key)
        stare_col = self._stare_column_name
        if isinstance(result, (geopandas.GeoDataFrame, pandas.DataFrame)) and stare_col in result:
            result.__class__ = STAREDataFrame
            result._stare_column_name = stare_col
        elif  isinstance(result, geopandas.GeoSeries) and key==stare_col:            
            pass
            #result.__class__ = starepandas.STARESeries
        else:
            pass
            #result.__class__ = geopandas.GeoDataFrame

        return result
    
    @property
    def _constructor(self):
        return STAREDataFrame

            
    def set_stare(self, col, inplace=False):
        """
        Set the StareDataFrame stare indices using either an existing column or
        the specified input. By default yields a new object.
        The original stare column is replaced with the input.
        Parameters
        ----------
        col : array-like of stare indices
        inplace : boolean, default False
            Modify the StareDataFrame  in place (do not create a new object)
        Examples
        --------
        >>> df = df.set_geometry([[4611686018427387903, 4611686018427387903, 4611686018427387903])        
        Returns
        -------
        StareDataFrame """
        
        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.copy()
                
        if isinstance(col, (pandas.Series, list, numpy.ndarray)):
            frame[self._stare_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        else:
            raise ValueError("Must pass array-like object")

        if not inplace:
            return frame


    def trixels(self, stare_column=None):
        """Returns a Polygon or Multipolygon GeoSeries 
        containing the trixels referred by the STARE indices"""
        trixels_series = []
        if stare_column==None:
            stare_column=self._stare_column_name
        for index_values in self[stare_column]:
            trixels = starepandas.to_trixels(index_values, as_multipolygon=True)
            trixels_series.append(trixels)        
        return trixels_series
            
            
    def set_trixels(self, trixels=None, inplace=False):
        if trixels is None:
            trixels = self.trixels()
        if inplace:
            frame = self
        else:
            frame = self.copy()
        frame[self._trixel_column_name] = trixels
        if not inplace:
            return frame
    
    
    def plot(self, trixels=False, boundary=False, *args, **kwargs):
        if trixels==True:
            boundary = True
            df = self.set_geometry(self._trixel_column_name)
        else:
            df = self
        if boundary:
            df = df[df.geometry.is_empty==False]
            df = df.set_geometry(df.geometry.boundary)
        return super(STAREDataFrame, df).plot(*args, **kwargs)
    
    
    def to_scidb(self, connection):
        pass
        
            
    def stare_intersects(self, other, method=1):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that intersects `other`.
        An object is said to intersect `other` if its `boundary` and `interior`
        intersects in any way with those of the other.
        Parameters
        ----------
        other: Collection of STARE indices 
            The STARE index collection representing the spatial object to test if is
            intersected.
        """
        
        if isinstance(other, (int, numpy.int64)):
            other = [other]
            
        if self.stare.dtype == numpy.int64:
            data = pystare.intersects(other, self.stare, method)
        else:
            data = []
            for srange in self.stare:                                
                data.append(pystare.intersects(srange, other, method).any())
        return pandas.Series(data, index=self.index)
    
    
    def stare_disjoint(self, other, method=1):
        return self.stare_intersects(other, method)==False
        
    
    def stare_intersection(self, other):
        """Returns a ``STARESeries`` of the spatial intersection of self with `other`.
        Parameters
        ----------
        other: Collection of STARE indices.
            The STARE index collection representing the object to find the
            intersection with.
        """        
        data = []
        for srange in self.stare:
            data.append(pystare.intersect(srange, other))
        return pandas.Series(data, index=self.index)



