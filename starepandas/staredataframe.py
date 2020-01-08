import geopandas
import starepandas.pystare


class STAREDataFrame(geopandas.GeoDataFrame):
    
    def add_stare(self, level=None):
        """ Adds a STARE column to dataframe containing 
        the STARE representation of the geometries"""       
        self = self.assign(stare=starepandas.pystare.from_geopandas(self, resolution=10))
    
    def stare_intersects(self, other):
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

        pass
    
    def stare_intersection(self, other):
        """Returns a ``STARESeries`` of the spatial intersection of self with `other`.
        Parameters
        ----------
        other: Collection of STARE indices.
            The STARE index collection representing the object to find the
            intersection with.
        """
        
        pass
    

    def trixels(self):
        """Returns a Polygon or Multipolygon GeoSeries 
        containing the trixels referred by the STARE indices"""
        return starepandas.pystare.to_trixels_series(self.stare)
    
    def add_trixels(self):
        self = self.assign(stare=starepandas.pystare.from_geopandas(self, resolution=10))
