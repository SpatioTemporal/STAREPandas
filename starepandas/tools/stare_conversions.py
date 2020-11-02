import dask
import shapely
import numpy
import pystare


def stare_from_gdf(gdf, level=-1, nonconvex=True, force_ccw=True):
    """
    Takes a GeoDataFrame and returns a corresponding series of sets of trixel indices
    """
    if not gdf._geometry_column_name in gdf.keys():
        print('no geom column set')
    
    if set(gdf.geom_type) == {'Point'}:
        lat = gdf.geometry.y
        lon = gdf.geometry.x
        return pystare.from_latlon(lat, lon, level)    
    else: 
        index_values = []
        for geom in gdf.geometry:
            if geom.type == 'Polygon':
                index_values.append(from_polygon(geom, level, nonconvex, force_ccw))
            elif geom.type == 'MultiPolygon':                
                index_values.append(from_multipolygon(geom, level, nonconvex, force_ccw))
            elif geom.type == 'Point':
                index_values.append(from_point(geom, level))
        return index_values        
    

def stare_from_geoseries(series, level=-1, nonconvex=True, force_ccw=True):
    """
    Takes a GeoSeries and returns a corresponding series of sets of trixel indices
    """
    index_values = []
    for geom in series:
        if geom.type == 'Polygon':
            index_values.append(from_polygon(geom, level, nonconvex, force_ccw))
        elif geom.type == 'MultiPolygon':                
            index_values.append(from_multipolygon(geom, level, nonconvex, force_ccw))
        elif geom.type == 'Point':
            index_values.append(from_point(geom, level))
    return index_values      
    
def stare_from_xy(lon, lat, level=-1, n_cores=1):
    return pystare.from_latlon(lat, lon, level)


def stare_row(row, level):    
    """ Dask helper function """
    return pystare.from_latlon(row.lat, row.lon, level)


def stare_from_xy_df(df, level=-1, n_cores=1):
    """ Takes a dataframe; assumes columns with lat and lon"""
    rename_dict = {}
    rename_dict['Latitude'] = 'lat'
    rename_dict['latitude'] = 'lat'
    rename_dict['y'] = 'lat'
    rename_dict['Longitude'] = 'lon'
    rename_dict['longitude'] = 'lon'
    rename_dict['x'] = 'lon'    
    df = df.rename(columns=rename_dict) 
    if n_cores>1:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_cores)
        return ddf.map_partitions(stare_row, level, meta=('stare', 'int')).compute(scheduler='processes')    
    else: 
        return pystare.from_latlon(df.lat, df.lon, level)


def to_trixels(sids, as_multipolygon=False):
    if isinstance(sids, (numpy.int64, int)):
        # If single value was passed
        sids = [sids]
    
    if isinstance(sids, (numpy.ndarray)):
        # This is not ideal, but when we read sidecars, we get unit64 and have to cast
        sids = sids.astype(numpy.int64)
    latv, lonv = pystare._to_vertices_latlon(sids)
    for i in range(len(latv)):
        latv[i] = pystare.shiftarg_lat(latv[i])
        lonv[i] = pystare.shiftarg_lon(lonv[i])
    i = 0    
    trixels = []
    while i < len(latv):
        geom = shapely.geometry.Polygon([[lonv[i], latv[i]], [lonv[i+1], latv[i+1]], [lonv[i+2], latv[i+2]]])
        trixels.append(geom)
        i += 4    
    if i == 4:
        trixels = trixels[0]
    elif as_multipolygon:
        trixels = shapely.geometry.MultiPolygon(trixels)        
    return trixels   

       
# Shapely 
def from_shapely(geom, level=-1, force_ccw=False):
    """ Wrapper"""
    if geom.geom_type == 'Point':
        return from_point(geom, level)
    if geom.geom_type == 'Polygon':
        return from_polygon(geom, level)
    if geom.geom_type == 'MultiPolygon':
        return from_multipolygon(geom, level)
    
    
def from_point(point, level=-1):
    lat = point.y
    lon = point.x
    index_value = pystare.from_latlon([lat], [lon], level)[0]
    return index_value


def from_boundary(boundary, level=-1, nonconvex=True, force_ccw=False):
    """ 
    Return a range of indices covering the region circumscribed by the polygon. 
    Node orientation is relevant and CW
    """    
    if force_ccw and not boundary.is_ccw:
        boundary.coords = list(boundary.coords)[::-1]
    latlon = boundary.coords.xy
    lon = latlon[0]
    lat = latlon[1]
    if nonconvex:
       range_indices = pystare.to_nonconvex_hull_range_from_latlon(lat, lon, level)
    else:
       range_indices = pystare.to_hull_range_from_latlon(lat, lon, level)
    return range_indices


def from_polygon(polygon, level=-1, nonconvex=True, force_ccw=False):
    """
    A Polygon is a planar Surface defined by 1 exterior boundary and 0 or more interior boundaries. Each interior
    boundary defines a hole in the Polygon. A Triangle is a polygon with 3 distinct, non-collinear vertices and no interior boundary. """

    if force_ccw:
        polygon = shapely.geometry.polygon.orient(polygon)
    sids_ext = from_boundary(polygon.exterior, level, nonconvex, force_ccw)
    sids_int = []
    for interior in polygon.interiors:
        sids_int += list(from_boundary(interior, level, nonconvex, force_ccw))
    return sids_ext
        
    
def from_multipolygon(multipolygon, level=-1, nonconvex=True, force_ccw=False):
    range_indices = []
    for polygon in multipolygon.geoms:
        range_indices += list(from_polygon(polygon, level, nonconvex, force_ccw))
    return range_indices

