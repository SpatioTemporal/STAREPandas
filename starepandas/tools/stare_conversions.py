import dask
import shapely

# https://github.com/numpy/numpy/issues/14868
import os

os.environ["OMP_NUM_THREADS"] = "1"

import numpy
import pystare
import multiprocessing


def stare_from_gdf(gdf, level=-1, convex=False, force_ccw=True, n_workers=1):
    """
    Takes a GeoDataFrame and returns a corresponding series of sets of trixel indices

    :param gdf: A geopandas.GeoDataFrame containing features to look (sets of) STARE indices up for
    :param level: STARE level
    :param convex: Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    :param force_ccw: Toggle if a counter clockwise orientation of the geometries should be enforced
    :param n_workers: Number of workers used to lookup STARE indices in parallel
    :return: A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry
    :rtype: numpy.ndarray
    """

    if gdf._geometry_column_name in gdf.keys():
        pass
    else:
        print('no geom column set')

    if set(gdf.geom_type) == {'Point'}:
        # This might be a speedup since we don't need to iterate over python lists
        lat = gdf.geometry.y
        lon = gdf.geometry.x
        return pystare.from_latlon(lat, lon, level)
    else:
        return stare_from_geoseries(gdf.geometry, level=level, convex=convex, force_ccw=force_ccw, n_workers=n_workers)


def stare_from_geoseries(series, level=-1, convex=False, force_ccw=True, n_workers=1):
    """
    Takes a GeoSeries and returns a corresponding series of sets of trixel indices

    :param series: A geopandas.GeoSeries containing features to look (sets of) STARE indices up for
    :param level: STARE level
    :param convex: Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    :param force_ccw: Toggle if a counter clockwise orientation of the geometries should be enforced
    :param n_workers: Number of workers used to lookup STARE indices in parallel
    :return: A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry
    :rtype: numpy.ndarray
    """
    if n_workers > len(series):
        # Cannot have more partitions than rows
        n_workers = len(series) - 1

    if n_workers == 1:
        stare = []
        for geom in series:
            sids = from_shapely(geom=geom, level=level, convex=convex, force_ccw=force_ccw)
            stare.append(sids)
    else:
        ddf = dask.dataframe.from_pandas(series, npartitions=n_workers)
        meta = {'stare': 'int64'}
        res = ddf.map_partitions(lambda df: numpy.array(stare_from_geoseries(df, level, convex, force_ccw, 1)),
                                 meta=meta)
        stare = res.compute(scheduler='processes')
    return stare


def stare_from_xy(lon, lat, level=-1):
    """
    Takes a single lon and lat or a list/array of lon and lat and returns a (set of) STARE index/ices

    :param lon: Longitude of point
    :param lat: Latitude of point
    :param level: STARE Level
    :return: Single STARE index value or array of STARE index values
    :rtype: int64 or numpy.ndarray
    """
    return pystare.from_latlon(lat, lon, level)


def stare_row(row, level):
    """ Dask helper function """
    return pystare.from_latlon(row.lat, row.lon, level)


def stare_from_xy_df(df, level=-1, n_cores=1):
    """ Takes a dataframe; assumes columns with lat and lon"""
    rename_dict = {'Latitude': 'lat', 'latitude': 'lat', 'y': 'lat', 'Longitude': 'lon', 'longitude': 'lon', 'x': 'lon'}
    df = df.rename(columns=rename_dict)
    if n_cores > 1:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_cores)
        return ddf.map_partitions(stare_row, level, meta=('stare', 'int')).compute(scheduler='processes')
    else:
        return pystare.from_latlon(df.lat, df.lon, level)


def trixels_from_stareseries(sids_series, n_workers=1):
    if n_workers > len(sids_series):
        # Cannot have more partitions than rows        
        n_workers = len(sids_series) - 1

    if n_workers == 1:
        trixels_series = []
        for sids in sids_series:
            trixels = to_trixels(sids, as_multipolygon=True)
            trixels_series.append(trixels)
    else:
        ddf = dask.dataframe.from_pandas(sids_series, npartitions=n_workers)
        meta = {'trixels': 'object'}
        res = ddf.map_partitions(lambda df: numpy.array(trixels_from_stareseries(df, 1)), meta=meta)
        trixels_series = res.compute(scheduler='processes')
    return trixels_series


def to_trixels(sids, as_multipolygon=False):
    if isinstance(sids, (numpy.int64, int)):
        # If single value was passed
        sids = [sids]

    if isinstance(sids, (numpy.ndarray)):
        # This is not ideal, but when we read sidecars, we get unit64 and have to cast
        sids = sids.astype(numpy.int64)

    lons, lats, intmat = pystare.triangulate_indices(sids)

    i = 0
    trixels = []
    while i < len(lats):
        geom = shapely.geometry.Polygon([[lons[i], lats[i]], [lons[i + 1], lats[i + 1]], [lons[i + 2], lats[i + 2]]])
        trixels.append(geom)
        i += 3

    if i == 3:
        trixels = trixels[0]
    elif as_multipolygon:
        trixels = shapely.geometry.MultiPolygon(trixels)
    return trixels


# Shapely 

def from_geom_row(row, level):
    return from_shapely(row.geometry)


def from_shapely(geom, level=-1, convex=False, force_ccw=False):
    """ Wrapper"""
    if geom.geom_type == 'Point':
        return from_point(geom, level=level)
    if geom.geom_type == 'Polygon':
        return from_polygon(geom, level=level, convex=convex, force_ccw=force_ccw)
    if geom.geom_type == 'MultiPolygon':
        return from_multipolygon(geom, level=level, convex=convex, force_ccw=force_ccw)


def from_point(point, level=-1):
    lat = point.y
    lon = point.x
    index_value = pystare.from_latlon([lat], [lon], level)[0]
    return index_value


def from_boundary(boundary, level=-1, convex=False, force_ccw=False):
    """ 
    Return a range of indices covering the region circumscribed by the polygon. 
    Node orientation is relevant and CW
    """
    if force_ccw and not boundary.is_ccw:
        boundary.coords = list(boundary.coords)[::-1]
    latlon = boundary.coords.xy
    lon = latlon[0]
    lat = latlon[1]
    if convex:
        range_indices = pystare.to_hull_range_from_latlon(lat, lon, level)
    else:
        range_indices = pystare.to_nonconvex_hull_range_from_latlon(lat, lon, level)

    return range_indices


def from_polygon(polygon, level=-1, convex=False, force_ccw=False):
    """
    A Polygon is a planar Surface defined by 1 exterior boundary and 0 or more interior boundaries. Each interior
    boundary defines a hole in the Polygon. A Triangle is a polygon with 3 distinct, non-collinear vertices and no interior boundary. """

    if force_ccw:
        polygon = shapely.geometry.polygon.orient(polygon)
    sids_ext = from_boundary(polygon.exterior, level, convex, force_ccw)

    if len(polygon.interiors) > 0:
        sids_int = []
        for interior in polygon.interiors:
            if interior.is_ccw:
                interior.coords = list(interior.coords)[::-1]
            sids_int.append(from_boundary(interior, level, convex, force_ccw=False))
        sids_int = numpy.concatenate(sids_int)
        sids = pystare.intersect(sids_int, sids_ext)
    else:
        sids = sids_ext
    return sids


def from_multipolygon(multipolygon, level=-1, convex=False, force_ccw=False):
    range_indices = []
    for polygon in multipolygon.geoms:
        range_indices.append(from_polygon(polygon, level, convex, force_ccw))
    range_indices = numpy.concatenate(range_indices)
    return range_indices


def _merge_stare(sids, coerce_sids=True):
    sids = numpy.concatenate(list(sids))
    sids = numpy.unique(sids)
    if coerce_sids:
        s_range = pystare.to_compressed_range(sids)
        sids = pystare.expand_intervals(s_range, -1, multi_resolution=True)
    return list(sids)


def dissolve(sids):
    s_range = pystare.to_compressed_range(sids)
    expanded = pystare.expand_intervals(s_range, -1, multi_resolution=True)
    return expanded


def merge_stare(sids, dissolve_sids=True, n_workers=1, n_chunks=1):
    sids = numpy.concatenate(list(sids))
    dissolved = numpy.unique(sids)

    if not dissolve_sids:
        return list(dissolved)

    if n_workers == 1 and n_chunks == 1:
        dissolved = dissolve(dissolved)
    else:
        if n_workers > 1:
            chunks = numpy.array_split(dissolved, n_workers)
            with multiprocessing.Pool(processes=n_workers) as pool:
                dissolved = pool.map(dissolve, chunks)
        elif n_chunks > 1:
            chunks = numpy.array_split(dissolved, n_chunks)
            dissolved = []
            for chunk in chunks:
                dissolved.append(dissolve(chunk))
        dissolved = numpy.concatenate(dissolved)
        dissolved = numpy.unique(dissolved)
        dissolved = dissolve(dissolved)

    return list(dissolved)


def series_intersects(other, series, method=1, n_workers=1):
    """ 
    Returns a bool series of length len(series).
    True for every row in which row intersects other.    
    """

    if n_workers > len(series):
        # Cannot have more partitions than rows        
        n_workers = len(series)

    if n_workers == 1:
        if series.dtype == numpy.int64:
            # We have a series of sids; don't need to iterate. Can send the whole array to pystare/
            intersects = pystare.intersects(other, series, method)
        else:
            intersects = []
            for sids in series:
                if len(sids) < len(other):
                    # If we do method 1, larger item first is faster
                    intersects.append(pystare.intersects(other, sids, method).any())
                else:
                    intersects.append(pystare.intersects(sids, other, method).any())
            intersects = numpy.array(intersects)
    else:
        ddf = dask.dataframe.from_pandas(series, npartitions=n_workers)
        meta = {'intersects': 'bool'}
        res = ddf.map_partitions(lambda df: series_intersects(other, df, method, 1), meta=meta)
        intersects = res.compute(scheduler='processes')
    return intersects
