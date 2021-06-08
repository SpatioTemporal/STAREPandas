import dask.dataframe
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

    Example::

    >>> import geopandas
    >>> import starepandas
    >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    >>> italy = world[world.name=='Italy']
    >>> starepandas.stare_from_gdf(italy, level=3, convex=False, force_ccw=True, n_workers=1)
    [array([4269412446747230211, 4548635623644200963, 4566650022153682947,
           4548635623644200963, 4548635623644200963])]
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


def stare_from_geoseries(series, level=0, convex=False, force_ccw=True, n_workers=1):
    """
    Takes a GeoSeries and returns a corresponding series of sets of trixel indices

    :param series: A geopandas.GeoSeries containing features to look (sets of) STARE indices up for
    :param level: STARE level
    :param convex: Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    :param force_ccw: Toggle if a counter clockwise orientation of the geometries should be enforced
    :param n_workers: Number of workers used to lookup STARE indices in parallel
    :return: A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry
    :rtype: numpy.ndarray

    Example::

    >>> import geopandas
    >>> import starepandas
    >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    >>> germany = world[world.name=='Germany']
    >>> starepandas.stare_from_geoseries(germany.geometry, level=3, convex=True, force_ccw=True, n_workers=1)
    [array([4251398048237748227, 4269412446747230211, 4278419646001971203,
           4539628424389459971, 4548635623644200963, 4566650022153682947])]
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


def stare_from_xy(lon, lat, level=0):
    """Takes a list/array of lon and lat and returns a (set of) STARE index/ices

    :param lon: Longitude of point
    :param lat: Latitude of point
    :param level: STARE Level
    :return: Array of STARE index values
    :rtype: numpy.ndarray

    Example::

    >>> import starepandas
    >>> x = [10.1, 20.9]
    >>> y = [55.3, 60.1]
    >>> starepandas.stare_from_xy(x, y, level=15)
    array([4254264869405326191, 3640541580264132591])
    """
    return pystare.from_latlon(lat, lon, level)


def stare_row(row, level):
    """ Dask helper function """
    return pystare.from_latlon(row.lat, row.lon, level)


def stare_from_xy_df(df, level=0, n_workers=1):
    """ Takes a dataframe and generates an array of STARE index values.
    Assumes latitude column name is {'lat', 'Latitude', 'latitude', or 'y'} and
    longitude column name is {'lon', 'Longitude', 'longitude', or 'x'}

    :param df: Dataframe containing x/y coordinates
    :type df: pandas.DataFrame
    :param level: STARE Level
    :type level: int
    :param n_workers: Number of workers used to lookup STARE indices in parallel
    :type n_workers: int
    :return: Array of STARE index values
    :rtype: numpy.ndarray

    Example::

    >>> import starepandas
    >>> import pandas
    >>> x = [-119.42, 7.51]
    >>> y = [34.25, 47.59]
    >>> df = pandas.DataFrame({'lat': y, 'lon': x})
    >>> starepandas.stare_from_xy_df(df, level=20)
    array([3331752989521980116, 4271829667422230484])
    """
    rename_dict = {'Latitude': 'lat', 'latitude': 'lat', 'y': 'lat',
                   'Longitude': 'lon', 'longitude': 'lon', 'x': 'lon'}
    df = df.rename(columns=rename_dict)
    if n_workers > 1:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_workers)
        return ddf.map_partitions(stare_row, level, meta=('stare', 'int')).compute(scheduler='processes')
    else:
        return pystare.from_latlon(df.lat, df.lon, level)


def trixels_from_stareseries(sids_series, n_workers=1):
    """ Takes a series of STARE index values and creates an array of sets of trixels. If a row contains a set of sids
    (rather than a single sid); i.e. representing e.g. a region, a set of trixels will be generated and combined in a
    multipolygon

    :param sids_series: Series or array-like with STARE index values
    :type sids_series: array like
    :param n_workers: number of workers to use to lookup geometries in parallel
    :type n_workers: int
    :return: array like of trixels / triangle geometries
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> sids = [4611686018427387903, 4611686018427387903]
    >>> sdf = starepandas.STAREDataFrame(stare=sids)
    >>> trixels = starepandas.trixels_from_stareseries(sdf.stare)
    """
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
    """ Converts a (collection of) sid(s) into a (collection of) trixel(s)

    :param sids: (Collection of) STARE index value(s)
    :type sids: int64 or array-like
    :param as_multipolygon: If more than one sid is passed, toggle if the resulting trixels should be combined into a
        multipolygon
    :type as_multipolygon: bool
    :return: array like of trixels
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> sids = [4611686018427387903, 4611686018427387903]
    >>> trixels = starepandas.to_trixels(sids, as_multipolygon=True)
    """
    if isinstance(sids, (numpy.int64, int)):
        # If single value was passed
        sids = [sids]

    if isinstance(sids, numpy.ndarray):
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


def from_geom_row(row, level):
    """ Dask Helper function"""
    return from_shapely(row.geometry)


def from_shapely(geom, level=-1, convex=False, force_ccw=False):
    """ Wrapper for starepandas.from_point(), starepandas.from_ring(),
    starepandas.from_polygon(), starepandas.from_multipolygon()

    Takes a shapely Point, Polygon, or Multipolygon and looks up the STARE representation

    :param geom: A shapely geometry to look the sids up for
    :type geom: shapely.geometry.Point, shapely.geometry.Polygon, shapely.geometry.MultiPolygon
    :param level: Maximum STARE level to use for lookup
    :type level: int
    :param convex: Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry
        of the polygon.
    :type convex: bool
    :param force_ccw: Toggle if counter-clockwise should be forced. Counter clockwise meaning that the
        "inside" of a ring will always be left of its vertices and edges.
    :type force_ccw: bool
    :return: collection of sids
    :rtype: array-like

    Example::

    >>> import shapely
    >>> import starepandas
    >>> point = shapely.geometry.Point(10.5, 20)
    >>> starepandas.from_shapely(point)
    4611686018427387903

    >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> starepandas.from_shapely(polygon1, force_ccw=True, level=6)
    array([4430697608402436102, 4430838345890791430, 4430979083379146758])

    >>> polygon2 = shapely.geometry.Polygon([(5, 5), (6, 6), (6, 5)])
    >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    >>> starepandas.from_shapely(multipolygon, force_ccw=True, level=5)
    array([4430416133425725445, 4430979083379146757, 4416905334543613957])

    """
    if geom.geom_type == 'Point':
        return from_point(geom, level=level)
    if geom.geom_type == 'Polygon':
        return from_polygon(geom, level=level, convex=convex, force_ccw=force_ccw)
    if geom.geom_type == 'MultiPolygon':
        return from_multipolygon(geom, level=level, convex=convex, force_ccw=force_ccw)


def from_point(point, level=-1):
    """Takes a shapely Point, Polygon, or Multipolygon and looks up the STARE representation

    :param point: Shapely point
    :type point: shapely.geometry.Point
    :param level: STARE level to use for lookup
    :type level: int
    :return: collection of sids
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> import shapely
    >>> point = shapely.geometry.Point(10.5, 20)
    >>> starepandas.from_point(point, level=20)
    4598246232954051060
    """
    lat = point.y
    lon = point.x
    index_value = pystare.from_latlon([lat], [lon], level)[0]
    return index_value


def from_ring(ring, level=-1, convex=False, force_ccw=False):
    """ Return a range of indices covering the region inside/outside ring.
    Node orientation is relevant!

    :param ring: Ring to lookup sids for
    :type ring: shapely.geometry.polygon.LinearRing
    :param level: (Maximum) STARE level to use for lookup. Warning: default is maximum level (27)
    :type level: int
    :param convex: Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry
        of the ring
    :type convex: bool
    :param force_ccw: toggle if orientation of ring should be overwritten and ring should be interpreted as outer
        boundary.
    :type force_ccw: bool
    :return: collection of sids
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> import shapely
    >>> # Note: the ring is clockwise!
    >>> polygon = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> starepandas.from_ring(polygon.exterior, force_ccw=True, level=6)
    array([4430697608402436102, 4430838345890791430, 4430979083379146758])
    """
    if force_ccw and not ring.is_ccw:
        ring.coords = list(ring.coords)[::-1]
    latlon = ring.coords.xy
    lon = latlon[0]
    lat = latlon[1]
    if convex:
        range_indices = pystare.to_hull_range_from_latlon(lat, lon, level)
    else:
        range_indices = pystare.to_nonconvex_hull_range_from_latlon(lat, lon, level)

    return range_indices


def from_polygon(polygon, level=-1, convex=False, force_ccw=False):
    """ Lookup STARE index values for a polygon.
    A Polygon is a planar Surface defined by 1 exterior ring and 0 or more interior boundaries. Each interior
    ring defines a hole in the Polygon.

    :param polygon: Polygon to look sids up for
    :type polygon: shapely.geometry.Polygon
    :param level: (Maximum) STARE level to use for lookup: Warning: default is maximum level (27)
    :type level: int
    :param convex: Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry.
        This will be applied for both the exterior ring and the interior rings.
    :type convex: bool
    :param force_ccw: toggle if orientation of ring should be overwritten and ring should be interpreted as outer
        boundary. Note: Interior rings will always be forced to be clockwise!
    :type force_ccw: Bool
    :return: collection of sids
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> import shapely
    >>> polygon = shapely.geometry.Polygon([(0, 0), (2, 0), (1, 1)])
    >>> starepandas.from_polygon(polygon, level=5)
    array([4423097784031248389, 4430416133425725445, 4430979083379146757])
    """

    if force_ccw:
        polygon = shapely.geometry.polygon.orient(polygon)
    sids_ext = from_ring(polygon.exterior, level, convex, force_ccw)

    if len(polygon.interiors) > 0:
        sids_int = []
        for interior in polygon.interiors:
            if interior.is_ccw:
                interior.coords = list(interior.coords)[::-1]
            sids_int.append(from_ring(interior, level, convex, force_ccw=False))
        sids_int = numpy.concatenate(sids_int)
        sids = pystare.intersect(sids_int, sids_ext)
    else:
        sids = sids_ext
    return sids


def from_multipolygon(multipolygon, level=-1, convex=False, force_ccw=False):
    """ Lookup STARE index values for a multipolygon.

    :param polygon: Polygon to look sids up for
    :type polygon: shapely.geometry.Polygon
    :param level: (Maximum) STARE level to use for lookup: Warning: default is maximum level (27)
    :type level: int
    :param convex: Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry.
        This will be applied for both the exterior ring and the interior rings.
    :type convex: bool
    :param force_ccw: toggle if orientation of ring should be overwritten and ring should be interpreted as outer
        boundary. Note: Interior rings will always be forced to be clockwise!
    :type force_ccw: Bool
    :return: collection of sids
    :rtype: array-like

    Example::

    >>> import starepandas
    >>> import shapely
    >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> polygon2 = shapely.geometry.Polygon([(3, 1), (4, 2), (2, 1)])
    >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    >>> starepandas.from_multipolygon(multipolygon, force_ccw=True, level=3)
    array([4422534834077827075, 4413527634823086083, 4422534834077827075])
    """
    range_indices = []
    for polygon in multipolygon.geoms:
        range_indices.append(from_polygon(polygon, level, convex, force_ccw))
    range_indices = numpy.concatenate(range_indices)
    return range_indices


def dissolve(sids):
    """ Dissolve STARE index values.

    :param sids:
    :type sids:
    :return: dissolved
    :rtype: array-like


    .. image:: ../../../_static/dissolve.png

    """
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
