import dask.dataframe
import shapely
import pandas

# https://github.com/numpy/numpy/issues/14868
import os

os.environ["OMP_NUM_THREADS"] = "1"

import numpy
import pystare
import multiprocessing


def sids_from_gdf(gdf, resolution, convex=False, force_ccw=True, n_workers=1):
    """
    Takes a GeoDataFrame and returns a corresponding series of sets of trixel indices

    Parameters
    -----------
    gdf: geopandas.GeoDataFrame
        A dataframe containing features to look (sets of) STARE indices up for
    resolution: int
        STARE resolution
    convex: bool
        Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    force_ccw: bool
        Toggle if a counter clockwise orientation of the geometries should be enforced
    n_workers: int
        Number of workers used to lookup STARE indices in parallel

    Returns
    --------
    A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry

    Examples
    ---------
    >>> import geopandas
    >>> import starepandas
    >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    >>> italy = world[world.name=='Italy']
    >>> starepandas.sids_from_gdf(italy, resolution=3, convex=False, force_ccw=True, n_workers=1)
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
        return pystare.from_latlon(lat, lon, resolution)
    else:
        return sids_from_geoseries(gdf.geometry, resolution=resolution, convex=convex, force_ccw=force_ccw, n_workers=n_workers)


def sids_from_geoseries(series, resolution, convex=False, force_ccw=True, n_workers=1):
    """
    Takes a GeoSeries and returns a corresponding series of sets of trixel indices

    Parameters
    -----------
    series: geopandas.GeoSeries
        A geopandas.GeoSeries containing features to look (sets of) STARE indices up for
    resolution: int
        STARE resolution
    convex: bool
        Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    force_ccw: bool
        Toggle if a counter clockwise orientation of the geometries should be enforced
    n_workers: int
        Number of workers used to lookup STARE indices in parallel
    
    Returns
    --------
    sids
        A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry

    Examples
    ------------
    >>> import geopandas
    >>> import starepandas
    >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    >>> germany = world[world.name=='Germany']
    >>> starepandas.sids_from_geoseries(germany.geometry, resolution=3, convex=True)
    [array([4251398048237748227, 4269412446747230211, 4278419646001971203,
           4539628424389459971, 4548635623644200963, 4566650022153682947])]
    """
    if n_workers > len(series):
        # Cannot have more partitions than rows
        n_workers = len(series) - 1

    if n_workers == 1:
        stare = []
        for geom in series:
            sids = sids_from_shapely(geom=geom, resolution=resolution, convex=convex, force_ccw=force_ccw)
            stare.append(sids)
    else:
        ddf = dask.dataframe.from_pandas(series, npartitions=n_workers)
        meta = {'stare': 'int64'}
        res = ddf.map_partitions(lambda df: numpy.array(sids_from_geoseries(df, resolution, convex, force_ccw, 1)),
                                 meta=meta)
        stare = res.compute(scheduler='processes')
    return stare


def sids_from_xy(lon, lat, resolution):
    """Takes a list/array of lon and lat and returns a (set of) STARE index/ices

    Parameters
    -----------
    lon: numerical/float
        Longitude of point
    lat: numerical/float
        Latitude of point
    resolution: int
        STARE spatial resolution

    Returns
    ----------
    sids
        Array of STARE index values

    Examples
    ----------
    >>> import starepandas
    >>> x = [10.1, 20.9]
    >>> y = [55.3, 60.1]
    >>> starepandas.sids_from_xy(x, y, resolution=15)
    array([4254264869405326191, 3640541580264132591])
    """
    return pystare.from_latlon(lat, lon, resolution)


def sids_from_latlon_row(row, resolution):
    """ Dask helper function """
    return pystare.from_latlon(row.lat, row.lon, resolution)


def sids_from_xy_df(df, resolution, n_workers=1):
    """ Takes a dataframe and generates an array of STARE index values.
    Assumes latitude column name is {'lat', 'Latitude', 'latitude', or 'y'} and
    longitude column name is {'lon', 'Longitude', 'longitude', or 'x'}

    Parameters 
    --------------
    df: pandas.DataFrame
        Dataframe containing x/y coordinates    
    resolution: int
        STARE spatial resolution    
    n_workers: int
        Number of workers used to lookup STARE indices in parallel
        
    Returns
    --------
    sids     
        Array of STARE index values    

    Examples
    ------------
    >>> import starepandas
    >>> import pandas
    >>> x = [-119.42, 7.51]
    >>> y = [34.25, 47.59]
    >>> df = pandas.DataFrame({'lat': y, 'lon': x})
    >>> starepandas.sids_from_xy_df(df, resolution=20)
    array([3331752989521980116, 4271829667422230484])
    """
    rename_dict = {'Latitude': 'lat', 'latitude': 'lat', 'y': 'lat',
                   'Longitude': 'lon', 'longitude': 'lon', 'x': 'lon'}
    df = df.rename(columns=rename_dict)
    if n_workers > 1:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_workers)
        return ddf.map_partitions(sids_from_latlon_row, resolution, meta=('stare', 'int')).compute(scheduler='processes')
    else:
        return pystare.from_latlon(df.lat, df.lon, resolution)


def sids_from_geom_row(row, resolution):
    """ Dask Helper function"""
    return sids_from_shapely(geom=row.geometry, resolution=resolution)


def sids_from_shapely(geom, resolution, convex=False, force_ccw=False):
    """ Wrapper for starepandas.from_point(), starepandas.from_ring(),
    starepandas.from_polygon(), starepandas.from_multipolygon()

    Takes a shapely Point, Polygon, or Multipolygon and looks up the STARE representation

    Parameters
    -------------
    geom: shapely.geometry.Point, shapely.geometry.Polygon, shapely.geometry.MultiPolygon
        A shapely geometry to look the sids up for
    resolution: int
        Maximum STARE resolution to use for lookup
    convex: bool
        Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry
        of the polygon.
    force_ccw: bool
        Toggle if counter-clockwise should be forced. Counter clockwise meaning that the
        "inside" of a ring will always be left of its vertices and edges.

    Returns
    ---------
    sids:
        collection of sids

    Examples
    ---------
    >>> import shapely
    >>> import starepandas

    Point:

    >>> point = shapely.geometry.Point(10.5, 20)
    >>> starepandas.sids_from_shapely(point, resolution=27)
    4598246232954051067

    Polygon:

    >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> starepandas.sids_from_shapely(polygon1, force_ccw=True, resolution=6)
    array([4430697608402436102, 4430838345890791430, 4430979083379146758])

    Multipolygon:

    >>> polygon2 = shapely.geometry.Polygon([(5, 5), (6, 6), (6, 5)])
    >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    >>> starepandas.sids_from_shapely(multipolygon, force_ccw=True, resolution=5)
    array([4430416133425725445, 4430979083379146757, 4416905334543613957])

    """
    if geom.geom_type == 'Point':
        return sid_from_point(geom, resolution=resolution)
    if geom.geom_type == 'Polygon':
        return sids_from_polygon(geom, resolution=resolution, convex=convex, force_ccw=force_ccw)
    if geom.geom_type == 'MultiPolygon':
        return sids_from_multipolygon(geom, resolution=resolution, convex=convex, force_ccw=force_ccw)


def sid_from_point(point, resolution):
    """Takes a shapely Point, Polygon, or Multipolygon and returns the according SID

    Parameters
    -------------
    point: shapely.geometry.Point
        Shapely point
    resolution: int
        STARE resolution to use for lookup

    Returns
    ----------
    sid
        spatial index value

    Examples
    ---------
    >>> import starepandas
    >>> import shapely
    >>> point = shapely.geometry.Point(10.5, 20)
    >>> starepandas.sid_from_point(point, resolution=20)
    4598246232954051060
    """
    lat = point.y
    lon = point.x
    index_value = pystare.from_latlon([lat], [lon], resolution)[0]
    return index_value


def sids_from_ring(ring, resolution, convex=False, force_ccw=False):
    """
    Return a range of indices covering the region inside/outside ring.
    Node orientation is relevant!

    Parameters
    ------------
    ring: shapely.geometry.polygon.LinearRing
        Ring to lookup sids for
    resolution: int
        Maximum STARE resolution to use for lookup.
    convex: bool
        Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry of the ring
    force_ccw: bool
        toggle if orientation of ring should be overwritten and ring should be interpreted as outerboundary.

    Returns
    ---------
    collection of sids

    Examples
    ---------
    >>> import starepandas
    >>> import shapely
    >>> # Note: the ring is clockwise!
    >>> polygon = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> starepandas.sids_from_ring(polygon.exterior, force_ccw=True, resolution=6)
    array([4430697608402436102, 4430838345890791430, 4430979083379146758])
    """
    if force_ccw and not ring.is_ccw:
        ring.coords = list(ring.coords)[::-1]
    latlon = ring.coords.xy
    lon = latlon[0]
    lat = latlon[1]
    if convex:
        range_indices = pystare.cover_from_hull(lat, lon, resolution)
    else:
        range_indices = pystare.cover_from_ring(lat, lon, resolution)

    return range_indices


def sids_from_polygon(polygon, resolution, convex=False, force_ccw=False):
    """ Lookup STARE index values for a polygon.
    A Polygon is a planar Surface defined by 1 exterior ring and 0 or more interior boundaries. Each interior
    ring defines a hole in the Polygon.

    Parameters
    --------------
    polygon: shapely.geometry.Polygon
        Polygon to look sids up for
    resolution: int
        Maximum STARE resolution to use for lookup
    convex: bool
        Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry.
        This will be applied for both the exterior ring and the interior rings.
    force_ccw: bool
        toggle if orientation of ring should be overwritten and ring should be interpreted as outer
        boundary. Note: Interior rings will always be forced to be clockwise!

    Returns
    ------------
    sids:
        collection of sids

    Examples
    ---------
    >>> import starepandas
    >>> import shapely
    >>> polygon = shapely.geometry.Polygon([(0, 0), (2, 0), (1, 1)])
    >>> starepandas.sids_from_polygon(polygon, resolution=5)
    array([4423097784031248389, 4430416133425725445, 4430979083379146757])
    """

    if force_ccw:
        polygon = shapely.geometry.polygon.orient(polygon)
    sids_ext = sids_from_ring(polygon.exterior, resolution, convex, force_ccw)

    if len(polygon.interiors) > 0:
        sids_int = []
        for interior in polygon.interiors:
            if interior.is_ccw:
                interior.coords = list(interior.coords)[::-1]
            sids_int.append(sids_from_ring(interior, resolution, convex, force_ccw=False))
        sids_int = numpy.concatenate(sids_int)
        sids = pystare.intersection(sids_int, sids_ext)
    else:
        sids = sids_ext
    return sids


def sids_from_multipolygon(multipolygon, resolution, convex=False, force_ccw=False):
    """ Lookup STARE index values for a multipolygon.

    Parameters
    ------------
    multipolygon: shapely.geometry.MultiPolygon
        MultiPolygon to look sids up for
    resolution: int
        Maximum STARE resolution to use for lookup
    convex: bool
        Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry.
        This will be applied for both the exterior ring and the interior rings.
    force_ccw: bool
        Toggle if orientation of ring should be overwritten and ring should be interpreted as outer
        boundary. Note: Interior rings will always be forced to be clockwise!

    Returns
    ---------
    sids
        An array of collection of sids

    Examples
    ----------

    >>> import starepandas
    >>> import shapely
    >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> polygon2 = shapely.geometry.Polygon([(3, 1), (4, 2), (2, 1)])
    >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    >>> starepandas.sids_from_multipolygon(multipolygon, force_ccw=True, resolution=3)
    array([4422534834077827075, 4413527634823086083, 4422534834077827075])
    """
    range_indices = []
    for polygon in multipolygon.geoms:
        range_indices.append(sids_from_polygon(polygon, resolution, convex, force_ccw))
    range_indices = numpy.concatenate(range_indices)
    return range_indices


def dissolve_stare(sids):
    """ Dissolve STARE index values.
    Combine/dissolve sibiling sids into the parent sids. That is:
    1. Any 4 siblings with the same parent in the collection get replaced by the parent. And
    2. Any child whose parents is in the collection will be removed

    Parameters
    ------------
    sids: array-like
        A collection of SIDs to dissolve

    Returns
    ---------
    dissolved: numpy.array
        Dissolved SIDs

    See Also
    ----------
    merge_stare

    Examples
    ---------
    >>> import starepandas
    >>> # The two latter SIDs are contained in the first SID
    >>> sids = [4035225266123964416, 4254212798004854789, 4255901647865118724]
    >>> starepandas.dissolve_stare(sids)
    array([4035225266123964416])

    Notes
    --------
    .. image:: ../../../_static/dissolve.png

    """
    sids = numpy.unique(sids)
    s_range = pystare.to_compressed_range(sids)
    expanded = pystare.expand_intervals(s_range, -1, multi_resolution=True)
    return expanded


def merge_stare(sids, dissolve_sids=True, n_workers=1, n_chunks=1):
    """ Merges a collection (of a collection) of sids. I.e.

    1. Flatten the colection
    2. Remove duplicates
    3. Combine/dissolve sibiling sids into the parent sids.  C.f. :func:`~dissolve_stare`

    See Also
    ----------
    dissolve_stare

    Parameters
    -----------
    sids: array / list
        a list of collections of SIDs; such as a Series of sids
    dissolve_sids: bool
        toggle if sids should be dissolved. I.e. combining ancestors into parent sids when possible
    n_workers: int
        number of workers to use. (only relevant for dissolve / if dissolve=True)
    n_chunks: int
         Performance parameter. If n_chunks >1, the sid collection will be split into n_chunks for the dissolve.

    Returns
    --------
    sids: numpy.array
        merged SIDs

    Examples
    ----------
    >>> import starepandas
    >>> import pandas
    >>> sids = pandas.Series([[4035225266123964416],
    ...                       [4254212798004854789, 4255901647865118724]])
    >>> starepandas.merge_stare(sids)
    [4035225266123964416]

    >>> sids = [4035225266123964416, 4254212798004854789, 4255901647865118724]
    >>> starepandas.merge_stare(sids)
    [4035225266123964416]
    """

    if isinstance(sids, pandas.Series):
        sids = sids.to_numpy()
        if sids.dtype == numpy.dtype('O'):
            # If we receive a series of SID collections we merge all sids into a single 1D array
            # to_numpy() would have produced an array of lists in this case
            sids = numpy.concatenate(sids)

    # Remove duplicate SIDs
    dissolved = numpy.unique(sids)

    if not dissolve_sids:
        return list(dissolved)

    if n_workers == 1 and n_chunks == 1:
        dissolved = dissolve_stare(dissolved)
    else:
        if n_workers > 1:
            chunks = numpy.array_split(dissolved, n_workers)
            with multiprocessing.Pool(processes=n_workers) as pool:
                dissolved = pool.map(dissolve_stare, chunks)
        elif n_chunks > 1:
            chunks = numpy.array_split(dissolved, n_chunks)
            dissolved = []
            for chunk in chunks:
                dissolved.append(dissolve_stare(chunk))
        dissolved = numpy.concatenate(dissolved)
        dissolved = numpy.unique(dissolved)
        dissolved = dissolve_stare(dissolved)

    return list(dissolved)


def series_intersects(series, other, method='skiplist', n_workers=1):
    """  Returns a bool series of length len(series).
    True for every row in which row intersects other.

    Parameters
    -----------
    series: pandas.Series (like)
        The series to evaluate intersects with other
    other: (Collection of) SID(s)
        The collection of SIDs to test intersections of the series with
    method: str
        either 'skiplist', 'binsearch', or 'nn'
    n_workers: int
        number of workers to use.

    Returns
    --------
    intersects: bool numpy.array
        Array of len(series).

    Examples
    ---------
    >>> import starepandas
    >>> import pandas
    >>> series = pandas.Series([[4035225266123964416],
    ...                         [4254212798004854789, 4255901647865118724]])
    >>> starepandas.series_intersects(series, 4035225266123964416)
    array([ True,  True])

    """

    # Make sure other is iterable
    other = numpy.array([other]).flatten()

    if n_workers > len(series):
        # Cannot have more partitions than rows        
        n_workers = len(series)

    if n_workers == 1:
        if series.dtype == numpy.int64:
            # If we have a series of sids; don't need to iterate. Can send the whole array to pystare/
            intersects = pystare.intersects(other, series, method)
        else:
            intersects = []
            for sids in series:
                if len(sids) > len(other):
                    # For method 1, larger item first is faster
                    intersects.append(pystare.intersects(sids, other, method).any())
                else:
                    intersects.append(pystare.intersects(other, sids, method).any())
            intersects = numpy.array(intersects)
    else:
        if n_workers > len(series):
            # Cannot have more partitions than rows        
            n_workers = len(series)
        ddf = dask.dataframe.from_pandas(series, npartitions=n_workers)
        meta = {'intersects': 'bool'}
        res = ddf.map_partitions(lambda df: series_intersects(df, other, method, 1), meta=meta)
        intersects = res.compute(scheduler='processes')
    return intersects

