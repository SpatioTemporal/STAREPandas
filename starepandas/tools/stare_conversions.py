import dask.dataframe
import shapely
import math
import pandas

# https://github.com/numpy/numpy/issues/14868
import os

os.environ["OMP_NUM_THREADS"] = "1"

import numpy
import pystare
import multiprocessing
import geopandas


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

    Examples
    ---------
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

    Examples
    ------------
    >>> import geopandas
    >>> import starepandas
    >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    >>> germany = world[world.name=='Germany']
    >>> starepandas.stare_from_geoseries(germany.geometry, level=3, convex=True)
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

    Examples
    ----------
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

    Examples
    ------------
    >>> import starepandas
    >>> import pandas
    >>> x = [-119.42, 7.51]
    >>> y = [34.25, 47.59]
    >>> df = pandas.DataFrame({'lat': y, 'lon': x})
    >>> starepandas.stare_from_xy_df(df, level=20)
    array([3331752989521980116, 4271829667422230484])
    """
    rename_dict = {'Latitude' : 'lat', 'latitude': 'lat', 'y': 'lat',
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

    Examples
    -------------
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
        res = ddf.map_partitions(lambda df: numpy.array(trixels_from_stareseries(df, 1)).flatten(), meta=meta)
        trixels_series = res.compute(scheduler='processes')
    return trixels_series


def to_vertices(sids):
    """ Converts a (collection of) sid(s) into vertices. Vertices are a tuple of:

    1. the latitudes of the corners
    2. the longitudes of the corners
    3. the latitudes of the centers
    4. the longitudes of the centers

    The longitudes and latitudes of the corners are serialized i.e. they are 3 times the length of the length of sids

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to vertices

    Returns
    ---------
    vertices: (list of) tuple(s)
        (Collection of) trixel corner vertices; Three corner points per trixel.

    Examples
    -----------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([3])
    >>> starepandas.to_vertices(sids)
    (array([-29.9999996 , -38.92792794, -23.13179401]),
     array([ 9.73560999, 18.06057651, 19.71049975]),
     array([-30.75902492]),
     array([15.84277554]))
    """

    return pystare.to_vertices_latlon(sids)


def vertices2centers(vertices):
    """ Converts trixel vertices datastructure (c.f. :func:`~to_vertices`)
    into trixel centers in lon/lat representation.

    Parameters
    -----------
    vertices: tuple
        vertices data structure

    Returns
    ------------
    corners: numpy.array
        First dimension are sids, second dimension longitude/latitude

    Examples
    --------------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([18014398509481987])
    >>> vertices = starepandas.to_vertices(sids)
    >>> starepandas.vertices2centers(vertices)
    array([[ 25.66446757, -23.46672972]])
    """

    lat_center = vertices[2]
    lon_center = vertices[3]
    centers = numpy.array([lon_center, lat_center]).transpose()
    return centers


def vertices2centers_ecef(vertices):
    """ Converts trixel vertices datastructure (c.f. :func:`~to_vertices`)
    into trixel centers in ECEF representation.

    Parameters
    -----------
    vertices: tuple
        vertices data structure

    Returns
    -----------
    centers: numpy.array
         First dimension are sids, second dimension x/y/z

    Examples
    ----------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([1729382256910270464])
    >>> vertices = starepandas.to_vertices(sids)
    >>> starepandas.vertices2centers_ecef(vertices)
    array([[ 0.11957316, -0.11957316, -0.98559856]])
    """

    lat = vertices[2]
    lon = vertices[3]
    x = numpy.cos(lon / 360 * math.pi * 2) * numpy.cos(lat / 360 * math.pi * 2)
    y = numpy.sin(lon / 360 * math.pi * 2) * numpy.cos(lat / 360 * math.pi * 2)
    z = numpy.sin(lat / 360 * math.pi * 2)
    centers = numpy.array([x, y, z]).transpose()
    return centers


def vertices2centerpoints(vertices):
    """ Converts trixel vertices datastructure (c.f. :func:`~to_vertices`)
    into trixel centers in shapley point representation.

    Parameters
    -----------
    vertices: tuple
        vertices data structure

    Returns
    ---------
    centerpoints: list
        A (collection of) trixel center shapely point(s)

    Examples
    ----------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([2882303761517117440])
    >>> vertices = starepandas.to_vertices(sids)
    >>> points = starepandas.vertices2centerpoints(vertices)
    >>> print(points[0])
    POINT (251.5650509020583 24.09484285959212)
    """

    lat_center = vertices[2]
    lon_center = vertices[3]
    return geopandas.points_from_xy(lon_center, lat_center)


def vertices2corners(vertices):
    """ Converts trixel vertices datastructure (c.f. :func:`~to_vertices`) into
    trixel corners in lon/lat representation.

    Parameters
    -----------
    vertices: tuple
        vertices data structure

    Returns
    ------------
    corners: numpy.array
        First dimension are the sids
        Second dimension the corers
        Third dimension longitude/latitude

    Examples
    --------------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([3458764513820540928])
    >>> vertices = starepandas.to_vertices(sids)
    >>> starepandas.vertices2corners(vertices)
    array([[[189.73560999,  29.9999996 ],
            [315.        ,  45.00000069],
            [ 80.26439001,  29.9999996 ]]])
    """

    lats = vertices[0]
    lons = vertices[1]

    corners = []
    i = 0
    while i < len(lons):
        corner = ((lons[i], lats[i]), (lons[i + 1], lats[i + 1]), (lons[i + 2], lats[i + 2]))
        corners.append(corner)
        i += 3
    corners = numpy.array(corners)
    return corners


def corners2ecef(corners):
    """ Converts corners lon/lat array (as returned by  :func:`~vertices2corners`) to ECEF representation.

    Parameters
    -----------
    corners: array
        corners lon/lat array

    Returns
    --------
    corners: numpy.array
        First dimension are the sids (same as input first dimension).
        Second dimension the corners (1,2,3).
        Third dimension x,y,z.
    """

    corners = numpy.array(corners)
    lon = corners[:, :, 0]
    lat = corners[:, :, 1]
    length = lat.shape[0]

    x = numpy.cos(lon / 360 * math.pi * 2) * numpy.cos(lat / 360 * math.pi * 2)
    y = numpy.sin(lon / 360 * math.pi * 2) * numpy.cos(lat / 360 * math.pi * 2)
    z = numpy.sin(lat / 360 * math.pi * 2)

    corners_ecef = numpy.array([x, y, z]).transpose()
    corners_ecef = corners_ecef.reshape(length, 3, 3)
    return corners_ecef


def vertices2corners_ecef(vertices):
    """ Converts trixel vertices datastructure (c.f. :func:`~to_vertices`) to trixel corners in ECEF representation.

    Parameters
    ----------
    vertices: tuple
        vertices data structure

    Returns
    -------
    corners: numpy.array
        First dimension are the sids (same as input first dimension).
        Second dimension the corners (1,2,3).
        Third dimension x,y,z.

    Examples
    ----------
    >>> import starepandas
    >>> import numpy
    >>> sids = numpy.array([3458764513820540928])
    >>> vertices = starepandas.to_vertices(sids)
    >>> starepandas.vertices2corners_ecef(vertices)
    array([[[-0.85355339, -0.14644661,  0.49999999],
            [ 0.49999999, -0.49999999,  0.70710679],
            [ 0.14644661,  0.85355339,  0.49999999]]])
    """

    corners = vertices2corners(vertices)
    corners_ecef = corners2ecef(corners)
    return corners_ecef


def vertices2gring(vertices):
    """" Converts trixel vertices datastructure (c.f. :func:`~to_vertices`) to
    the 3 great circles constraining the trixel(s).

    Parameters
    -----------
    vertices: tuple
        vertices data structure

    Returns
    ---------
    grings: numpy.array
        The great circles constraining the trixels given by their ECEF norm vectors.
    """
    corners = vertices2corners(vertices)
    return corners2gring(corners)


def to_centers(sids):
    """ Converts a (collection of) sid(s) into a (collection of) trixel center longitude, latitude pairs.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to vertices

    Returns
    --------
    Centers: (list of) tuple(s)
        List of centers. A center is a pair of longitude/latitude.

    Examples
    ----------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> starepandas.to_centers(sids)
    array([[19.50219018, 23.29074702],
           [18.65957821, 25.34384175]])
    """
    vertices = to_vertices(sids)
    centers = vertices2centers(vertices)
    return centers


def to_centers_ecef(sids):
    """ Converts a (collection of) sid(s) into a (collection of) trixel centers in ECEF represenation.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to vertices

    Returns
    --------
    Centers: (list of) tuple(s)
        List of centers. A center is a pair of longitude/latitude.

    Examples
    ----------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> starepandas.to_centers_ecef(sids)
    array([[0.86581415, 0.30663812, 0.39539717],
           [0.8562505 , 0.28915168, 0.42804953]])

    """
    vertices = to_vertices(sids)
    centers = vertices2centers_ecef(vertices)
    return centers


def to_centerpoints(sids):
    """ Converts a (collection of) sid(s) into a (collection of) trixel center shapely points.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to vertices

    Returns
    --------
    Centers: (list of) tuple(s)
        List of centers. A center is a pair of longitude/latitude.

    Examples
    ----------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> centerpoints = starepandas.to_centerpoints(sids)
    >>> print(centerpoints[0])
    POINT (19.50219017924583 23.29074702177385)
    """

    vertices = to_vertices(sids)
    centerpoints = vertices2centerpoints(vertices)
    return centerpoints


def to_corners(sids):
    """ Converts a (collection of) sid(s) into (collection of) corners in lon/lat representation.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to corners

    Returns
    ---------
    corners: numpy.array
        (Collection of) trixel corners in lon/lat representations; Three corner points per trixel.
        First dimension are the SIDs, second dimension the corners (1 through 3), third dimensio lon/lat.

    Examples
    -----------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> starepandas.to_corners(sids)
    array([[[20.55604548, 22.47991609],
            [19.73607532, 24.53819039],
            [18.21460548, 22.84521749]],
            [[18.88878121, 26.59188366],
            [17.35402492, 24.89163021],
            [19.73607532, 24.53819039]]])
    """

    vertices = to_vertices(sids)
    corners = vertices2corners(vertices)
    return corners


def to_corners_ecef(sids):
    """ Converts a (collection of) sid(s) into (collection of) trixel corners in ECEF representation.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to corners

    Returns
    ---------
    corners: numpy.array
        (Collection of) trixel corners in ECEF representations; Three corner points per trixel.
        First dimension are the SIDs, second dimension the corners (1 through 3), third dimensio x/y/z.

    Examples
    -----------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> starepandas.to_corners_ecef(sids)
    array([[[0.86518091, 0.32444285, 0.38235956],
            [0.84606293, 0.28948702, 0.44763242],
            [0.85624806, 0.30718957, 0.41529968]],
           [[0.86581405, 0.27056689, 0.42090331],
            [0.87538003, 0.2880576 , 0.38824299],
            [0.85624806, 0.30718957, 0.41529968]]])
    """

    vertices = to_vertices(sids)
    corners_ecef = vertices2corners_ecef(vertices)
    return corners_ecef


def to_gring(sids):
    """ Converts a (collection of) sid(s) to (a collection of) the 3 great circles constraining the trixel(s).

    Parameters
    -----------
    sids: int or collection of ints
        sids to covert to great circles

    Returns
    ----------
    great_circles: numpy.array
        Great circles constraining the trixels. Three great circles per trixel defined by their ECEF norm vector.
        First dimensions are SIDs, second dimension are the 3 great circle (1 through 3), third dimension are x/y/z.

    Examples
    ---------
    >>> import starepandas
    >>> sids = [4611263805962321926, 4611404543450677254]
    >>> starepandas.to_gring(sids)
    array([[[ 0.01728414, -0.03191472, -0.01202901],
            [ 0.00073153,  0.03145575, -0.02172531],
            [-0.00036603,  0.03111274, -0.02225885]],
           [[-0.01693076,  0.00082534,  0.03429666],
            [-0.01582108,  0.0011905 ,  0.03478885],
            [ 0.01728414, -0.03191472, -0.01202901]]])
    """

    corners = to_corners_ecef(sids)
    return corners2gring(corners)


def corners2gring(corners):
    corners = numpy.array(corners)
    length = corners.shape[0]
    corners = corners.reshape(3, length, 3)
    gc1 = numpy.cross(corners[0], corners[1])
    gc2 = numpy.cross(corners[1], corners[2])
    gc3 = numpy.cross(corners[2], corners[0])
    gcs = numpy.array([gc1, gc2, gc3]).reshape(length, 3, 3)
    return gcs


def to_trixels(sids, as_multipolygon=False):
    """ Converts a (collection of) sid(s) into a (collection of) trixel(s)

    :param sids: (Collection of) STARE index value(s)
    :type sids: int64 or array-like
    :param as_multipolygon: If more than one sid is passed, toggle if the resulting trixels should be combined into a
        multipolygon
    :type as_multipolygon: bool
    :return: array like of trixels
    :rtype: array-like

    Examples
    ---------
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

    trixels = []
    vertices = to_corners(sids)
    for vertex in vertices:
        geom = shapely.geometry.Polygon(vertex)
        trixels.append(geom)

    if len(trixels) == 1:
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

    Examples
    ---------
    >>> import shapely
    >>> import starepandas

    Point:

    >>> point = shapely.geometry.Point(10.5, 20)
    >>> starepandas.from_shapely(point)
    4611686018427387903

    Polygon:

    >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    >>> starepandas.from_shapely(polygon1, force_ccw=True, level=6)
    array([4430697608402436102, 4430838345890791430, 4430979083379146758])

    Multipolygon:

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

    Examples
    ---------
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
    """
    Return a range of indices covering the region inside/outside ring.
    Node orientation is relevant!

    Parameters
    ------------
    ring: shapely.geometry.polygon.LinearRing
        Ring to lookup sids for
    level: int
        (Maximum) STARE level to use for lookup. Warning: default is maximum level (27)
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

    Examples
    ---------
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

    Examples
    ----------

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


def dissolve_stare(sids):
    """ Dissolve STARE index values.
    Combine/dissolve sibiling sids into the parent sids. That is:
        1. Any 4 siblings with the same parent in the collection get replaced by the parent. And
        2. Any child whose parents is in the collection will be removed

    Parameters
    -----------
    sids: array-like
        A collection of SIDs to dissolve

    Returns
    --------
    dissolved: numpy.array
        Dissolved SIDs

    See Also
    ---------
    merge_stare

    Examples
    --------
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
        # If we receive a series of SID collections we merge all sids into a single 1D array
        sids = numpy.concatenate(sids.array)

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
    method = {'skiplist': 0, 'binsearch': 1, 'nn': 2}[method]

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


def int2hex(sids):
    """ Converts int sids to hex sids

    Parameters
    -----------
    sids: array-like or int64
        int representations of SIDs

    Returns
    --------
    sid: array-like or str
        hex representations of SIDs

    Examples
    -----------
    >>> import starepandas
    >>> sid = 3458764513820540928
    >>> starepandas.int2hex(sid)
    '0x3000000000000000'
    """

    if hasattr(sids, "__len__"):
        return ["0x%016x" % sid for sid in sids]
    else:
        return "0x%016x" % sids


def hex2int(sids):
    """ Converts hex SIDs to int SIDs

    Parameters
    -----------
    sids: array-like or str
        hex representations of SIDs

    Returns
    ----------
    sid: array-like or int64
        int representation of SIDs


    Examples
    -----------
    >>> import starepandas
    >>> sid = '0x3000000000000000'
    >>> starepandas.hex2int(sid)
    3458764513820540928
    """

    if isinstance(sids, str):
        return int(sids, 16)
    else:
        return [int(sid, 16) for sid in sids]
