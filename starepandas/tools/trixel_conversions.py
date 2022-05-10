import math

import dask.dataframe
import geopandas._vectorized as vectorized
import numpy
import pystare
import shapely
import geopandas
from shapely.geometry import Point

def to_vertices(sids, wrap_lon=True):
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
    wrap_lon: bool
        toggle if vertices should be wraped around antimeridian. If true:
        lon = ((lon + 180.0) % 360.0) - 180.0

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
    vs = pystare.to_vertices_latlon(sids)

    if wrap_lon:
        # If the trixel is not counterclockwise, then it is wrapped around the antimeridian
        # and we need to move the vertices over
        vs[1][vs[1]>180] = (vs[1][vs[1]>180] + 180) % 360.0 - 180
        vs[3][vs[3]>180] = (vs[3][vs[3]>180] + 180) % 360.0 - 180
    return vs


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
    >>> sids = numpy.array([18014398509481987])
    >>> vertices = starepandas.tools.trixel_conversions.to_vertices(sids)
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
    >>> sids = numpy.array([1729382256910270464])
    >>> vertices = starepandas.tools.trixel_conversions.to_vertices(sids)
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
    >>> sids = numpy.array([2882303761517117440])
    >>> vertices = starepandas.tools.to_vertices(sids)
    >>> points = starepandas.vertices2centerpoints(vertices)
    >>> print(points[0])
    POINT (-108.4349490979417 24.09484285959212)
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
    >>> sids = numpy.array([3458764513820540928])
    >>> vertices = starepandas.tools.trixel_conversions.to_vertices(sids)
    >>> starepandas.vertices2corners(vertices)
    array([[[-170.26439001,   29.9999996 ],
            [ -45.        ,   45.00000069],
            [  80.26439001,   29.9999996 ]]])

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
    >>> sids = numpy.array([3458764513820540928])
    >>> vertices = starepandas.tools.trixel_conversions.to_vertices(sids)
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
    corners = vertices2corners_ecef(vertices)
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
    >>> print(Point(19.50219017924583,23.29074702177385).distance(centerpoints[0]) < 1.0e-12)
    True
    """

    vertices = to_vertices(sids)
    centerpoints = vertices2centerpoints(vertices)
    return centerpoints


def to_corners(sids, wrap_lon=True):
    """ Converts a (collection of) sid(s) into (collection of) corners in lon/lat representation.

    Parameters
    ----------
    sids: int or collection of ints
        sids to covert to corners
    wrap_lon: bool
        toggle if vertices should be wraped around antimeridian. If true:
        lon = ((lon + 180.0) % 360.0) - 180.0

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

    vertices = to_vertices(sids, wrap_lon=wrap_lon)
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


def to_trixels(sids, as_multipolygon=False, wrap_lon=True):
    """
    Converts a (collection of) sid(s) into a (collection of) trixel(s)

    Parameters
    ------------
    sids: int64 or array-like
        (Collection of) STARE index value(s)
    as_multipolygon: bool
        If more than one sid is passed, toggle if the resulting trixels should be
        combined into a multipolygon. Otherwise a list of trixels is returned.
    wrap_lon: bool
        toggle if trixels should be wraped around antimeridian.

    Returns
    ---------
    trixels: Polygon or Multipolygon
        (collection of) trixel(s)

    Examples
    ---------
    >>> import starepandas
    >>> sids = [4611686018427387903, 4611686018427387903]
    >>> trixels = starepandas.to_trixels(sids, as_multipolygon=True)
    """
    if isinstance(sids, (numpy.int64, int)):
        # If single value was passed
        sids = [sids]
        as_multipolygon = False

    if isinstance(sids, numpy.ndarray):
        # This is not ideal, but when we read sidecars, we get unit64 and have to cast
        sids = sids.astype(numpy.int64)

    trixels = []
    vertices = to_corners(sids, wrap_lon=wrap_lon)
    for vertex in vertices:
        geom = shapely.geometry.Polygon(vertex)
        trixels.append(geom)

    if len(trixels) == 1 and not as_multipolygon:
        trixels = trixels[0]
    if as_multipolygon:
        trixels = shapely.geometry.MultiPolygon(trixels)
    return trixels


def trixels_from_stareseries(sids_series, n_workers=1, wrap_lon=True):
    """ Takes a series of STARE index values and creates an array of sets of trixels. If a row contains a set of sids
    (rather than a single sid); i.e. representing e.g. a region, a set of trixels will be generated and combined in a
    multipolygon

    Parameters
    -----------
    sids_series: array-like
        Series or array-like with STARE index values
    n_workers: int
        number of workers to use to lookup geometries in parallel
    wrap_lon: bool
            toggle if trixels should be wraped around antimeridian.

    Returns
    -----------
    trixel_series: geopandas.GeoSeries
        Series of trixels / triangle geometries

    Examples
    -------------
    >>> import starepandas
    >>> sids = [4611686018427387903, 4611686018427387903]
    >>> sdf = starepandas.STAREDataFrame(sids=sids)
    >>> trixels = starepandas.trixels_from_stareseries(sdf.sids)
    """
    npartitions = n_workers
    if len(sids_series) <= 1:
        npartitions = 1
    elif npartitions >= len(sids_series):
        # Cannot have more partitions than rows
        npartitions = len(sids_series) - 1

    if npartitions == 1:
        trixels_series = []
        for sids in sids_series:
            trixels = to_trixels(sids, as_multipolygon=True, wrap_lon=wrap_lon)
            trixels_series.append(trixels)
    else:
        ddf = dask.dataframe.from_pandas(sids_series, npartitions=npartitions)
        meta = {'trixels': 'object'}
        res = ddf.map_partitions(lambda df:
                                 vectorized.from_shapely(
                                     trixels_from_stareseries(df, n_workers=1,
                                                              wrap_lon=wrap_lon)).flatten(),
                                 meta=meta)
        trixels_series = res.compute(scheduler='processes')
        # Since the array would be ragged, we are probably safer with a list of arrays
        trixels_series = trixels_series.tolist()
    trixels_series = geopandas.GeoSeries(trixels_series, crs='EPSG:4326', index=sids_series.index)
    return trixels_series


def split_antimeridian_series(trixels_series, n_workers=1):
    npartitions = n_workers

    if len(trixels_series) <= 1:
        npartitions = 1
    elif npartitions >= len(trixels_series):
        # Cannot have more partitions than rows
        npartitions = len(trixels_series) - 1

    if npartitions == 1:
        split = []
        for row in trixels_series:
            if row.geom_type == 'Polygon':
                # We need to catch single Polygons
                row = [row]
            row = split_antimeridian(row)
            split.append(row)
        trixels_series = geopandas.GeoSeries(split,
                                             crs='EPSG:4326',
                                             index=trixels_series.index)
    else:
        ddf = dask.dataframe.from_pandas(trixels_series, npartitions=npartitions)
        meta = {'trixels': 'object'}
        res = ddf.map_partitions(lambda df:
                                 vectorized.from_shapely(split_antimeridian_series(df, n_workers=1)).flatten(),
                                 meta=meta)
        trixels_series = res.compute(scheduler='processes')
        # Since the array would be ragged, we are probably safer with a list of arrays
        trixels_series = trixels_series.tolist()
    trixels_series = geopandas.GeoSeries(trixels_series,
                                         crs='EPSG:4326',
                                         index=trixels_series.index)
    return trixels_series


def split_antimeridian(trixels):
    """Splits trixels at the antimeridian

    This works on trixels that cross the meridian and whose longitudes have *not* been wrapped around the
    antimeridian. I.e. when creating the trixels use sdf.make_trixels(wrap_lon=False)

    Parameters
    ------------
    trixels: A polygon, multipolygon, collection of polygons, or a geometry series
        A collection of trixels.
    """
    bbox = shapely.geometry.Polygon([(-180, -90), (180, -90), (180, 90), (-180, 90)])

    trixels = geopandas.GeoSeries(trixels, crs='EPSG:4326')

    exploded = trixels.explode(index_parts=True).reset_index(drop=True)

    for idx, trixel in exploded.iteritems():
        if not trixel.exterior.is_ccw:
            # If trixels are not CCW they have been constructed incorrectly
            # and we wrap their verices around the antimeridian
            x = (numpy.array(trixel.exterior.xy[0]) + 180) % 360.0 - 180
            y = numpy.array(trixel.exterior.xy[1])
            # TODO: this appears to break
            exploded[idx] = shapely.geometry.Polygon(zip(x, y))

    inside = exploded.intersection(bbox)
    inside[inside.geom_type != 'Polygon'] = shapely.wkt.loads('POLYGON EMPTY')
    inside = geopandas.tools.collect(inside, multi=True)

    outside = exploded.difference(bbox)
    outside[outside.geom_type != 'Polygon'] = shapely.wkt.loads('POLYGON EMPTY')
    outside = outside.apply(lambda x: shapely.affinity.translate(x, xoff=-360))
    outside = geopandas.tools.collect(outside)

    split = inside.union(outside)
    return split
