import dask.dataframe
import shapely
import pandas
import numpy
import pystare

# https://github.com/numpy/numpy/issues/14868
# import os
# os.environ["OMP_NUM_THREADS"] = "1"


def sids_from_gdf(gdf, level, convex=False, force_ccw=True, n_partitions=1, num_workers=None):
    """
    Takes a GeoDataFrame and returns a corresponding series of sets of trixel indices

    Parameters
    -----------
    gdf: geopandas.GeoDataFrame
        A dataframe containing features to look (sets of) STARE indices up for
    level: int
        STARE level
    convex: bool
        Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    force_ccw: bool
        Toggle if a counterclockwise orientation of the geometries should be enforced
    num_workers: int
        Number of workers used to lookup STARE indices in parallel

    Returns
    --------
    A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry

    Examples
    ---------
    # >>> import geopandas
    # >>> import starepandas
    # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    # >>> italy = world[world.name=='Italy']
    # >>> starepandas.sids_from_gdf(italy, level=3, convex=False, force_ccw=True, num_workers=1)
    # 141    [4269412446747230211, 4548635623644200963, 456...
    # Name: sids, dtype: object
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
        return sids_from_geoseries(gdf.geometry, level=level, convex=convex, force_ccw=force_ccw,
                                   n_partitions=n_partitions, num_workers=num_workers)


def sids_from_geoseries(series, level, convex=False, force_ccw=True, n_partitions=1, num_workers=None):
    """
    Takes a GeoSeries and returns a corresponding series of sets of trixel indices

    Parameters
    -----------
    series: geopandas.GeoSeries
        A geopandas.GeoSeries containing features to look (sets of) STARE indices up for
    level: int
        STARE level
    convex: bool
        Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
    force_ccw: bool
        Toggle if a counterclockwise orientation of the geometries should be enforced
    n_partitions: int
        Number of workers used to lookup STARE indices in parallel
    num_workers: int
        number of workers

    Returns
    --------
    sids
        A numpy array of length=len(gdf.index) holding the set of stare indices of each geometry

    Examples
    ------------
    # >>> import geopandas
    # >>> import starepandas
    # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    # >>> germany = world[world.name=='Germany']
    # >>> starepandas.sids_from_geoseries(germany.geometry, level=3, convex=True)
    # 121    [4251398048237748227, 4269412446747230211, 427...
    # Name: sids, dtype: object
    """

    if len(series) <= 1:
        n_partitions = 1
    elif n_partitions >= len(series) / 2:
        # Cannot have more partitions than rows
        n_partitions = int(len(series) / 2)

    if n_partitions == 1:
        sids = series.apply(sids_from_shapely, level=level, convex=convex, force_ccw=force_ccw)
        sids.name = 'sids'
        return sids
    else:
        ddf = dask.dataframe.from_pandas(series, npartitions=n_partitions)
        meta = {'name': 'int64'}
        res = ddf.map_partitions(sids_from_geoseries, level=level, convex=convex,
                                 force_ccw=force_ccw, n_partitions=1, num_workers=1, meta=meta)
        sids = res.compute(scheduler='processes', num_workers=num_workers)
        sids.name = 'sids'
    return sids


def sids_from_xy(lon, lat, level):
    """Takes a list/array of lon and lat and returns a (set of) STARE index/ices

    Parameters
    -----------
    lon: numerical/float
        Longitude of point
    lat: numerical/float
        Latitude of point
    level: int
        STARE spatial level

    Returns
    ----------
    sids
        Array of STARE index values

    Examples
    ----------
    # >>> import starepandas
    # >>> x = [10.1, 20.9]
    # >>> y = [55.3, 60.1]
    # >>> starepandas.sids_from_xy(x, y, level=15)
    # array([4254264869405326191, 3640541580264132591])
    """
    return pystare.from_latlon(lat, lon, level)


def sids_from_latlon_row(row, level):
    """ Dask helper function """
    return pystare.from_latlon(row.lat, row.lon, level)


def sids_from_xy_df(df, level, n_partitions=1, num_workers=None):
    """ Takes a dataframe and generates an array of STARE index values.
    Assumes latitude column name is {'lat', 'Latitude', 'latitude', or 'y'} and
    longitude column name is {'lon', 'Longitude', 'longitude', or 'x'}

    Parameters
    --------------
    df: pandas.DataFrame
        Dataframe containing x/y coordinates
    level: int
        STARE spatial level
    n_partitions: int
        Number of workers used to lookup STARE indices in parallel

    Returns
    --------
    sids
        Array of STARE index values

    Examples
    ------------
    # >>> import starepandas
    # >>> import pandas
    # >>> x = [-119.42, 7.51]
    # >>> y = [34.25, 47.59]
    # >>> df = pandas.DataFrame({'lat': y, 'lon': x})
    # >>> starepandas.sids_from_xy_df(df, level=20)
    # array([3331752989521980116, 4271829667422230484])
    """
    rename_dict = {'Latitude': 'lat', 'latitude': 'lat', 'y': 'lat',
                   'Longitude': 'lon', 'longitude': 'lon', 'x': 'lon'}
    df = df.rename(columns=rename_dict)
    if n_partitions > 1:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_partitions)
        res = ddf.map_partitions(sids_from_latlon_row, level=level, meta=('sids', 'int64'))
        sids = res.compute(scheduler='processes', num_workers=num_workers)
        return sids
    else:
        return pystare.from_latlon(df.lat, df.lon, level)


def sids_from_shapely(geom, level, convex=False, force_ccw=False):
    """ Wrapper for starepandas.from_point(), starepandas.from_ring(),
    starepandas.from_polygon(), starepandas.from_multipolygon()

    Takes a shapely Point, Polygon, or Multipolygon and looks up the STARE representation

    Parameters
    -------------
    geom: shapely.geometry.Point, shapely.geometry.Polygon, shapely.geometry.MultiPolygon
        A shapely geometry to look the sids up for
    level: int
        Maximum STARE level to use for lookup
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
    # >>> import shapely
    # >>> import starepandas
    #
    # Point:
    #
    # >>> point = shapely.geometry.Point(10.5, 20)
    # >>> starepandas.sids_from_shapely(point, level=27)
    # 4598246232954051067
    #
    # Polygon:
    #
    # >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    # >>> starepandas.sids_from_shapely(polygon1, force_ccw=True, level=6)
    # array([4430697608402436102, 4430838345890791430, 4430979083379146758])
    #
    # Multipolygon:
    #
    # >>> polygon2 = shapely.geometry.Polygon([(5, 5), (6, 6), (6, 5)])
    # >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    # >>> starepandas.sids_from_shapely(multipolygon, force_ccw=True, level=5)
    # array([4430416133425725445, 4430979083379146757, 4416905334543613957])

    """
    if geom.geom_type == 'Point':
        return sid_from_point(geom, level=level)
    if geom.geom_type == 'Polygon':
        return sids_from_polygon(geom, level=level, convex=convex, force_ccw=force_ccw)
    if geom.geom_type == 'MultiPolygon':
        return sids_from_multipolygon(geom, level=level, convex=convex, force_ccw=force_ccw)


def sid_from_point(point, level):
    """Takes a shapely Point, Polygon, or Multipolygon and returns the according SID

    Parameters
    -------------
    point: shapely.geometry.Point
        Shapely point
    level: int
        STARE level to use for lookup

    Returns
    ----------
    sid
        spatial index value

    Examples
    ---------
    # >>> import starepandas
    # >>> import shapely
    # >>> point = shapely.geometry.Point(10.5, 20)
    # >>> starepandas.sid_from_point(point, level=20)
    # 4598246232954051060
    """
    lat = point.y
    lon = point.x
    index_value = pystare.from_latlon([lat], [lon], level)[0]
    return index_value


def sids_from_ring(ring, level, convex=False, force_ccw=False):
    """
    Return a range of indices covering the region inside/outside ring.
    Node orientation is relevant!

    Parameters
    ------------
    ring: shapely.geometry.polygon.LinearRing
        Ring to lookup sids for
    level: int
        Maximum STARE level to use for lookup.
    convex: bool
        Toggle if the STARE lookup should be performed on the convex hull rather than the actual geometry of the ring
    force_ccw: bool
        toggle if orientation of ring should be overwritten and ring should be interpreted as outerboundary.

    Returns
    ---------
    collection of sids

    Examples
    ---------
    # >>> import starepandas
    # >>> import shapely
    # >>> # Note: the ring is clockwise!
    # >>> polygon = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    # >>> starepandas.sids_from_ring(polygon.exterior, force_ccw=True, level=6)
    # array([4430697608402436102, 4430838345890791430, 4430979083379146758])
    """
    #if force_ccw and not ring.is_ccw:
    if force_ccw and not ring_is_ccw(ring):
        ring = shapely.geometry.LinearRing(ring.coords[::-1])
    latlon = ring.coords.xy
    lon = latlon[0]
    lat = latlon[1]
    if convex:
        range_indices = pystare.cover_from_hull(lat, lon, level)
    else:
        range_indices = pystare.cover_from_ring(lat, lon, level)

    return range_indices


def sids_from_polygon(polygon, level, convex=False, force_ccw=False):
    """ Lookup STARE index values for a polygon.
    A Polygon is a planar Surface defined by 1 exterior ring and 0 or more interior boundaries. Each interior
    ring defines a hole in the Polygon.

    Parameters
    --------------
    polygon: shapely.geometry.Polygon
        Polygon to look sids up for
    level: int
        Maximum STARE level to use for lookup
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
    # >>> import starepandas
    # >>> import shapely
    # >>> polygon = shapely.geometry.Polygon([(0, 0), (2, 0), (1, 1)])
    # >>> starepandas.sids_from_polygon(polygon, level=5)
    # array([4423097784031248389, 4430416133425725445, 4430979083379146757])
    """

    if force_ccw:
        polygon = shapely.geometry.polygon.orient(polygon)
    sids_ext = sids_from_ring(polygon.exterior, level, convex, force_ccw)

    if len(polygon.interiors) > 0:
        sids_int = []
        for interior in polygon.interiors:
            # Interior polygons should be clockwise, thereofore we don't force ccw
            sids_int.append(sids_from_ring(interior, level, convex, force_ccw=False))
        sids_int = numpy.concatenate(sids_int)
        sids = pystare.intersection(sids_int, sids_ext)
    else:
        sids = sids_ext
    return sids


def sids_from_multipolygon(multipolygon, level, convex=False, force_ccw=False):
    """ Lookup STARE index values for a multipolygon.

    Parameters
    ------------
    multipolygon: shapely.geometry.MultiPolygon
        MultiPolygon to look sids up for
    level: int
        Maximum STARE level to use for lookup
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
    #
    # >>> import starepandas
    # >>> import shapely
    # >>> polygon1 = shapely.geometry.Polygon([(0, 0), (1, 1), (1, 0)])
    # >>> polygon2 = shapely.geometry.Polygon([(3, 1), (4, 2), (2, 1)])
    # >>> multipolygon = shapely.geometry.MultiPolygon([polygon1, polygon2])
    # >>> starepandas.sids_from_multipolygon(multipolygon, force_ccw=True, level=3)
    # array([4422534834077827075, 4413527634823086083, 4422534834077827075])
    """
    range_indices = []
    for polygon in multipolygon.geoms:
        range_indices.append(sids_from_polygon(polygon, level, convex, force_ccw))
    range_indices = numpy.concatenate(range_indices)
    return range_indices


def compress_sids(sids):
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
    # >>> import starepandas
    # >>> # The two latter SIDs are contained in the first SID
    # >>> sids = [4035225266123964416, 4254212798004854789, 4255901647865118724]
    # >>> starepandas.compress_sids(sids)
    # array([4035225266123964416])

    Notes
    --------
    .. image:: ../../../_static/dissolve.png

    """
    sids = numpy.unique(sids)
    s_range = pystare.to_compressed_range(sids)
    expanded = pystare.expand_intervals(s_range, -1, multi_resolution=True)
    return expanded


def series_intersects(series, other, method='binsearch', n_partitions=1, num_workers=None):
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
    n_partitions: int
        number of partitions
    num_workers: int
        number of workers
    Returns
    --------
    intersects: bool numpy.array
        Array of len(series).

    Examples
    ---------
    # >>> import starepandas
    # >>> import pandas
    # >>> series = pandas.Series([[4035225266123964416],
    # ...                         [4254212798004854789, 4255901647865118724]])
    # >>> starepandas.series_intersects(series, 4035225266123964416)
    # array([ True,  True])

    """

    # Make sure other is iterable
    other = numpy.array([other]).flatten()

    if len(series) <= 1:
        n_partitions = 1
    elif n_partitions >= len(series):
        # Cannot have more partitions than rows
        n_partitions = len(series) - 1

    if n_partitions == 1:
        if series.dtype in [numpy.dtype('uint64'), numpy.dtype('int64'), pandas.UInt64Dtype(), pandas.Int64Dtype()]:
            # If we have a series of sids; don't need to iterate. Can send the whole array to pystare/
            if pandas.isna(series).sum() > 0:
                raise Exception('NaN values in the sids. Use e.g. ```sdf.dropna(subset=["sids"], inplace=True)```')
            intersects = pystare.intersects(other, series, method)
        else:
            intersects = []
            for sids in series:
                if isinstance(sids, str):
                    sids = numpy.array(sids.strip('[]').split(), dtype=numpy.int64)
                if len(sids) > len(other):
                    # For method 1, larger item first is faster
                    intersects.append(pystare.intersects(sids, other, method).any())
                else:
                    intersects.append(pystare.intersects(other, sids, method).any())
            intersects = numpy.array(intersects, dtype='bool')
    else:
        ddf = dask.dataframe.from_pandas(series, npartitions=n_partitions)
        meta = {'intersects': 'bool'}
        res = ddf.map_partitions(lambda df: numpy.array(series_intersects(df, other, method, 1)), meta=meta)
        intersects = res.compute(scheduler='processes', num_workers=num_workers)
    return intersects


def make_circular_sids(df, level, diameter, n_partitions=1, num_workers=None):
    """Create Circular sids cover

    Parameters
    -----------
    df: pandas.DataFrame
        the dataframe
    level: int
        the max stare level for the cover
    diameter: float
        circle diameter in degrees; may be approximated from a metric distance d and the earth radius r with:
        phi = d /2/pi/r*360
    n_partitions: int
        number of dask partitions to use
    num_workers: int
        number of dask workers to use
    """

    if num_workers is not None and n_partitions is None:
        n_partitions = num_workers * 10
    elif num_workers is None and n_partitions is None:
        n_partitions = 1
        num_workers = 1

    if len(df) <= 1:
        n_partitions = 1
    elif n_partitions >= len(df):
        # Cannot have more partitions than rows
        n_partitions = len(df) - 1

    if n_partitions == 1:
        sids = []
        for idx, row in df.iterrows():
            sid = row.sids
            sid = pystare.spatial_coerce_resolution(sid, level)
            sid = pystare.spatial_clear_to_resolution(numpy.array(sid))
            circle_sids = pystare.sid2circular_cover(sid, diameter, level)
            sids.append(circle_sids)
        sids = numpy.array(sids, dtype=object)
    else:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_partitions)
        meta = {'sids': 'int64'}
        res = ddf.map_partitions(make_circular_sids, level=level, diameter=diameter,
                                 n_partitions=1, num_workers=1, meta=meta)
        sids = res.compute(scheduler='processes', num_workers=num_workers)
        sids = list(sids)
    return sids


def speedy_subset(df, right_sids):
    """ Speedy intersects is meant to subset large (long) STAREDataFrame to a subset that intersects the roi.

    This method works particularly well if
     a) the df has significantly more SIDs than the roi_sids
     b) The SIDs of the df are at higher level than the roi_sids

    Parameters
    -----------
    df: starepandas.STAREDataFrame
        the dataframe that is to be subset
    right_sids: array-like
        a set of SIDs describing the roi to whch the df is to be subset
    """
    right_sids = numpy.array(right_sids)

    left_sids = df[df._sid_column_name]

    # Dropping values outside of range
    top_bound = pystare.spatial_clear_to_resolution(right_sids.max())
    level = pystare.spatial_resolution(top_bound)
    top_bound += pystare.spatial_increment_from_level(level)
    bottom_bound = right_sids.min()
    candidate_sids = left_sids[(left_sids >= bottom_bound) * (left_sids <= top_bound)]
    if len(candidate_sids) == 0:
        # return empty df
        return df[df.sids == 0]
    candidate_sids = candidate_sids.astype('int64')

    # finding the intersection level
    left_min_level = pystare.spatial_resolution(candidate_sids).max()
    right_min_level = pystare.spatial_resolution(right_sids).max()
    intersecting_level = min(right_min_level, left_min_level)

    # Clearing to intersecting level, allowing us to group them / extract only the distinct values
    coerced_sids = pystare.spatial_coerce_resolution(candidate_sids, intersecting_level)
    cleared_sids = pystare.spatial_clear_to_resolution(coerced_sids)
    distinct_sids = numpy.unique(cleared_sids)

    # Now doing the intersection on the distinct values
    intersects = distinct_sids[pystare.intersects(right_sids, distinct_sids)]
    intersecting = candidate_sids[cleared_sids.isin(intersects)]

    return df.iloc[intersecting.index]


def latlon_to_xyz(latitude, longitude, altitude=0, earth_radius=1):
    # Convert degrees to radians
    lat_rad = numpy.radians(latitude)
    lon_rad = numpy.radians(longitude)

    # Convert spherical coordinates to Cartesian coordinates
    x = (earth_radius + altitude) * numpy.cos(lat_rad) * numpy.cos(lon_rad)
    y = (earth_radius + altitude) * numpy.cos(lat_rad) * numpy.sin(lon_rad)
    z = (earth_radius + altitude) * numpy.sin(lat_rad)

    return x, y, z

def xyz_to_latlon(x, y, z):
    # Assuming XYZ coordinates are in a Cartesian coordinate system
    radius = numpy.sqrt(x**2 + y**2 + z**2)

    # Calculate longitude
    lon = numpy.arctan2(y, x)

    # Calculate latitude
    lat = numpy.arcsin(z / radius)

    # Convert radians to degrees
    lon = numpy.degrees(lon)
    lat = numpy.degrees(lat)

    return lat, lon

def ring_is_ccw(ring):
    lons = ring.xy[0]
    lats = ring.xy[1]
    xs, ys, zs = latlon_to_xyz(lats, lons)
    vertices = numpy.array(list(zip(xs, ys, zs)))
    return is_ccw(vertices)


def project_spherical_polygon(vertices):
    # Calculate centroid of the spherical polygon
    if not numpy.all(vertices[0] == vertices[-1]):
        vertices = numpy.vstack((vertices, vertices[0]))
    centroid = numpy.mean(vertices, axis=0)

    # Compute the normal vector (centroid vector)
    normal_vector = centroid / numpy.linalg.norm(centroid)

    # Project vertices onto the plane perpendicular to the centroid vector
    projected_vertices = vertices - numpy.outer(vertices.dot(normal_vector), normal_vector)

    x_axis = normal_vector - numpy.array([1, 0, 0]) * normal_vector.dot([1, 0, 0])
    x_axis /= numpy.linalg.norm(x_axis)
    y_axis = numpy.cross(normal_vector, x_axis)
    y_axis = y_axis / numpy.linalg.norm(y_axis)

    transformed_coordinates = numpy.dot(projected_vertices, numpy.array([x_axis, y_axis]).T)

    return transformed_coordinates


def signed_area(vertices):
    return 0.5 * numpy.sum(numpy.cross(numpy.roll(vertices, 1, axis=0), vertices))


def is_ccw(vertices):
    projected = project_spherical_polygon(vertices)
    area = signed_area(projected)
    if area > 0.0:
        return True
    else:
        return False
