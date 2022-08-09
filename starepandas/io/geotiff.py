import starepandas
import rasterio
import numpy
import geopandas
import pyproj
import pystare
import dask
import xarray


def flatten_values(values):
    for key in values:
        values[key] = values[key].flatten()
    return values


def read_bands(file_path, bands=None):
    values = {}
    with rasterio.open(file_path) as src:
        if bands is None:
            bands = range(1, src.count + 1)
        for band in bands:
            values[f'band_{band}'] = src.read(band)
    return values


def read_meta(file_path):
    meta = {}
    with rasterio.open(file_path) as src:
        meta['crs'] = src.crs
        meta['trans'] = src.transform
        meta['height'] = src.height
        meta['width'] = src.width
    return meta


def read_geotiff(file_path, bands=None, add_sids=True,
                 add_pts=False, add_xy=False, add_coordinates=False, add_latlon=False,
                 add_trixels=False, n_workers=1):
    """
    Reads a geotiff into a STAREDataFrame

    Parameters
    -----------
    file_path: str
        location of geotiff
    bands: list
        only read the specified bands. Default: Read all bands
    add_sids: bool
        toggle adding spatial indices
    add_pts: bool
        toggle adding shapely points to the dataframe
    add_xy: bool
        toggle adding the x/y array coordinates
    add_coordinates: bool
        toggle adding native coordinates
    add_latlon: bool
        toggle adding the WGS84 latitudes and longitudes
    add_trixels: bool
        toggle adding trixels
    n_workers: int
        number of workers to use to make trixels (default: 1)

    Returns
    --------
    sdf: STAREDataFrame
        the dataframe

    """
    meta = read_meta(file_path)
    trans = meta['trans']
    trans = numpy.array([trans.a, trans.b, trans.c, trans.d, trans.e, trans.f], dtype='float32')

    cols, rows = numpy.meshgrid(numpy.arange(meta['width'], dtype='float32'),
                                numpy.arange(meta['height'], dtype='float32'), copy=False)

    xs = cols * trans[0] + rows * trans[1] + trans[2] + trans[0] / 2
    ys = cols * trans[3] + rows * trans[4] + trans[5] + trans[4] / 2

    epsg_4326 = 'EPSG:4326'
    transformer = pyproj.Transformer.from_crs(meta['crs'], epsg_4326)
    lats, lons = transformer.transform(xs, ys)

    values = read_bands(file_path, bands)
    values = flatten_values(values)
    sdf = starepandas.STAREDataFrame(values)

    if add_latlon:
        lats = lats
        sdf['lon'] = lons.astype('float32').flatten()
        sdf['lat'] = lats.astype('float32').flatten()
    if add_xy:
        sdf['ix'] = cols.flatten()
        sdf['iy'] = rows.flatten()
    if add_coordinates:
        sdf['x'] = xs.astype('float32').flatten()
        sdf['y'] = ys.astype('float32').flatten()
    if add_pts:
        pts = geopandas.points_from_xy(lons.flatten(), lats.flatten(), crs=epsg_4326)
        sdf.set_geometry(pts, inplace=True)
        sdf.set_crs(epsg_4326, inplace=True)
    if add_sids:
        sids = pystare.from_latlon_2d(lats, lons, adapt_level=True)
        sdf.set_sids(sids.flatten(), inplace=True)
        if add_trixels:
            trixels = sdf.make_trixels()
            sdf.set_trixels(trixels, inplace=True)
            sdf.set_crs(epsg_4326, inplace=True)

    sdf.dropna(inplace=True)
    sdf.reset_index(inplace=True, drop=True)
    return sdf


def read_large_geotiff(file_path):
    dask.config.set({'temporary-directory': '/tablespace/dask'})
    dask.config.set({'distributed.comm.timeouts.tcp': '600s'})
    dask.config.set({'distributed.comm.timeouts.connect': '600s'})
    dask.config.get('distributed.comm.timeouts')
    bands = None
    with rasterio.open(file_path) as src:
        src_crs = src.crs
        values = {}
        if bands is None:
            bands = range(1, src.count + 1)
        for band in bands:
            values[f'band_{band}'] = src.read(band)
        height = values['band_1'].shape[0]
        width = values['band_1'].shape[1]
        transform = src.transform

    colrow = numpy.meshgrid(numpy.arange(width, dtype='int32'), numpy.arange(height, dtype='int32'))
    colrow = xarray.DataArray(colrow, copy=False).chunk({'dim_1': 1000, 'dim_2': 1000})
    trans = numpy.array([transform.a, transform.b, transform.c, transform.d, transform.e, transform.f], dtype='float64')

    def wrap_xy(colrow):
        xs = colrow[0] * trans[0] + colrow[1] * trans[1] + trans[2] + trans[0] / 2
        ys = colrow[0] * trans[3] + colrow[1] * trans[4] + trans[5] + trans[4] / 2
        return numpy.array([xs, ys])

    xy = xarray.apply_ufunc(wrap_xy, colrow, dask="parallelized", output_dtypes=['float64'])

    epsg_4326 = 'EPSG:4326'
    transformer = pyproj.Transformer.from_crs(src_crs, epsg_4326)

    def wrap_transform(coords):
        return numpy.array(transformer.transform(coords[0], coords[1]), dtype='float64')

    coords = xarray.apply_ufunc(wrap_transform, xy, dask="parallelized", output_dtypes=['float64'])

    sids = xarray.apply_ufunc(pystare.from_latlon_2d, coords[0], coords[1], dask="parallelized",
                              kwargs={'adapt_level': True})

    with dask.distributed.Client(n_workers=60, threads_per_worker=1, memory_limit='10GB', processes=True) as client:
        sids = sids.compute()
    sids = xarray.DataArray.to_numpy(sids)

    sdf = starepandas.STAREDataFrame({'sids': sids.flatten(), 'snow': values['band_1'].flatten()})
    return sdf
