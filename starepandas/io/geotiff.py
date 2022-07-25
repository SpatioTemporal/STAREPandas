import starepandas
import rasterio
import numpy
import geopandas
import pyproj
import pystare


def flatten_values(values):
    for key in values:
        values[key] = values[key].flatten()
    return values


def read_geotiff(file_path, bands=None, add_sids=True,
                 add_pts=False, add_xy=False, add_latlon=False,
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

    with rasterio.open(file_path) as src:
        values = {}
        if bands is None:
            bands = range(1, src.count + 1)
        for band in bands:
            values[f'band_{band}'] = src.read(band)
        height = values['band_1'].shape[0]
        width = values['band_1'].shape[1]
        cols, rows = numpy.meshgrid(numpy.arange(width), numpy.arange(height))
        xs, ys = rasterio.transform.xy(src.transform, rows, cols)
        xs = numpy.array(xs)
        ys = numpy.array(ys)
        src_crs = src.crs

    epsg_4326 = 'EPSG:4326'
    transformer = pyproj.Transformer.from_crs(src_crs, epsg_4326)
    lats, lons = transformer.transform(xs, ys)

    values = flatten_values(values)
    sdf = starepandas.STAREDataFrame(values)

    if add_latlon:
        sdf['lon'] = lons.flatten()
        sdf['lat'] = lats.flatten()
    if add_xy:
        sdf['x'] = cols.flatten()
        sdf['y'] = rows.flatten()
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
