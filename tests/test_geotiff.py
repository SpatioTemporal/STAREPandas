import starepandas


def test_geotiff():
    file_path = 'tests/data/wind.tif'
    sdf = starepandas.read_geotiff(file_path, add_pts=True, add_latlon=True, add_xy=True, add_trixels=True)
    assert sdf.shape == (93, 8)
