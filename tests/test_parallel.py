import starepandas
import geopandas


def test_sid_lookup():
    countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    countries = countries[countries['continent'] == 'Europe'][0:5]
    sids = starepandas.sids_from_geoseries(countries.geometry, resolution=6, convex=False, force_ccw=True, n_workers=2)
    assert sids[0][0] == 3999759419058421766


def test_trixel_lookup():
    pass


def test_dissolve():
    pass


def test_intersects():
    countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    iceland = countries[countries.name == 'Iceland']
    sids = starepandas.sids_from_gdf(iceland, resolution=8, force_ccw=True)
    fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    modis = starepandas.read_granule(fname, latlon=False, sidecar=True)
    intersects = modis.stare_intersects(other=sids[0], n_workers=2)
    assert 1384 == sum(intersects)
