import starepandas
import geopandas


countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))


def test_sid_lookup():
    europe = countries[countries['continent'] == 'Europe'][0:5]
    sids = starepandas.sids_from_geoseries(europe.geometry, level=6, convex=False, force_ccw=True, n_partitions=2)
    assert sids.iloc[0][0] == 3999759419058421766


def test_trixel_lookup():
    pass


def test_dissolve():
    pass


def test_intersects():
    iceland = countries[countries.name == 'Iceland']
    sids = starepandas.sids_from_gdf(iceland, level=8, force_ccw=True)
    #sids = sids.astype('int64')
    fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    modis = starepandas.read_granule(fname, latlon=False, sidecar=True)
    intersects = modis.stare_intersects(other=sids.iloc[0], n_partitions=2)
    assert 1384 == sum(intersects)


def test_issue62():
    # This leads to len(series) > n_workers. resulting in nworkers=0 and thus division by zero
    starepandas.sids_from_geoseries(countries[0:1].geometry, level=6, n_partitions=2)


def test_issue62_2():
    # This leads to more workers than len(series) resulting in a ValueError
    starepandas.sids_from_geoseries(countries[0:3].geometry, level=6, n_partitions=3)


def test_issue62_3():
    # This would recursively spin up more workers
    starepandas.sids_from_geoseries(countries[0:0].geometry, level=6, n_partitions=1)

