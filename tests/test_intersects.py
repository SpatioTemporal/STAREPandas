import starepandas
import geopandas
import pandas
import numpy

countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
countries = starepandas.STAREDataFrame(countries)


def test_types():
    # Can we send: int, list and array to intersects?
    series = pandas.Series([[4035225266123964416], [4254212798004854789, 4255901647865118724]])
    assert 2 == starepandas.series_intersects(series, 4035225266123964416).sum()
    assert 2 == starepandas.series_intersects(series, [4035225266123964416]).sum()
    assert 2 == starepandas.series_intersects(series, numpy.array([4035225266123964416])).sum()


def test_polygon():
    iceland = countries[countries.name == 'Iceland']
    sids = starepandas.sids_from_gdf(iceland, resolution=8, force_ccw=True)
    fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    modis = starepandas.read_granule(fname, latlon=False, sidecar=True)
    intersects = modis.stare_intersects(sids[0])
    assert 1384 == sum(intersects)


def test_polygon2():
    brazil_sids = countries[countries.name == 'Brazil'].make_sids(resolution=5)[0]
    cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas', 'Sao Paulo', 'Bridgetown']

    latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48, -23.55, 13.1]
    longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86, -46.63, -59.62]
    data = {'City': cities, 'Latitude': latitudes, 'Longitude': longitudes}

    cities = starepandas.STAREDataFrame(data)
    sids = starepandas.sids_from_xy(cities.Longitude, cities.Latitude, resolution=27)
    cities.set_sids(sids, inplace=True)

    intersects_stare = cities.stare_intersects(brazil_sids)
    assert sum(intersects_stare) == 2

    disjoint = cities.stare_disjoint((brazil_sids))
    assert sum(disjoint) == 5
