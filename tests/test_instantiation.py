import pandas
import geopandas
import starepandas
import numpy


def test_points():
    cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
    countries = ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela']
    latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
    longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
    data = {'City' : cities, 'Country': countries,
            'Latitude': latitudes, 'Longitude': longitudes}

    df = pandas.DataFrame(data)
    geom = geopandas.points_from_xy(df.Longitude, df.Latitude)
    gdf = geopandas.GeoDataFrame(df, geometry=geom)

    stare1 = starepandas.sids_from_xy(df.Longitude, df.Latitude, resolution=5)
    stare2 = starepandas.sids_from_xy_df(gdf, n_workers=1, resolution=5)
    stare3 = starepandas.sids_from_gdf(gdf, resolution=5)
    assert numpy.array_equal(stare1, stare2)
    assert numpy.array_equal(stare2, stare3)


def test_polygon():
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    africa = world[world.continent == 'Africa']
    stare = starepandas.sids_from_gdf(africa, resolution=5, force_ccw=True)
