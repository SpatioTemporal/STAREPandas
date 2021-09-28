import starepandas
import pandas
import geopandas
import pytest


cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
countries = ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela']
latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
data = {'City': cities,  'Country': countries,
        'Latitude': latitudes, 'Longitude': longitudes}

df = pandas.DataFrame(data)
geom = geopandas.points_from_xy(df.Longitude, df.Latitude)
gdf = geopandas.GeoDataFrame(df, geometry=geom)

stare = starepandas.sids_from_gdf(gdf, resolution=5)
sdf = starepandas.STAREDataFrame(gdf)
sdf.set_sids(stare, inplace=True)

trixels = sdf.make_trixels()
trixel_df = sdf.set_trixels(trixels, inplace=False)


def test_plot_notrixel():
    sdf.plot(trixels=False, color='r')


def test_plot_trixel():
    trixel_df.plot(trixels=True, color='b')


def test_issue51_a():
    with pytest.raises(AttributeError):
        sdf.plot(trixels=True)
