import starepandas
import pandas
import geopandas


cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
countries = ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela']
latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
data = {'City': cities,  'Country': countries,
        'Latitude': latitudes, 'Longitude': longitudes}

df = pandas.DataFrame(data)
geom = geopandas.points_from_xy(df.Longitude, df.Latitude)
gdf = geopandas.GeoDataFrame(df, geometry=geom)

stare = starepandas.stare_from_gdf(gdf, level=5)
sdf = starepandas.STAREDataFrame(gdf)
sdf.set_stare(stare, inplace=True)

trixels = sdf.make_trixels()
sdf.set_trixels(trixels, inplace=True)


def test_plot1():
    sdf.plot(trixels=False, color='r')


def test_plot2():
    sdf.plot(trixels=False, color='b')
