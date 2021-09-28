import starepandas
import geopandas


cities = ['Buenos Aires', 'Brasilia', 'Santiago',  'Bogota', 'Caracas', 'Sao Paulo', 'Bridgetown']
latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48, -23.55, 13.1]
longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86, -46.63, -59.62]
data = {'City': cities,  'Latitude': latitudes, 'Longitude': longitudes}
cities = starepandas.STAREDataFrame(data)
stare = starepandas.sids_from_xy(cities.Longitude, cities.Latitude, resolution=26)
cities.set_sids(stare, inplace=True)

countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
samerica = countries[countries.continent == 'South America']
stare = starepandas.sids_from_gdf(samerica, resolution=6, force_ccw=True)
samerica = starepandas.STAREDataFrame(samerica, sids=stare)


def join():
    joined = starepandas.stare_join(samerica, cities, how='left')
    column_names = ['pop_est', 'continent', 'name', 'iso_a3', 'gdp_md_est', 'geometry', 'stare_left', 'trixels_left',
                    'key_right', 'City', 'Latitude', 'Longitude', 'stare_right', 'trixels_right']
    assert list(joined.columns) == column_names