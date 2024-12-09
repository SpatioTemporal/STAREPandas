import geopandas as gpd
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

    stare1 = starepandas.sids_from_xy(df.Longitude, df.Latitude, level=5)
    stare2 = starepandas.sids_from_xy_df(gdf, n_partitions=1, level=5)
    stare3 = starepandas.sids_from_gdf(gdf, level=5)
    assert numpy.array_equal(stare1, stare2)
    assert numpy.array_equal(stare2, stare3)


def test_polygon():
    # Create a simple polygon representing Africa
    africa_geometry = Polygon([
        (-17.625, 37.21), (51.27, 37.21), (51.27, -34.82), (-17.625, -34.82), (-17.625, 37.21)
    ])

    # Create a GeoDataFrame for Africa
    data = {'continent': ['Africa'], 'name': ['Africa'], 'pop_est': [1340598147]}
    africa_gdf = gpd.GeoDataFrame(data, geometry=[africa_geometry], crs="EPSG:4326")

    # Generate STARE SIDs
    stare = starepandas.sids_from_gdf(africa_gdf, level=5, force_ccw=True)
