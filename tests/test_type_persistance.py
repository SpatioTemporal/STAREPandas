import geopandas
import starepandas

countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
countries = starepandas.STAREDataFrame(countries)


def test_iloc():
    subset = countries.iloc[0:1]
    assert isinstance(subset, starepandas.STAREDataFrame)


def test_conditional():
    subset = countries[countries.continent == 'Africa']
    assert isinstance(subset, starepandas.STAREDataFrame)


def test_geom():
    countries['geom2'] = countries['geometry']
    countries.set_geometry('geom2', inplace=True)
    sdf = starepandas.STAREDataFrame(countries)
    assert sdf._geometry_column_name == countries._geometry_column_name