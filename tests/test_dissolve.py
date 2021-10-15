import geopandas
import starepandas

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
west = world[world['continent'].isin(['Europe', 'North America'])]
west = starepandas.STAREDataFrame(west, add_sids=True, resolution=4, add_trixels=False)


def test_dissolve():
    dissolved = west.stare_dissolve(by='continent', aggfunc='sum')
    assert len(dissolved) == 2

