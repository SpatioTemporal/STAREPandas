import starepandas
import geopandas

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))


def test_trixels():
    germany = world[world.name == 'Germany']
    germany = starepandas.STAREDataFrame(germany, add_sids=True, resolution=8)
    trixels = germany.make_trixels()
    germany.set_trixels(trixels)

