import starepandas
import geopandas
import shapely

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))


def test_trixels():
    germany = world[world.name == 'Germany']
    germany = starepandas.STAREDataFrame(germany, add_sids=True, resolution=8)
    trixels = germany.make_trixels()
    germany.set_trixels(trixels)


def test_wrap():
    geom = shapely.wkt.loads('POLYGON((-100 0, -200 0, -150 40, -100 0))')
    geom_split = starepandas.split_antimeridian(geom)
    assert min(geom_split.geoms[0].exterior.xy[0]) >= -180.0