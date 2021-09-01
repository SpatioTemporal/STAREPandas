import starepandas
import geopandas


def test_enclave():
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    rsa = world[world.name == 'South Africa']
    rsa = starepandas.STAREDataFrame(rsa, add_stare=True, level=5, add_trixels=False)
    assert len(rsa.stare.iloc[0]) == 35
