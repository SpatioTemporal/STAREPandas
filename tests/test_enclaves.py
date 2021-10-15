import starepandas
import geopandas


def test_enclave():
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    rsa = world[world.name == 'South Africa']
    rsa = starepandas.STAREDataFrame(rsa, add_sids=True, resolution=5, add_trixels=False)
    assert len(rsa.sids.iloc[0]) == 35
