import pytest
import starepandas
import os


def test_read_cldmsk_viirsl2():
    fname = 'tests/data/granules/CLDMSK_L2_VIIRS_SNPP.A2020219.0742.001.2020219190616.nc'
    granule = starepandas.io.granules.CLDMSKL2VIIRS(fname)
    granule.read_timestamps()

    assert granule.ts_start == '2020-08-06T07:42:00.000Z'
    assert granule.ts_end == '2020-08-06T07:48:00.000Z'
    granule.read_data()
    granule.read_latlon()


def test_read_vnp02dnb():
    fname = 'tests/data/granules/viirs/VNP02DNB.A2022308.1930.002.2022309051542.nc'
    granule = starepandas.io.granules.VNP02DNB(fname)
    granule.read_timestamps()
    assert granule.ts_start == '2022-11-04T19:30:00.000Z'
    assert granule.ts_end == '2022-11-04T19:36:00.000Z'
    granule.read_data()


def test_read_vnp03dnb():
    fname = 'tests/data/granules/viirs/VNP03DNB.A2022308.1930.002.2022309041547.nc'
    granule = starepandas.io.granules.VNP03DNB(fname)
    granule.set_nom_res('750m')
    scp = granule.guess_sidecar_path()
    assert scp == 'tests/data/granules/viirs/VNP03DNB.A2022308.1930.002.2022309041547_stare.nc'

    granule.read_timestamps()
    assert granule.ts_start == '2022-11-04T19:30:00.000Z'
    assert granule.ts_end == '2022-11-04T19:36:00.000Z'

    granule.read_data()
    granule.read_latlon()
    granule.read_sidecar_cover()
    granule.read_sidecar_index()
    df = granule.to_df()
    assert 105078784 == df.size


def test_read_granule_vnp03dnb_nom_res():
    fname   = 'tests/data/granules/viirs/VNP03DNB.A2022308.1930.002.2022309041547.nc'
    scp     = 'tests/data/granules/viirs/VNP03DNB.A2022308.1930.002.2022309041547_stare.nc'

    df = starepandas.read_granule(
        fname,
        sidecar=True,
        sidecar_path=scp,
        latlon=True,
        nom_res='750m'
        )

    assert 78809088 == df.size


def test_read_mod05():
    fname = 'tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522.hdf'
    granule = starepandas.io.granules.Mod05(fname)
    granule.read_timestamps()
    assert str(granule.ts_start) == '2019-12-02 00:00:00'
    assert str(granule.ts_end) == '2019-12-02 00:05:00'
    granule.read_data()
    granule.read_latlon()
    scp = granule.guess_sidecar_path()

    assert scp == 'tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522_stare.nc'
    granule.read_sidecar_cover()
    granule.read_sidecar_index()
    df = granule.to_df()
    assert 1205820 == df.size


def test_read_granules():
    starepandas.read_granule('tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522.hdf',nom_res='5km')
    starepandas.read_granule('tests/data/granules/viirs/VNP02DNB.A2022308.1930.002.2022309051542.nc',nom_res='750m')


def test_sidecar_not_found():
    fname = 'tests/data/granules/MYD05_L2.A2020060.1635.061.2020061153519.hdf'
    with pytest.raises(starepandas.io.granules.SidecarNotFoundError):
        starepandas.read_granule(fname, latlon=False, sidecar=True)


def test_bootstrap():
    fname = 'tests/data/granules/MYD05_L2.A2020060.1635.061.2020061153519.hdf'
    modis = starepandas.read_granule(fname, add_sids=True, adapt_resolution=True)
    #trixels = modis.make_trixels()
    #modis.set_trixels(trixels, inplace=True)


def test_find_sidecar():
    fpath = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    mod05 = starepandas.io.granules.Mod05(fpath)
    sidecar_path = mod05.guess_sidecar_path()
    assert os.path.exists(sidecar_path)


def test_find_companion():
    granule = starepandas.io.granules.VNP02DNB('tests/data/granules/viirs/VNP02DNB.A2022308.1930.002.2022309051542.nc')
    companion = granule.guess_companion_path(prefix='VNP03DNB')
    assert companion == 'tests/data/granules/viirs/VNP03DNB.A2022308.1930.002.2022309041547.nc'
