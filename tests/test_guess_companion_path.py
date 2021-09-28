import starepandas
import pytest


def test_multiple_companions():
    granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    with pytest.raises(starepandas.io.granules.MultipleCompanionsFoundError):
        starepandas.io.granules.guess_companion_path(granule_path)


def test_no_companions():
    granule_path = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    with pytest.raises(starepandas.io.granules.CompanionNotFoundError):
        starepandas.io.granules.guess_companion_path(granule_path)


def test_companion_found():
    granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    companion = starepandas.datasets.get_path('VNP03DNB.A2020219.0742.001.2020219124651.nc')
    companion_test = starepandas.io.granules.guess_companion_path(granule_path, prefix='VNP03DNB')
    assert companion == companion_test


def test_folder_set():
    granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    folder = '/'.join(granule_path.split('/')[0:-1])
    companion = starepandas.datasets.get_path('VNP03DNB.A2020219.0742.001.2020219124651.nc')
    companion_test = starepandas.io.granules.guess_companion_path(granule_path, folder=folder, prefix='VNP03DNB')
    assert companion == companion_test


def test_issue47():
    granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    folder = '/'.join(granule_path.split('/')[0:-1]) + '/'
    companion = starepandas.datasets.get_path('VNP03DNB.A2020219.0742.001.2020219124651.nc')
    companion_test = starepandas.io.granules.guess_companion_path(granule_path, folder=folder, prefix='VNP03DNB')
    assert companion == companion_test


def test_granule_guescompanion():
    granule_path = 'tests/data/granules/VNP02DNB.A2020219.0742.001.2020219125654.nc'
    granule = starepandas.io.granules.VNP02DNB(granule_path)
    companion_test = granule.guess_companion_path(prefix='VNP03DNB', folder='tests/data/granules/')
    assert companion_test == 'tests/data/granules/VNP03DNB.A2020219.0742.001.2020219124651.nc'