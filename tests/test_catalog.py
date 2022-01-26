import starepandas


def test_creation():
    folder = 'tests/data/catalog/'
    catalog = starepandas.folder2catalog(path=folder, granule_extension='hdf', add_sf=False)
    print(catalog)
    assert catalog.index.size == 3
