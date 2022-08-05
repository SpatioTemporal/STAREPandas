import starepandas


def test():
    file_path = 'tests/data/granules/MOD09.A2002299.0710.006.2015151173939.hdf'
    sdf = starepandas.read_granule(file_path, sidecar=True)
    sdf.spatial_resolution()
