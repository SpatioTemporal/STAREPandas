import starepandas
import shapely
import numpy
import glob
import re
import pandas


# Dask helpers
def slam(client, action, data, partition_factor=1.5, dbg=0):
    np = sum(client.nthreads().values())
    if dbg > 0:
        print('slam: np = %i' % np)
    shard_bounds = [int(i * len(data) / (1.0 * partition_factor * np)) for i in range(int(partition_factor * np))]
    if shard_bounds[-1] != len(data):
        if dbg > 0:
            print('a sb[-1]: ', shard_bounds[-1], len(data))
        shard_bounds = shard_bounds + [len(data)]
    if dbg > 0:
        print('sb: ', shard_bounds)
    data_shards = [data[shard_bounds[i]:shard_bounds[i + 1]] for i in range(len(shard_bounds) - 1)]
    if dbg > 0:
        print('ds len:        ', len(data_shards))
        print('ds item len:   ', len(data_shards[0]))
        print('ds type:       ', type(data_shards[0]))
        try:
            print('ds dtype:      ', data_shards[0].dtype)
        except:
            pass
    big_future = client.scatter(data_shards)
    results = client.map(action, big_future)
    return results


def make_row(granule_path, add_sf=False):
    granule = starepandas.io.granules.granule_factory(granule_path)

    granule.sidecar_path = granule.guess_sidecar_path()
    if not granule.sidecar_path:
        print('no sidecar found for {}'.format(granule_path))
        return None
    granule.read_sidecar_cover()

    row = {}
    row['granule_path'] = granule_path
    row['sidecar_path'] = granule.sidecar_path
    row['stare_cover'] = granule.stare_cover

    granule.read_timestamps()
    row['begining'] = granule.ts_start
    row['ending'] = granule.ts_end

    if add_sf:
        row['geom'] = get_sf_cover(granule_path)
    row = pandas.DataFrame([row])
    return row


def get_sf_cover(granule_path):
    hdf = starepandas.io.s3.sd_wrapper(granule_path)
    lon1 = hdf.select('Longitude').get()[0:-1, 0].astype(numpy.double)
    lat1 = hdf.select('Latitude').get()[0:-1, 0].astype(numpy.double)

    lon2 = hdf.select('Longitude').get()[-1, 0:-1].astype(numpy.double)
    lat2 = hdf.select('Latitude').get()[-1, 0:-1].astype(numpy.double)

    lon3 = hdf.select('Longitude').get()[0:-1, -1].astype(numpy.double)
    lat3 = hdf.select('Latitude').get()[0:-1, -1].astype(numpy.double)
    lon3 = numpy.flip(lon3)
    lat3 = numpy.flip(lat3)

    lon4 = hdf.select('Longitude').get()[0, 0:-1].astype(numpy.double)
    lat4 = hdf.select('Latitude').get()[0, 0:-1].astype(numpy.double)
    lon4 = numpy.flip(lon4)
    lat4 = numpy.flip(lat4)

    lon = numpy.concatenate((lon1, lon2, lon3, lon4))
    lat = numpy.concatenate((lat1, lat2, lat3, lat4))
    return shapely.geometry.Polygon(zip(lon, lat))


def folder2catalog(path, granule_trunk='', granule_extension='*', add_sf=False, client=None):
    """ Reads a folder of granules into a STAREDataFrame catalog

    :param path: Path of the folder containing granules
    :type path: str
    :param granule_trunk: Granule identifier (e.g. MOD09)
    :type granule_trunk: str
    :param granule_extension: Extension of the granule (e.g. hdf, nc, HDF5)
    :type granule_extension: str
    :param add_sf: toggle creating simple feature representation of the iFOVs
    :type add_sf: bool
    :param client:
    :type client:
    :return: catalog
    :rtype: starepandas.STAREDataFrame
    """
    term = '{path}/{granule_trunk}*.{ext}'.format(path=path, granule_trunk=granule_trunk, ext=granule_extension)
    s3 = None
    if path[0:5] != 's3://':
        granule_paths = glob.glob(term)
    else:
        granule_paths, s3 = starepandas.io.s3.s3_glob(path, '.*\.{ext}$'.format(ext=granule_extension))
    if not granule_paths:
        print('no granules in folder')
        return None

    pattern = '.*[^_stare]\.(nc|hdf|HDF5)'
    granule_paths = list(filter(re.compile(pattern).match, granule_paths))

    rows = []
    if client is None:
        for granule_path in granule_paths:
            if s3 is not None:
                granule_url = 's3://{bucket_name}/{granule}'.format(bucket_name=s3[0]['bucket_name'],
                                                                    granule=granule_path)
            else:
                granule_url = granule_path
            row = make_row(granule_url, add_sf)
            rows.append(row)
    else:
        pass
        # client=Client()
        # client.close()
    df = pandas.concat(rows)
    df = starepandas.STAREDataFrame(df, sids='stare_cover')
    if add_sf:
        df.set_geometry('geom', inplace=True)
    return df
