import starepandas 
import shapely
from pyhdf.SD import SD
import numpy
import glob
import datetime

# from dask_gateway import Gateway
# from dask_gateway import GatewayCluster
# from dask.distributed import Client

# Dask helpers
def slam(client,action,data,partition_factor=1.5,dbg=0):
    np = sum(client.nthreads().values())
    if dbg>0:
        print('slam: np = %i'%np)
    shard_bounds = [int(i*len(data)/(1.0*partition_factor*np)) for i in range(int(partition_factor*np))] 
    if shard_bounds[-1] != len(data):
        if dbg>0:
            print('a sb[-1]: ',shard_bounds[-1],len(data))
        shard_bounds = shard_bounds + [len(data)]
    if dbg>0:
        print('sb: ',shard_bounds)
    data_shards = [data[shard_bounds[i]:shard_bounds[i+1]] for i in range(len(shard_bounds)-1)]
    if dbg>0:
        print('ds len:        ',len(data_shards))
        print('ds item len:   ',len(data_shards[0]))
        print('ds type:       ',type(data_shards[0]))
        try:
            print('ds dtype:      ',data_shards[0].dtype)
        except:
            pass
    big_future = client.scatter(data_shards)
    results    = client.map(action,big_future)
    return results

def get_timestamps(granule_name):
    meta = starepandas.get_hdfeos_metadata(granule_name)
    meta_group = meta['CoreMetadata']['INVENTORYMETADATA']['RANGEDATETIME']
    begining_date = meta_group['RANGEBEGINNINGDATE']['VALUE']
    begining_time = meta_group['RANGEBEGINNINGTIME']['VALUE']
    end_date = meta_group['RANGEENDINGDATE']['VALUE']
    end_time = meta_group['RANGEENDINGTIME']['VALUE']
    begining= datetime.datetime.strptime(begining_date+begining_time, '"%Y-%m-%d""%H:%M:%S.%f"') 
    end = datetime.datetime.strptime(end_date+end_time, '"%Y-%m-%d""%H:%M:%S.%f"') 
    return begining, end

# granule_name might be a url
def make_row(granule_name, add_sf=False):
    sidecar_name = starepandas.guess_sidecar_name(granule_name)
    if not sidecar_name:
        print('no sidecar found for {}'.format(granule_name))
        return None
    stare_cover = starepandas.read_sidecar_cover(sidecar_name)
    row = {}
    row['granule_name'] = granule_name
    row['sidecar_name'] = sidecar_name
    row['stare_cover'] = stare_cover
    begining, end = get_timestamps(granule_name)
    row['begining'] = begining
    row['ending'] = end
    if add_sf:
        row['geom'] = get_sf_cover(granule_name)
    return row

def get_sf_cover(granule_name):    
    hdf = starepandas.SD_wrapper(granule_name)
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


def folder2catalogue(path, granule_extension='hdf', add_sf=False, client = None):
    term = '{path}/*.{ext}'.format(path=path, ext=granule_extension)
    s3 = None
    if path[0:5] != 's3://':
        granule_names = glob.glob(term)
    else:
        granule_names,s3 = starepandas.s3_glob(path,'.*\.{ext}$'.format(ext=granule_extension))
    if not granule_names:
        print('no granules in folder')
        return None
    df = starepandas.STAREDataFrame()
    if client is None:
        for granule_name in granule_names:
            if s3 is not None:
                granule_url = 's3://{bucket_name}/{granule}'.format(bucket_name=s3[0]['bucket_name'],granule=granule_name)
            else:
                granule_url = granule_name
            row = make_row(granule_url, add_sf)
            df = df.append(row, ignore_index=True)
    else:
        pass
        # client=Client()
        # client.close()
    df.set_stare('stare_cover', inplace=True)
    if add_sf:
        df.set_geometry('geom', inplace=True)
    return df


