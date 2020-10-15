import starepandas 
import shapely
from pyhdf.SD import SD
import numpy
import glob
import datetime


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


def make_row(granule_name, add_sf=False):
    sidecar_name = starepandas.guess_sidecar_name(granule_name)
    stare_cover = starepandas.read_sidecar(sidecar_name)
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
    hdf = SD(granule_name)
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


def folder2catalogue(path, granule_extension='hdf', add_sf=False):
    term = '{path}/*.{ext}'.format(path=path, ext=granule_extension)
    granule_names = glob.glob(term)    
    df = starepandas.STAREDataFrame()
    for granule_name in granule_names:
        row = make_row(granule_name, add_sf)
        df = df.append(row, ignore_index=True)
    df.set_stare('stare_cover', inplace=True)
    if add_sf:
        df.set_geometry('geom', inplace=True)
    return df
