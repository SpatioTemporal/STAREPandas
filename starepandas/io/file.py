import netCDF4
from pyhdf.SD import SD
import starepandas 
import geopandas
import pandas
import os
import glob
import numpy
import pystare
import netCDF4


def read_mod09(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=0, row_max=-1, col_min=0, col_max=-1):
    modis = read_modis_base(file_path, sidecar, track_first, add_stare, adapt_resolution, 
               row_min, row_max, col_min, col_max)
    file_path = os.path.abspath(file_path)
    hdf = SD(file_path)
    for dataset_name in dict(filter(lambda elem: '1km' in elem[0], hdf.datasets().items())).keys():
        modis[dataset_name] = hdf.select(dataset_name).get()[row_min:row_max, col_min:col_max].flatten()
    return modis


def read_mod05(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=0, row_max=-1, col_min=0, col_max=-1):
    modis = read_modis_base(file_path, sidecar, track_first, add_stare, adapt_resolution, 
               row_min, row_max, col_min, col_max)    
    return modis
    

def read_modis_base(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=0, row_max=-1, col_min=0, col_max=-1):
    file_path = os.path.abspath(file_path)
    hdf = SD(file_path)
    lon = hdf.select('Longitude').get()[row_min:row_max, col_min:col_max].astype(numpy.double)
    lat = hdf.select('Latitude').get()[row_min:row_max, col_min:col_max].astype(numpy.double)
    if track_first:
        lon = numpy.ascontiguousarray(lon.transpose())
        lat = numpy.ascontiguousarray(lat.transpose())
    
    modis = {'lat': lat.flatten(), 'lon': lon.flatten()}
    if sidecar:
        sidecar_path = guess_sidecar_name(file_path)
        modis_['stare'] = read_sidecar(sidecar_path)    
    elif add_stare:
        stare = pystare.from_latlon2D(lat=lat, lon=lon, adapt_resolution=adapt_resolution)
        modis['stare'] = stare.flatten()
    modis = starepandas.STAREDataFrame(modis)
    return modis

def guess_sidecar_name(file_path):
    sidecar_name = file_path.split('.')[0:-1] + '_stare.nc'


def read_sidecar(file_path):
    ds = netCDF4.Dataset(file_path)
    stare = ds['STARE_index_1km'][:,:].flatten().astype(numpy.int64)
    return stare


# Below needs revision
def guess_vnp03path(vnp02_path):
    name_trunk = vnp02_path.split('.')[0:-2]
    pattern = '.'.join(name_trunk).replace('VNP02DNB', 'VNP03DNB') + '*'
    vnp03_path = glob.glob(pattern)[0]
    return vnp03_path


def read_vnp02dnb(vnp02_path, vnp03_path=None):
    vnp02_path = os.path.abspath(vnp02_path)
    if not vnp03_path:
        vnp03_path = guess_vnp03path(vnp02_path)            
    vnp02_netcdf = netCDF4.Dataset(vnp02_path, 'r', format='NETCDF4')
    dnb = vnp02_netcdf.groups['observation_data']['DNB_observations'][:].data.flatten()
    vnp02_netcdf.groups['observation_data']['DNB_quality_flags'][:].data.flatten()
    
    vnp03_netcdf = netCDF4.Dataset(vnp03_path, 'r', format='NETCDF4')
    lat = vnp03_netcdf.groups['geolocation_data']['latitude'][:].data.flatten()
    lon = vnp03_netcdf.groups['geolocation_data']['longitude'][:].data.flatten()
        
    vnp02 = {'lat': lat, 'lon': lon, 'dnb': dnb}
    vnp02 = STAREDataFrame(vnp02)
    return vnp02

