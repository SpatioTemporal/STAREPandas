import netCDF4
from pyhdf.SD import SD
import starepandas 
import geopandas
import pandas
import os
import glob
import numpy
import pystare
import collections
import re


def get_hdfeos_metadata(file_path):    
    hdf= starepandas.SD_wrapper(file_path)
    metadata = {}
    metadata['ArchiveMetadata'] = get_metadata_group(hdf, 'ArchiveMetadata')
    metadata['StructMetadata']  = get_metadata_group(hdf, 'StructMetadata')
    metadata['CoreMetadata']    = get_metadata_group(hdf, 'CoreMetadata')    
    return metadata
    
def get_metadata_group(hdf, group_name):
    metadata_group = {}
    keys = [s for s in hdf.attributes().keys() if group_name in s]
    for key in keys:    
        string = hdf.attributes()[key]
        m = starepandas.parse_hdfeos_metadata(string)
        metadata_group  = {**metadata_group, **m}    
    return metadata_group

def parse_hdfeos_metadata(string):
    out = {} #collections.OrderedDict()
    lines0 = [i.replace('\t','') for i in string.split('\n')]
    lines = []
    for line in lines0:
        if "=" in line:
            
            key = line.split('=')[0]
            value = '='.join(line.split('=')[1:])
            lines.append(key.strip()+'='+value.strip())
        else:
            lines.append(line)
    i = -1
    while i<(len(lines))-1:        
        i+=1
        line = lines[i]
        if "=" in line:
            key = line.split('=')[0]
            value = '='.join(line.split('=')[1:])#.join('=')
            if key in ['GROUP', 'OBJECT']:
                endIdx = lines[i+1:].index('END_{}={}'.format(key, value))
                endIdx += i+1
                out[value] = parse_hdfeos_metadata("\n".join(lines[i+1:endIdx]))
                i = endIdx
            elif ('END_GROUP' not in key) and ('END_OBJECT' not in key):
                out[key] = str(value)
    return out

def read_modis_base(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=None, row_max=None, col_min=None, col_max=None):
    file_path = os.path.abspath(file_path)
    hdf = starepandas.SD_wrapper(file_path)
    lon = hdf.select('Longitude').get()[row_min:row_max, col_min:col_max].astype(numpy.double)
    lat = hdf.select('Latitude').get()[row_min:row_max, col_min:col_max].astype(numpy.double)
    if track_first:
        lon = numpy.ascontiguousarray(lon.transpose())
        lat = numpy.ascontiguousarray(lat.transpose())
    
    modis = {'lat': lat.flatten(), 'lon': lon.flatten()}
    if sidecar:
        sidecar_path = guess_sidecar_name(file_path)
        modis['stare']       = read_mod05_sidecar_index(sidecar_path)
        modis['stare_cover'] = read_mod05_sidecar_cover(sidecar_path)
    elif add_stare:
        stare = pystare.from_latlon2D(lat=lat, lon=lon, adapt_resolution=adapt_resolution)
        modis['stare'] = stare.flatten()
        # modis['stare_cover']
    modis = starepandas.STAREDataFrame(modis)
    return modis

def read_mod09(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=None, row_max=None, col_min=None, col_max=None):
    modis = read_modis_base(file_path, sidecar, track_first, add_stare, adapt_resolution, 
               row_min, row_max, col_min, col_max)
    file_path = os.path.abspath(file_path)
    hdf = starepandas.SD_wrapper(file_path)
    for dataset_name in dict(filter(lambda elem: '1km' in elem[0], hdf.datasets().items())).keys():
        modis[dataset_name] = hdf.select(dataset_name).get()[row_min:row_max, col_min:col_max].flatten()
    return modis


def read_mod05(file_path, sidecar=None,
               track_first=False, add_stare=True, adapt_resolution=True, 
               row_min=None, row_max=None, col_min=None, col_max=None):
    modis = read_modis_base(file_path, sidecar, track_first, add_stare, adapt_resolution, 
               row_min, row_max, col_min, col_max)    
    file_path = os.path.abspath(file_path)
    hdf = starepandas.SD_wrapper(file_path)
    dataset_names = ['Scan_Start_Time', 'Solar_Zenith', 'Solar_Azimuth', 'Sensor_Zenith',
                     'Sensor_Azimuth', 'Water_Vapor_Infrared']
    dataset_names2 = ['Cloud_Mask_QA', 'Water_Vapor_Near_Infrared', 
                      'Water_Vaport_Corretion_Factors', 'Quality_Assurance_Near_Infrared', 'Quality_Assurance_Infrared']
    for dataset_name in dataset_names:
        modis[dataset_name] = hdf.select(dataset_name).get()[row_min:row_max, col_min:col_max].flatten()
    return modis
    

def guess_sidecar_name(file_path):
    name =  '.'.join(file_path.split('.')[0:-1]) + '_stare.nc'
    # print('guessing name: ',name)
    if 's3://' == file_path[0:5]:
        tokens = starepandas.parse_s3_url(name)
        names,_ = starepandas.s3_glob(name)
        target = '{prefix}/{resource}'.format(prefix=tokens['prefix'],resource=tokens['resource'])
        # print('found names:\n',names)
        if target in names:
            return name
    else:
        if glob.glob(name):
            return name
    return None


def read_sidecar_cover(file_path):
    if re.search('MOD05|MYD05',file_path,re.IGNORECASE):
        return read_mod05_sidecar_cover(file_path)
    else:
        print('read_sidecar_cover cannot handle %s'%file_path)
    return None

def read_mod05_sidecar_index(file_path):
    ds = starepandas.nc4_Dataset_wrapper(file_path)
    stare = ds['STARE_index_5km'][:,:].flatten().astype(numpy.int64)
    return stare

def read_mod05_sidecar_cover(file_path):
    ds = starepandas.nc4_Dataset_wrapper(file_path)
    stare = ds['STARE_cover_5km'][:].flatten().astype(numpy.int64)
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
    vnp02_netcdf = starepandas.nc4_Dataset_wrapper(vnp02_path, 'r', format='NETCDF4')
    dnb = vnp02_netcdf.groups['observation_data']['DNB_observations'][:].data.flatten()
    vnp02_netcdf.groups['observation_data']['DNB_quality_flags'][:].data.flatten()
    
    vnp03_netcdf = starepandas.nc4_Dataset_wrapper(vnp03_path, 'r', format='NETCDF4')
    lat = vnp03_netcdf.groups['geolocation_data']['latitude'][:].data.flatten()
    lon = vnp03_netcdf.groups['geolocation_data']['longitude'][:].data.flatten()
        
    vnp02 = {'lat': lat, 'lon': lon, 'dnb': dnb}
    vnp02 = STAREDataFrame(vnp02)
    return vnp02

