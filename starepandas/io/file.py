import netCDF4
from pyhdf.SD import SD
import starepandas 
import geopandas
import pandas
import os
import glob
import numpy
import pystare
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
    out = {} 
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


class Granule:
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.sidecar_name = self.guess_sidecar_name()
        #self.file_path = os.path.abspath(file_path)
    
    def guess_sidecar_name(self):
        name =  '.'.join(self.file_path.split('.')[0:-1]) + '_stare.nc'
        if 's3://' == self.file_path[0:5]:
            tokens = starepandas.parse_s3_url(name)
            names,_ = starepandas.s3_glob(name)
            target = '{prefix}/{resource}'.format(prefix=tokens['prefix'],resource=tokens['resource'])            
            if target in names:
                return name
        else:
            if glob.glob(name):
                return name
        return None
    
    def add_stare(self, adapt_resolution=True):
        self.stare = pystare.from_latlon2D(lat=self.lat, lon=self.lon, adapt_resolution=adapt_resolution)        
    
    def read_sidecar_index(self, sidecar_path=None):
        if not sidecar_path:
            sidecar_path = self.guess_sidecar_name()
        ds = starepandas.nc4_Dataset_wrapper(sidecar_path)
        self.stare = ds['STARE_index_{}'.format(self.nom_res)][:,:].astype(numpy.int64)
        
    def read_sidecar_cover(self, sidecar_path=None):
        if not sidecar_path:
            sidecar_path = self.guess_sidecar_name()
        ds = starepandas.nc4_Dataset_wrapper(sidecar_path)        
        self.stare_cover = ds['STARE_cover_{}'.format(self.nom_res)][:].astype(numpy.int64)        
    
    def to_df(self):
        df = {}
        df['lat']  = self.lat.flatten()
        df['lon'] = self.lon.flatten()
        df['stare'] = self.stare.flatten()
        for key in self.data.keys():
            df[key] = self.data[key].flatten()
        return starepandas.STAREDataFrame(df)


class Modis(Granule):
    def __init__(self, file_path):                
        super(Modis, self).__init__(file_path)
        self.hdf = starepandas.SD_wrapper(file_path)        
    
    def read_latlon(self, track_first=False):
        self.lon = self.hdf.select('Longitude').get().astype(numpy.double)
        self.lat = self.hdf.select('Latitude').get().astype(numpy.double)
        if track_first:
            self.lon = numpy.ascontiguousarray(self.lon.transpose())
            self.lat = numpy.ascontiguousarray(self.lat.transpose())
            

class Mod09(Modis):
    def __init__(self, file_path):
        super(Mod09, self).__init__(file_path)
        self.nom_res = '1km'
    
    def read_data(self):
        self.data = {}
        for dataset_name in dict(filter(lambda elem: '1km' in elem[0], self.hdf.datasets().items())).keys():
            self.data[dataset_name] = self.hdf.select(dataset_name).get()

class Mod05(Modis):
    def __init__(self, file_path):
        super(Mod05, self).__init__(file_path)
        self.nom_res = '5km'
        
    def read_data(self):
        dataset_names = ['Scan_Start_Time', 'Solar_Zenith', 'Solar_Azimuth', 
                         'Sensor_Zenith', 'Sensor_Azimuth', 'Water_Vapor_Infrared']
    
        dataset_names2 = ['Cloud_Mask_QA', 'Water_Vapor_Near_Infrared', 
                          'Water_Vaport_Corretion_Factors', 'Quality_Assurance_Near_Infrared', 'Quality_Assurance_Infrared']
        self.data = {}
        for dataset_name in dataset_names:
            self.data[dataset_name] = self.hdf.select(dataset_name).get()
    

def read_mod09(file_path, sidecar=False, track_first=False, add_stare=True, adapt_resolution=True):
    return read_granule(file_path, sidecar, track_first, add_stare, adapt_resolution)

def read_mod05(file_path, sidecar=False, track_first=False, add_stare=True, adapt_resolution=True):
    return read_granule(file_path, sidecar, track_first, add_stare, adapt_resolution)


def read_granule(file_path, sidecar=False, track_first=False, add_stare=True, adapt_resolution=True):
    
    if re.search('MOD05|MYD05', file_path, re.IGNORECASE):
        granule = Mod05(file_path)
    elif re.search('MOD09|MYD09', file_path, re.IGNORECASE):
        granule = Mod09(file_path)
    else:
        print('read_granule() cannot handle %s'%file_path)
    
    granule.read_latlon()
    granule.read_data()
    
    if sidecar:        
        granule.read_sidecar_index()
    elif add_stare:        
        granule.add_stare(adapt_resolution)
    
    return granule.to_df()
    

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

