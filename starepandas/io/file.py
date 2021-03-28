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
        self.data = {}
        self.lat = None
        self.lon = None
        self.stare = None
        #self.file_path = os.path.abspath(file_path)
    
    def guess_sidecar_name(self):
        name =  '.'.join(self.file_path.split('.')[0:-1]) + '_stare.nc'
        if 's3://' == self.file_path[0:5]:
            tokens = starepandas.parse_s3_url(name)
            names,_ = starepandas.s3_glob(name)
            target = '{prefix}/{resource}'.format(prefix=tokens['prefix'], resource=tokens['resource'])            
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
        if self.lat is not None:
            df['lat']  = self.lat.flatten()
            df['lon'] = self.lon.flatten()
        if self.stare is not None:
            df['stare'] = self.stare.flatten()
        for key in self.data.keys():
            df[key] = self.data[key].flatten()
        return starepandas.STAREDataFrame(df)
    

class VNP03DNB(Granule):
    def __init__(self, file_path):                
        super(VNP03DNB, self).__init__(file_path)
        self.nc = starepandas.nc4_Dataset_wrapper(file_path)
        self.nom_res = '750m'
        
    def read_latlon(self):
        self.lats = self.nc.groups['geolocation_data']['latitude'][:].data.astype(numpy.double)
        self.lons = self.nc.groups['geolocation_data']['longitude'][:].data.astype(numpy.double)
        
    def read_data(self):
        self.data = {}
        self.data['moon_illumination_fraction'] = self.nc.groups['geolocation_data']['moon_illumination_fraction'][:].data.astype(numpy.int)
        self.data['land_water_mask'] = self.nc.groups['geolocation_data']['land_water_mask'][:].data.astype(numpy.boolean)
        land_water_mask
        
    def get_timestamps(self):
        self.begining = self.nc.StartTime
        self.end = self.nc.EndTime


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
            
    def get_timestamps(self):
        meta = get_hdfeos_metadata(granule_name)
        meta_group = meta['CoreMetadata']['INVENTORYMETADATA']['RANGEDATETIME']
        begining_date = meta_group['RANGEBEGINNINGDATE']['VALUE']
        begining_time = meta_group['RANGEBEGINNINGTIME']['VALUE']
        end_date = meta_group['RANGEENDINGDATE']['VALUE']
        end_time = meta_group['RANGEENDINGTIME']['VALUE']
        self.begining = datetime.datetime.strptime(begining_date+begining_time, '"%Y-%m-%d""%H:%M:%S.%f"') 
        self.end = datetime.datetime.strptime(end_date+end_time, '"%Y-%m-%d""%H:%M:%S.%f"') 
            

class Mod09(Modis):
    
    def __init__(self, file_path):
        super(Mod09, self).__init__(file_path)
        self.nom_res = '1km'
    
    def read_data(self):
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
        for dataset_name in dataset_names:
            self.data[dataset_name] = self.hdf.select(dataset_name).get()
            

class CLDMSK_L2_VIIRS(Granule):
    
    def __init__(self, file_path):
        super(CLDMSK_L2_VIIRS, self).__init__(file_path)
        self.nom_res = '750m'
        self.netcdf = starepandas.nc4_Dataset_wrapper(self.file_path, 'r', format='NETCDF4')
        
    def read_data(self):
        # 0 = cloudy, 1 = probably cloudy, 2 = probably clear, 3 = confident clear, -1 = no result
        self.data['Integer_Cloud_Mask'] = self.netcdf.groups['geophysical_data']['Integer_Cloud_Mask'][:].data
        
    def read_latlon(self):        
        self.lat = self.netcdf.groups['geolocation_data']['latitude'][:].data.astype(numpy.double)
        self.lon = self.netcdf.groups['geolocation_data']['longitude'][:].data.astype(numpy.double)


class VNP03DNB(Granule):
    
    def __init__(self, file_path):
        super(VNP03DNB, self).__init__(file_path)
        self.nom_res = '750m'
        self.netcdf = starepandas.nc4_Dataset_wrapper(self.file_path, 'r', format='NETCDF4')
    
    def read_data(self):        
        self.data['moon_illumination_fraction'] = self.netcdf.groups['geolocation_data']['moon_illumination_fraction'][:].data
        
        # 1: Shallow_Ocean 2: Land 3: Coastline 4: Shallow_Inland 5: Ephemeral 6: Deep_Inland 7: Continental 8: Deep_Ocean
        self.data['land_water_mask'] = self.netcdf.groups['geolocation_data']['land_water_mask'][:].data
        
        # 1: Input_invalid 2: Pointing_bad 3: Terrain_bad
        self.data['quality_flag'] = self.netcdf.groups['geolocation_data']['quality_flag'][:].data
                
    def read_latlon(self):        
        self.lat = self.netcdf.groups['geolocation_data']['latitude'][:].data.astype(numpy.double)
        self.lon = self.netcdf.groups['geolocation_data']['longitude'][:].data.astype(numpy.double)
    
            
class VNP02DNB(Granule):
    
    def __init__(self, vnp02_path, vnp03_path=None, vnp03_folder=None):
        self.data = {}
        self.lat = None
        self.lon = None
        self.stare = None
        self.vnp02_path = vnp02_path
        if not vnp03_path:
            vnp03_path = self.guess_vnp03path(vnp03_folder) 
        self.vnp03 = VNP03DNB(vnp03_path)
        self.sidecar_name = self.vnp03.guess_sidecar_name()
        self.netcdf = starepandas.nc4_Dataset_wrapper(self.vnp02_path, 'r', format='NETCDF4')
            
    def guess_vnp03path(self, vnp03_folder=None):
        name_trunk = self.vnp02_path
        if vnp03_folder:
            name_trunk = self.vnp02_path.split('/')[-1]
            name_trunk = vnp03_folder + '/' + name_trunk    
        name_trunk = name_trunk.split('.')[0:-2]
        pattern = '.'.join(name_trunk).replace('VNP02DNB', 'VNP03DNB') + '*[0-9].nc'
        matches = glob.glob(pattern)
        if len(matches) < 1:
            print('did not find VNP03 companion')            
        vnp03_path = matches[0]
        return vnp03_path
    
    def read_latlon(self):
        self.vnp03.read_latlon()
        self.lat = self.vnp03.lat
        self.lon = self.vnp03.lon
    
    def read_data(self):
        dnb = self.netcdf.groups['observation_data']['DNB_observations'][:].data
        quality_flags = self.netcdf.groups['observation_data']['DNB_quality_flags'][:].data
        self.data['DNB_observations'] = dnb
        self.data['DNB_quality_flags'] = quality_flags
        self.vnp03.read_data()
        self.data = {**self.data, **self.vnp03.data}
        
    def read_sidecar_cover(self):
        self.vnp03.read_sidecar_cover()
        self.stare_cover = self.vnp03.stare_cover
        
    def read_sidecar_index(self, sidecar_path):
        self.vnp03.read_sidecar_index(sidecar_path)
        self.stare = self.vnp03.stare
        

def read_granule(file_path, read_latlon=True, sidecar=False, sidecar_path=None, track_first=False, add_stare=True, adapt_resolution=True, **kwargs):
    
    if re.search('MOD05|MYD05', file_path, re.IGNORECASE):
        granule = Mod05(file_path)
    elif re.search('MOD09|MYD09', file_path, re.IGNORECASE):
        granule = Mod09(file_path)
    elif re.search('VNP02DNB|VJ102DNB', file_path, re.IGNORECASE):
        granule = VNP02DNB(file_path, **kwargs)
    elif re.search('VNP03DNB|VJ103DNB', file_path, re.IGNORECASE):
        granule = VNP03DNB(file_path, **kwargs)
    elif re.search('CLDMSK_L2_VIIRS', file_path, re.IGNORECASE):
        granule = CLDMSK_L2_VIIRS(file_path, **kwargs)
    else:
        print('read_granule() cannot handle %s'%file_path)
        return None
    
    if read_latlon:
        granule.read_latlon()
        
    granule.read_data()
    
    if sidecar:        
        granule.read_sidecar_index(sidecar_path)
    elif add_stare:        
        granule.add_stare(adapt_resolution)
    
    return granule.to_df()
    

# Below needs revision




