from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import datetime
import numpy


def get_hdfeos_metadata(file_path):    
    hdf= starepandas.io.s3.sd_wrapper(file_path)
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
        m = parse_hdfeos_metadata(string)
        metadata_group = {**metadata_group, **m}
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
    while i < (len(lines))-1:
        i += 1
        line = lines[i]
        if "=" in line:
            key = line.split('=')[0]
            value = '='.join(line.split('=')[1:])
            if key in ['GROUP', 'OBJECT']:
                endIdx = lines[i+1:].index('END_{}={}'.format(key, value))
                endIdx += i+1
                out[value] = parse_hdfeos_metadata("\n".join(lines[i+1:endIdx]))
                i = endIdx
            elif ('END_GROUP' not in key) and ('END_OBJECT' not in key):
                out[key] = str(value)
    return out 


class Modis(Granule):
    
    def __init__(self, file_path, sidecar_path=None):                
        super(Modis, self).__init__(file_path, sidecar_path)
        self.hdf = starepandas.io.s3.sd_wrapper(file_path)
    
    def read_latlon(self, track_first=False):
        self.lon = self.hdf.select('Longitude').get().astype(numpy.double)
        self.lat = self.hdf.select('Latitude').get().astype(numpy.double)
        if track_first:
            self.lon = numpy.ascontiguousarray(self.lon.transpose())
            self.lat = numpy.ascontiguousarray(self.lat.transpose())
            
    def read_timestamps(self):
        meta = get_hdfeos_metadata(self.file_path)
        meta_group = meta['CoreMetadata']['INVENTORYMETADATA']['RANGEDATETIME']
        begining_date = meta_group['RANGEBEGINNINGDATE']['VALUE']
        begining_time = meta_group['RANGEBEGINNINGTIME']['VALUE']
        end_date = meta_group['RANGEENDINGDATE']['VALUE']
        end_time = meta_group['RANGEENDINGTIME']['VALUE']
        self.ts_start = datetime.datetime.strptime(begining_date+begining_time, '"%Y-%m-%d""%H:%M:%S.%f"') 
        self.ts_end = datetime.datetime.strptime(end_date+end_time, '"%Y-%m-%d""%H:%M:%S.%f"')         
    

class Mod09(Modis):
    
    def __init__(self, file_path, sidecar_path=None):
        super(Mod09, self).__init__(file_path)
        self.nom_res = '1km'
    
    def read_data(self):
        for dataset_name in dict(filter(lambda elem: '1km' in elem[0], self.hdf.datasets().items())).keys():
            self.data[dataset_name] = self.hdf.select(dataset_name).get()
            

class Mod05(Modis):
    
    def __init__(self, file_path, sidecar_path=None):
        super(Mod05, self).__init__(file_path, sidecar_path)
        self.nom_res = '5km'
        
    def read_data(self):
        dataset_names = ['Scan_Start_Time', 'Solar_Zenith', 'Solar_Azimuth', 
                         'Sensor_Zenith', 'Sensor_Azimuth', 'Water_Vapor_Infrared']
    
        dataset_names2 = ['Cloud_Mask_QA', 'Water_Vapor_Near_Infrared', 
                          'Water_Vaport_Corretion_Factors', 'Quality_Assurance_Near_Infrared', 'Quality_Assurance_Infrared']
        for dataset_name in dataset_names:
            self.data[dataset_name] = self.hdf.select(dataset_name).get()
