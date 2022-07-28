from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import datetime
import numpy


def get_hdfeos_metadata(file_path):    
    hdf = starepandas.io.s3.sd_wrapper(file_path)
    metadata = {'ArchiveMetadata': get_metadata_group(hdf, 'ArchiveMetadata'),
                'StructMetadata': get_metadata_group(hdf, 'StructMetadata'),
                'CoreMetadata': get_metadata_group(hdf, 'CoreMetadata')}
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
    lines0 = [i.replace('\t', '') for i in string.split('\n')]
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
        beginning_date = meta_group['RANGEBEGINNINGDATE']['VALUE']
        beginning_time = meta_group['RANGEBEGINNINGTIME']['VALUE']
        end_date = meta_group['RANGEENDINGDATE']['VALUE']
        end_time = meta_group['RANGEENDINGTIME']['VALUE']
        self.ts_start = datetime.datetime.strptime(beginning_date+beginning_time, '"%Y-%m-%d""%H:%M:%S.%f"')
        self.ts_end = datetime.datetime.strptime(end_date+end_time, '"%Y-%m-%d""%H:%M:%S.%f"')

    def read_dataset(self, dataset_name, resample_factor=None):
        ds = self.hdf.select(dataset_name)
        data = ds.get()
        if resample_factor is not None:
            data = self.resample(data=data, factor=resample_factor)

        attributes = ds.attributes()

        if '_FillValue' in attributes.keys():
            fill_value = attributes['_FillValue']
            mask = data == fill_value
            data = numpy.ma.array(data, mask=mask)

        if 'scale_factor' in attributes.keys():
            scale_factor = attributes['scale_factor']
            if scale_factor < 1:
                # This is insane
                data = data * scale_factor
            else:
                data = data / scale_factor

        self.data[dataset_name] = data

    def resample(self, data, factor):
        data = data.repeat(factor, axis=0)
        data = data.repeat(factor, axis=1)
        return data


class Mod09(Modis):
    def __init__(self, file_path, sidecar_path=None, nom_res=None):
        super(Mod09, self).__init__(file_path, sidecar_path)
        if nom_res is None:
            self.nom_res = '1km'
        else:
            self.nom_res = nom_res

    def read_data(self):
        if self.nom_res == '1km':
            self.read_data_1km()
        elif self.nom_res == '500m':
            self.read_data_500m()
        elif self.nom_res == '250m':
            self.read_data_250m()

    def read_data_1km(self):
        datasets = dict(filter(lambda elem: '1km' in elem[0], self.hdf.datasets().items())).keys()
        for dataset_name in datasets:
            if self.nom_res == '500m':
                resample_factor = 2
            else:
                resample_factor = None
            self.read_dataset(dataset_name, resample_factor=resample_factor)

    def read_data_500m(self):
        datasets = dict(filter(lambda elem: '500m' in elem[0], self.hdf.datasets().items())).keys()
        for dataset_name in datasets:
            self.read_dataset(dataset_name=dataset_name, resample_factor=None)

    def read_data_250m(self):
        datasets = dict(filter(lambda elem: '250m' in elem[0], self.hdf.datasets().items())).keys()
        for dataset_name in datasets:
            self.read_dataset(dataset_name=dataset_name, resample_factor=None)


class Mod03(Modis):
    def __init__(self, file_path, sidecar_path=None, nom_res=None):
        super(Mod03, self).__init__(file_path, sidecar_path)
        if nom_res is None:
            self.nom_res = '1km'
        else:
            self.nom_res = nom_res

    def read_data(self):
        self.read_data1km()

    def read_data1km(self):
        dataset_names = ['SensorAzimuth', 'SensorZenith', 'SolarAzimuth', 'SolarZenith']
        for dataset_name in dataset_names:
            if self.nom_res == '500m':
                resample_factor = 2
            else:
                resample_factor = None
            self.read_dataset(dataset_name=dataset_name, resample_factor=resample_factor)


class Mod05(Modis):

    def __init__(self, file_path, sidecar_path=None):
        super(Mod05, self).__init__(file_path, sidecar_path)
        self.nom_res = '5km'
        
    def read_data(self):
        dataset_names = ['Scan_Start_Time', 'Solar_Zenith', 'Solar_Azimuth', 
                         'Sensor_Zenith', 'Sensor_Azimuth', 'Water_Vapor_Infrared']
    
        dataset_names2 = ['Cloud_Mask_QA', 'Water_Vapor_Near_Infrared', 
                          'Water_Vaport_Corretion_Factors', 'Quality_Assurance_Near_Infrared',
                          'Quality_Assurance_Infrared']

        for dataset_name in dataset_names:
            self.read_dataset(dataset_name=dataset_name, resample_factor=None)


class Mod09GA(Modis):
    def read_latlon(self):
        pass

    def read_data(self):
        dataset_names = ['sur_refl_b01_1', 'sur_refl_b02_1', 'sur_refl_b03_1', 'sur_refl_b04_1', 'sur_refl_b05_1',
                          'sur_refl_b06_1', 'sur_refl_b07_1', 'QC_500m_1', 'obscov_500m_1']
        for dataset_name in dataset_names:
            self.read_dataset(dataset_name)
