from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import numpy


class VIIRSL2(Granule):
    
    def __init__(self, file_path, sidecar_path=None):
        super(VIIRSL2, self).__init__(file_path, sidecar_path)
        self.nom_res = '750m'
        self.netcdf = starepandas.io.s3.nc4_dataset_wrapper(self.file_path, 'r', format='NETCDF4')
    
    def read_timestamps(self):
        self.ts_start = self.netcdf.time_coverage_start
        self.ts_end = self.netcdf.time_coverage_end       
    
    def read_latlon(self):        
        self.lat = self.netcdf.groups['geolocation_data']['latitude'][:].astype(numpy.double)
        self.lon = self.netcdf.groups['geolocation_data']['longitude'][:].astype(numpy.double)


class CLDMSKL2VIIRS(VIIRSL2):
    
    def __init__(self, file_path, sidecar_path=None):
        super(CLDMSKL2VIIRS, self).__init__(file_path, sidecar_path)
        
    def read_data(self):
        """
        reads the data from a CLDMSKL2VIIRS granule into the self.data dictionary.
        Only the Integer_Cloud_Mask is read. The values are to be interpreted as:
        0 = cloudy, 1 = probably cloudy, 2 = probably clear, 3 = confident clear, -1 = no result

        :return: None
        """

        self.data['Integer_Cloud_Mask'] = self.netcdf.groups['geophysical_data']['Integer_Cloud_Mask'][:]

        # There appear to be 10 QA dimensions which are nowhere documented. Leaving this open for now
        # Cloud Mask QA (1km) Bit 1: 0 not useful 1 useful. Bit 2-7: confidence levels
        # self.data['Quality_Assurance'] = self.netcdf.groups['geophysical_data']['Quality_Assurance'][:]


class VNP03MOD(VIIRSL2):
    
    def __init__(self, file_path, sidecar_path=None):
        super().__init__(file_path, sidecar_path)        
        
    def read_data(self):
        """
        reads the data from a VNP03MOD granule into the self.data dictionary.
        Two variables are read:

        a) land_water_mask
        1: Shallow_Ocean 2: Land 3: Coastline 4: Shallow_Inland 5: Ephemeral 6: Deep_Inland 7: Continental 8: Deep_Ocean
        b) quality_flag
        1: Input_invalid 2: Pointing_bad 3: Terrain_bad
        :return: None
        """

        self.data['land_water_mask'] = self.netcdf.groups['geolocation_data']['land_water_mask'][:]
        self.data['quality_flag'] = self.netcdf.groups['geolocation_data']['quality_flag'][:]
    

class VNP03DNB(VIIRSL2):
    
    def __init__(self, file_path, sidecar_path=None):
        super().__init__(file_path, sidecar_path)
    
    def read_data(self):
        """
        reads the data from a VNP03DNB granule into the self.data dictionary.
        Three variables are read:

        a) moon_illumination_fraction
        b) land_water_mask
        1: Shallow_Ocean 2: Land 3: Coastline 4: Shallow_Inland 5: Ephemeral 6: Deep_Inland 7: Continental 8: Deep_Ocean
        c) quality_flag
        1: Input_invalid 2: Pointing_bad 3: Terrain_bad
        :return:
        """

        group = self.netcdf.groups['geolocation_data']
        self.data['moon_illumination_fraction'] = group['moon_illumination_fraction'][:]
        self.data['land_water_mask'] = group['land_water_mask'][:]
        self.data['quality_flag'] = group['quality_flag'][:]
                

class VNP02DNB(VIIRSL2):
    
    def __init__(self, file_path, sidecar_path=None):
        super(VNP02DNB, self).__init__(file_path, sidecar_path)
        self.companion_prefix = 'VNP03DNB'
            
    def read_data(self):
        dnb = self.netcdf.groups['observation_data']['DNB_observations'][:]
        quality_flags = self.netcdf.groups['observation_data']['DNB_quality_flags'][:]
        self.data['DNB_observations'] = dnb
        self.data['DNB_quality_flags'] = quality_flags                
        
    def read_latlon(self):
        pass

