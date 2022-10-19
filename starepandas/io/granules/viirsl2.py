from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import numpy
from starepandas.io.granules.modis import Modis
import datetime
import pystare


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

        """

        self.data['land_water_mask'] = self.netcdf.groups['geolocation_data']['land_water_mask'][:]
        self.data['quality_flag'] = self.netcdf.groups['geolocation_data']['quality_flag'][:]

        self.data['sensor_azimuth'] = self.netcdf.groups['geolocation_data']['sensor_azimuth'][:]
        self.data['sensor_zenith'] = self.netcdf.groups['geolocation_data']['sensor_zenith'][:]
        self.data['solar_azimuth'] = self.netcdf.groups['geolocation_data']['solar_azimuth'][:]
        self.data['solar_zenith'] = self.netcdf.groups['geolocation_data']['solar_zenith'][:]

        self.read_latlon()
        #self.read_timestamps()


class VNP09(Modis):

    def __init__(self, file_path, sidecar_path=None, nom_res=None):
        super().__init__(file_path, sidecar_path)

        if nom_res is None:
            self.nom_res = '750m'
        else:
            self.nom_res = nom_res

    def read_data(self):
        if self.nom_res == '750m':
            self.read_data_750m()
        elif self.nom_res == '375m':
            self.read_data_375km()

    def read_data_750m(self):
        datasets = dict(filter(lambda elem: '750m' in elem[0], self.hdf.datasets().items())).keys()
        for dataset_name in datasets:
            if self.nom_res == '375m':
                resample_factor = 2
            else:
                resample_factor = None
            self.read_dataset(dataset_name, resample_factor=resample_factor)

        self.read_dataset('QF1 Surface Reflectance', resample_factor=resample_factor)
        self.read_dataset('QF2 Surface Reflectance', resample_factor=resample_factor)

    def read_data_375m(self):
        datasets = dict(filter(lambda elem: '375m' in elem[0], self.hdf.datasets().items())).keys()
        for dataset_name in datasets:
            self.read_dataset(dataset_name=dataset_name, resample_factor=None)

    def read_dataset(self, dataset_name, resample_factor=None):
        ds = self.hdf.select(dataset_name)
        data = ds.get()
        dtype = data.dtype
        if resample_factor is not None:
            data = self.resample(array=data, factor=resample_factor)

        attributes = ds.attributes()

        if 'FILL_VALUES' in attributes.keys():
            #fill_values = attributes['FILL_VALUES']
            mask = data < 0
            data = numpy.ma.array(data, mask=mask)

        if 'Scale' in attributes.keys():
            # why would you name it "Scale" in VNP09 and "scal_factor" in MOD09. Also ... those are floats.
            scale_factor = attributes['Scale']
            scale_factor = 10000
            if scale_factor < 1:
                # This is insane
                data = data * scale_factor
            else:
                data = data / scale_factor

        self.data[dataset_name] = data

    def read_timestamps(self):
        self.ts_start = self.hdf.attributes()['StartTime']
        self.ts_end = self.hdf.attributes()['EndTime']
        try:
            self.ts_start = datetime.datetime.strptime(self.ts_start, '%Y-%m-%d %H:%M:%S.%f')
            self.ts_end = datetime.datetime.strptime(self.ts_end, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.ts_start = datetime.datetime.strptime(self.ts_start, '%Y-%m-%d %H:%M:%S')
            self.ts_end = datetime.datetime.strptime(self.ts_end, '%Y-%m-%d %H:%M:%S')


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


def decode_qf1(qf):
    """
    Bits are listed from the MSB (bit 7) to the LSB (bit 0):
    6-7    SUN GLINT
       00 -- none
       01 -- geometry based
       10 -- wind speed based
       11 -- geometry & wind speed based
    5      low sun mask
       0 -- high
       1 -- low
    4      day/night
       0 -- day\n\t
       1 -- night\n\t
    2-3    cloud detection & confidence
       00 -- confident clear
       01 -- probably clear
       10 -- probably cloudy
       11 -- confident cloudy
    0-1    cloud mask quality
       00 -- poor\n\t
       01 -- low\n\t
       10 -- medium\n\t
       11 -- high\n";
    """
    qf = qf.apply(lambda x: '{:08b}'.format(x))
    df = starepandas.STAREDataFrame(index=qf.index)
    df['cloud_mask_quality'] = qf.str.slice(start=-2)
    df['cloud'] = qf.str.slice(start=-4, stop=-2)
    return df


def decode_qf2(qf):
    """
    Bits are listed from the MSB (bit 7) to the LSB (bit 0):\
    7      thin cirrus emissive;
           0 -- no cloud
           1 -- cloud
    6      thin cirrus reflective
           0 -- no cloud\n\t
           1 -- cloud\n\t
    5      snow/ice
           0 -- no snow/ice\n\t
           1 -- snow or ice\n\t
    4      heavy aerosol mask
           0 -- no heavy aerosol\n\t
           1 -- heavy aerosol\n\t
    3      shadow mask
           0 -- no cloud shadow\n\t
           1 -- shadow\n\t
    0-2    land/water background
           000 -- land & desert\n\t
           001 -- land no desert\n\t
           010 -- inland water\n\t
           011 -- sea water\n\t
           101 -- coastal\n";
    """
    qf = qf.apply(lambda x: '{:08b}'.format(x))
    df = starepandas.STAREDataFrame(index=qf.index)
    df['land_water'] = qf.str.slice(start=-3)
    df['shadow'] = qf.str.slice(start=-3, stop=-4).astype(bool)
    return df


def read_vnp09(file_path, roi_sids):
    # Read the MOD09
    vnp09 = VNP09(file_path, nom_res='750m')
    vnp09.read_data_750m()
    vnp09.read_timestamps()

    # Getting the geolocation info for Sensor and
    vnp03_path = vnp09.guess_companion_path(prefix='VNP03')
    vnp03 = starepandas.io.granules.VNP03MOD(vnp03_path)
    vnp03.read_sidecar_index()
    vnp03.read_sidecar_latlon()
    vnp03.read_data()

    # Converting to DF and joining
    vnp09 = vnp09.to_df(xy=True)
    vnp03 = vnp03.to_df()
    vnp09 = vnp09.join(vnp03)
    vnp09.dropna(inplace=True)
    vnp09.sids = vnp09.sids.astype('int64')

    qf1 = starepandas.io.granules.viirsl2.decode_qf1(vnp09['QF1 Surface Reflectance'])
    qf2 = starepandas.io.granules.viirsl2.decode_qf2(vnp09['QF2 Surface Reflectance'])
    vnp09 = vnp09.join(qf1).join(qf2)

    # Subsetting
    try:
        vnp09 = starepandas.speedy_subset(vnp09, roi_sids)
    except:
        print(file_path)
        raise Exception

    # Adding the lower level SIDS
    vnp09['sids14'] = vnp09.to_stare_level(14, clear_to_level=True).sids
    vnp09['sids15'] = vnp09.to_stare_level(15, clear_to_level=True).sids
    vnp09['sids16'] = vnp09.to_stare_level(16, clear_to_level=True).sids
    vnp09['sids17'] = vnp09.to_stare_level(17, clear_to_level=True).sids
    vnp09['sids18'] = vnp09.to_stare_level(18, clear_to_level=True).sids

    r = 6371007.181
    vnp09['area'] = pystare.to_area(vnp09['sids']) * r ** 2 / 1000 / 1000
    vnp09['level'] = pystare.spatial_resolution(vnp09['sids'])

    # Converting types
    vnp09.reset_index(inplace=True, drop=True)
    return vnp09
