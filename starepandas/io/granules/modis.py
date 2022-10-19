from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import dask
import datetime
import numpy
import pystare
import shapely
import matplotlib.patches
import pyproj


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
            lines.append(key.strip() + '=' + value.strip())
        else:
            lines.append(line)
    i = -1
    while i < (len(lines)) - 1:
        i += 1
        line = lines[i]
        if "=" in line:
            key = line.split('=')[0]
            value = '='.join(line.split('=')[1:])
            if key in ['GROUP', 'OBJECT']:
                endIdx = lines[i + 1:].index('END_{}={}'.format(key, value))
                endIdx += i + 1
                out[value] = parse_hdfeos_metadata("\n".join(lines[i + 1:endIdx]))
                i = endIdx
            elif ('END_GROUP' not in key) and ('END_OBJECT' not in key):
                out[key] = str(value)
    return out


class Modis(Granule):

    def __init__(self, file_path, sidecar_path=None, nom_res=None):
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
        try:
            self.ts_start = datetime.datetime.strptime(beginning_date + beginning_time, '"%Y-%m-%d""%H:%M:%S.%f"')
            self.ts_end = datetime.datetime.strptime(end_date + end_time, '"%Y-%m-%d""%H:%M:%S.%f"')
        except ValueError:
            self.ts_start = datetime.datetime.strptime(beginning_date + beginning_time, '"%Y-%m-%d""%H:%M:%S"')
            self.ts_end = datetime.datetime.strptime(end_date + end_time, '"%Y-%m-%d""%H:%M:%S"')

    def read_dataset(self, dataset_name, resample_factor=None):
        ds = self.hdf.select(dataset_name)
        data = ds.get()
        dtype = data.dtype
        if resample_factor is not None:
            data = self.resample(array=data, factor=resample_factor)

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

    def resample(self, array, factor):
        array = array.repeat(factor, axis=0)
        array = array.repeat(factor, axis=1)
        return array

    def decode_band_quality(self, qa_name):
        """
        Decode QA

        31      adjacency correction performed      1: yes; 0: no
        30      atmospheric correction performed    1: yes; 0: no
        26-29   band 7 data quality four bit range  0000: highest quality; 0111: noisy detector;
            1000: dead detector, data interpolated in L1B;
            1001: solar zenith >= 86 degrees; 1010: solar zenith >= 85 and < 86 degrees; 1011: missing input;
            1100: internal constant used in place of climatological data for at least one atmospheric constant
            1101: correction out of bounds pixel constrained to extreme allowable value
            1110: L1B data faulty; 1111: not processed due to deep ocean or clouds
        22-25   band 6 data quality four bit range;    SAME AS ABOVE
        18-21   band 5 data quality four bit range;    SAME AS ABOVE
        14-17   band 4 data quality four bit range;    SAME AS ABOVE
        10-13   band 3 data quality four bit range;    SAME AS ABOVE
        6-9     band 2 data quality four bit range;    SAME AS ABOVE
        2-5     band 1 data quality four bit range;    SAME AS ABOVE
        0-1     MODLAND QA bits
            00: ideal quality all bands;
            01: less than ideal quality some or all bands corrected product not produced due to;
            10: cloud effects all bands;
            11: other reasons some or all bands may be fill value
        Note that a value of (11) overrides a value of (01).";
        """
        qa = self.data[qa_name]
        # qa = state_series.apply(lambda x: '{:032b}'.format(x)[::-1])
        # df = starepandas.STAREDataFrame(index=state.index)
        # df['modland_qa'] = state.str.slice(start=0, stop=2).apply(lambda x: x[::-1]).astype('u1')

    def decode_state(self, state_ds):
        """
        Decode the state

        15  internal snow algorithm flag     1: yes; 0: no
        14  Salt pan                         1: yes; 0: no
        13  Pixel is adjacent to cloud       1: yes; 0: no
        12  MOD35 snow/ice flag              1: yes; 0: no
        11  internal fire algorithm flag     1: fire; 0: no fire
        10  internal cloud algorithm flag    1: cloud; 0: no cloud
        8-9 cirrus detected                 00: none; 01: small; 10: average; 11: high
        6-7 aerosol quantity                00: climatology; 01: low; 10: average; 11:  high
        3-5 land/water flag                000: shallow ocean; 001 land; 010: ocean coastlines and lake shorelines
                                           011: shallow inland water; 100: ephemeral water; 101: deep inland water
                                           110: continental/moderate ocean; 111: deep ocean
        2   cloud shadow                     1: yes; 0: no
        0-1 cloud state                     00: clear; 01: cloudy; 10: mixed; 11: not set assumed clear
        """
        state = self.data[state_ds]
        binary_repr = numpy.vectorize(numpy.binary_repr)
        state = binary_repr(state, width=16)

        def slicer_vectorized(a, start, end):
            b = a.data.view((str, 1))
            # need to flip because of major byte order
            b = numpy.flip(b)
            b = b.reshape(a.shape[0], a.shape[1], 16)
            b = b[:, :, start:end]
            b = numpy.flip(b)
            b = numpy.frombuffer(b.tobytes(), dtype=(str, end - start))
            b = b.reshape(a.shape)
            b = numpy.ma.masked_array(b, a.mask)
            b = b.astype('i1')
            return b

        self.data['cloud'] = slicer_vectorized(state, 0, 2)
        self.data['cloud_shadow'] = slicer_vectorized(state, 2, 3).astype(bool)
        self.data['cloud_internal'] = slicer_vectorized(state, 10, 11).astype(bool)
        self.data['snow_mod35'] = slicer_vectorized(state, 12, 13).astype(bool)
        self.data['snow_internal'] = slicer_vectorized(state, 15, 16).astype(bool)


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
    def __init__(self, file_path, sidecar_path=None):
        super(Mod09GA, self).__init__(file_path, sidecar_path)
        self.nom_res = '500m'

    def read_latlon(self):
        pass

    def read_data(self):
        # Note: those are the 500 m observations!
        dataset_names = ['sur_refl_b01_1', 'sur_refl_b02_1', 'sur_refl_b03_1', 'sur_refl_b04_1', 'sur_refl_b05_1',
                         'sur_refl_b06_1', 'sur_refl_b07_1', 'QC_500m_1', 'obscov_500m_1']
        for dataset_name in dataset_names:
            self.read_dataset(dataset_name)


def decode_state(state_series):
    """
    Decode the state

    15  internal snow algorithm flag     1: yes; 0: no
    14  Salt pan                         1: yes; 0: no
    13  Pixel is adjacent to cloud       1: yes; 0: no
    12  MOD35 snow/ice flag              1: yes; 0: no
    11  internal fire algorithm flag     1: fire; 0: no fire
    10  internal cloud algorithm flag    1: cloud; 0: no cloud
    8-9 cirrus detected                 00: none; 01: small; 10: average; 11: high
    6-7 aerosol quantity                00: climatology; 01: low; 10: average; 11:  high
    3-5 land/water flag                000: shallow ocean;
                                       001 land;
                                       010: ocean coastlines and lake shorelines
                                       011: shallow inland water;
                                       100: ephemeral water;
                                       101: deep inland water
                                       110: continental/moderate ocean;
                                       111: deep ocean
    2   cloud shadow                     1: yes; 0: no
    0-1 cloud state                     00: clear; 01: cloudy; 10: mixed; 11: not set assumed clear
    """

    state = state_series.apply(lambda x: '{:016b}'.format(x)[::-1])
    df = starepandas.STAREDataFrame(index=state.index)
    df['cloud'] = state.str.slice(start=0, stop=2).apply(lambda x: x[::-1])
    df['cloud_shadow'] = state.str.slice(start=2, stop=3).astype('u1').astype(bool)
    df['cloud_internal'] = state.str.slice(start=10, stop=11).astype('u1').astype(bool)
    df['snow_mod35'] = state.str.slice(start=12, stop=13).astype('u1').astype(bool)
    df['snow_internal'] = state.str.slice(start=15, stop=16).astype('u1').astype(bool)
    return df


def decode_qa(qa_series):
    """
        31      adjacency correction performed      1: yes; 0: no
        30      atmospheric correction performed    1: yes; 0: no
        26-29   band 7 data quality four bit range
            0000: highest quality;
            0111: noisy detector;
            1000: dead detector, data interpolated in L1B;
            1001: solar zenith >= 86 degrees;
            1010: solar zenith >= 85 and < 86 degrees;
            1011: missing input;
            1100: internal constant used in place of climatological data for at least one atmospheric constant
            1101: correction out of bounds pixel constrained to extreme allowable value
            1110: L1B data faulty; 1111: not processed due to deep ocean or clouds
        22-25   band 6 data quality four bit range;    SAME AS ABOVE
        18-21   band 5 data quality four bit range;    SAME AS ABOVE
        14-17   band 4 data quality four bit range;    SAME AS ABOVE
        10-13   band 3 data quality four bit range;    SAME AS ABOVE
        6-9     band 2 data quality four bit range;    SAME AS ABOVE
        2-5     band 1 data quality four bit range;    SAME AS ABOVE
        0-1     MODLAND QA bits
            0: ideal quality all bands;
            1: less than ideal quality some or all bands corrected product not produced due to;
            2: cloud effects all bands;
            3: other reasons some or all bands may be fill value
        Note that a value of (11) overrides a value of (01).";
    """
    qa = qa_series[qa_series.isna() == False]
    qa = qa.apply(lambda x: '{:032b}'.format(x)[::-1])
    qa = qa.str.slice(start=0, stop=2)
    qa = qa.apply(lambda x: x[::-1]).astype('u1')
    qa = qa.rename('modland')
    return qa


def read_mod09(file_path, roi_sids):
    # Read the MOD09
    mod09 = starepandas.io.granules.Mod09(file_path, nom_res='500m')
    mod09.read_data_500m()
    mod09.read_sidecar_index()
    mod09.read_sidecar_latlon()
    mod09.read_timestamps()

    # Adding the QA State flag
    ds_name = '1km Reflectance Data State QA'
    mod09.read_dataset(ds_name, resample_factor=2)
    states = decode_state(mod09['ds_name'])
    mod09 = mod09.join(states)

    # Gettin the geolocation info for Sensor and
    mod03_path = mod09.guess_companion_path(prefix='MOD03')
    mod03 = starepandas.io.granules.Mod03(mod03_path, nom_res='500m')
    mod03.read_data()

    # Converting to DF and joining
    mod09 = mod09.to_df(xy=True)
    mod03 = mod03.to_df()
    mod09 = mod09.join(mod03)

    mod09.sids = mod09.sids.astype('int64')

    # Subsetting
    try:
        mod09 = starepandas.speedy_subset(mod09, roi_sids)
    except:
        print(file_path)
        raise Exception

    # Adding the lower level SIDS
    mod09['sids14'] = mod09.to_stare_level(14, clear_to_level=True).sids
    mod09['sids15'] = mod09.to_stare_level(15, clear_to_level=True).sids
    mod09['sids16'] = mod09.to_stare_level(16, clear_to_level=True).sids
    mod09['sids17'] = mod09.to_stare_level(17, clear_to_level=True).sids
    mod09['sids18'] = mod09.to_stare_level(18, clear_to_level=True).sids

    r = 6371007.181
    mod09['area'] = pystare.to_area(mod09['sids']) * r ** 2 / 1000 / 1000
    mod09['level'] = pystare.spatial_resolution(mod09['sids'])

    # Converting types
    mod09.reset_index(inplace=True, drop=True)
    return mod09


def read_mod09ga(file_path, bbox=None):
    mod09ga = starepandas.io.granules.Mod09GA(file_path)
    mod09ga.read_data()
    mod09ga.read_timestamps()

    state_name = 'state_1km_1'
    mod09ga.read_dataset(state_name, resample_factor=2)
    mod09ga = mod09ga.to_df(xy=True)
    mod09ga = mod09ga.rename(columns={"x": "y", "y": "x"})

    if bbox:
        x_min = bbox[0]
        x_max = bbox[1]
        y_min = bbox[2]
        y_max = bbox[3]
        mod09ga = mod09ga[(mod09ga.x >= x_min) & (mod09ga.x <= x_max) & (mod09ga.y >= y_min) & (mod09ga.y <= y_max)]

    mod09ga = mod09ga.dropna(axis=0, how='any')
    if mod09ga.empty:
        return mod09ga

    states = decode_state(mod09ga[state_name])
    mod09ga = mod09ga.join(states)
    return mod09ga


def zenith2width(zenith):
    a = 0.09148844
    b = 40.88432179
    c = 1
    return numpy.exp(a * zenith) / b + c


def zenith2height(zenith):
    a = 0.06510478
    b = 35.23555162
    c = 0.97762256
    return numpy.exp(a * zenith) / b + c


def transform(geom, from_epsg, to_epsg):
    from_crs = pyproj.CRS(from_epsg)
    to_crs = pyproj.CRS(to_epsg)

    project = pyproj.Transformer.from_crs(from_crs, to_crs, always_xy=True).transform
    return shapely.ops.transform(project, geom)


def make_ellipse(point, crs, width, height, angle):
    transformed = transform(point, 4326, crs)
    center_x = transformed.x
    center_y = transformed.y
    ellipse = matplotlib.patches.Ellipse((center_x, center_y), width, height, angle)
    vertices = ellipse.get_verts()  # get the vertices from the ellipse object
    ellipse = shapely.geometry.LinearRing(vertices)
    ellipse = transform(ellipse, crs, 4326)
    return ellipse


def make_ellipse_sids(df, crs=3857, n_partitions=None, num_workers=None, level=17, modis_resolution=500):
    """ Create a series of sids corresponding to the STARE cover of an ellipse around the center of
    each iFOV.

    """
    if num_workers is not None and n_partitions is None:
        n_partitions = num_workers * 10
    elif num_workers is None and n_partitions is None:
        n_partitions = 1
        num_workers = 1

    if len(df) <= 1:
        n_partitions = 1
    elif n_partitions >= len(df):
        # Cannot have more partitions than rows
        n_partitions = len(df) - 1

    if n_partitions == 1:
        ellipses_sids = []
        for idx, row in df.iterrows():
            if 'geometry' in row.keys():
                point = row['geometry']
            elif 'longitude' in row.keys() and 'latitude' in row.keys():
                point = shapely.geometry.Point(row['longitude'], row['latitude'])
            elif 'sids' in row.keys():
                lat, lon = pystare.to_latlon([row['sids']])
                point = shapely.geometry.Point(lon, lat)
            else:
                raise

            azimuth = row['SensorAzimuth']
            zenith = row['SensorZenith']

            width = zenith2width(zenith) * modis_resolution
            height = zenith2height(zenith) * modis_resolution

            angle = 90 - azimuth
            ellipse = make_ellipse(point, crs, width, height, angle)

            ellipse_sids = starepandas.sids_from_ring(ring=ellipse, level=level)
            ellipses_sids.append(ellipse_sids)
        ellipses_sids = numpy.array(ellipses_sids, dtype=object)
    else:
        ddf = dask.dataframe.from_pandas(df, npartitions=n_partitions)
        meta = {'sids': 'int64'}
        res = ddf.map_partitions(make_ellipse_sids, crs=crs, n_partitions=1, num_workers=1,
                                 level=level, modis_resolution=modis_resolution, meta=meta)
        ellipses_sids = res.compute(scheduler='processes', num_workers=num_workers)
        ellipses_sids = list(ellipses_sids)

    return ellipses_sids
