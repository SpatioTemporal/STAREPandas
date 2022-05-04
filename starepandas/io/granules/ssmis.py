from starepandas.io.granules.granule import Granule
import starepandas.io.s3
import datetime
import numpy
import pystare


class SSMIS(Granule):
    
    def __init__(self, file_path, sidecar_path=None):                
        super().__init__(file_path, sidecar_path)
        self.netcdf = starepandas.io.s3.nc4_dataset_wrapper(self.file_path, 'r', format='NETCDF4')
        self.lat = None
        self.lon = None
        self.timestamps = None
        self.data = {'S1': {}, 'S2': {}, 'S3': {}, 'S4': {}}
        self.stare = None
        self.nom_res = ''
        self.scans = ['S1', 'S2', 'S3', 'S4']

    def read_latlon(self):
        self.lat = {}
        self.lon = {}
        for scan in self.scans:
            self.lat[scan] = self.netcdf.groups[scan]['Latitude'][:].astype(numpy.double)
            self.lon[scan] = self.netcdf.groups[scan]['Longitude'][:].astype(numpy.double)

    def read_timestamp_scan(self, scan):
        year = self.netcdf.groups[scan]['ScanTime']['Year'][:]
        month = self.netcdf.groups[scan]['ScanTime']['Month'][:]
        day = self.netcdf.groups[scan]['ScanTime']['DayOfMonth'][:]
        hour = self.netcdf.groups[scan]['ScanTime']['Hour'][:]
        minute = self.netcdf.groups[scan]['ScanTime']['Minute'][:]
        second = self.netcdf.groups[scan]['ScanTime']['Second'][:]
        millisecond = self.netcdf.groups[scan]['ScanTime']['MilliSecond'][:]

        timestamps = []
        for d in zip(year, month, day, hour, minute, second, millisecond):
            if d[5] == 60:
                # Someone made the decision to have minutes begin at 00:001 and end at 60:00 so we need to catch this.
                ts = datetime.datetime(d[0], d[1], d[2], d[3], d[4], 0, d[6])
                ts += datetime.timedelta(minutes=1)
                timestamps.append(ts)
            else:
                timestamps.append(datetime.datetime(*d))
        return numpy.array(timestamps)
            
    def read_timestamps(self):
        self.timestamps = {}
        ts = self.read_timestamp_scan('S1')
        ts_len = ts.shape[0]
        ts = numpy.repeat(ts, 90)
        ts = ts.reshape(ts_len, 90)
        self.timestamps['S1'] = ts

        ts = self.read_timestamp_scan('S2')
        ts_len = ts.shape[0]
        ts = numpy.repeat(ts, 90)
        ts = ts.reshape(ts_len, 90)
        self.timestamps['S2'] = ts

        ts = self.read_timestamp_scan('S3')
        ts_len = ts.shape[0]
        ts = numpy.repeat(ts, 180)
        ts = ts.reshape(ts_len, 180)
        self.timestamps['S3'] = ts

        ts = self.read_timestamp_scan('S4')
        ts_len = ts.shape[0]
        ts = numpy.repeat(ts, 180)
        ts = ts.reshape(ts_len, 180)
        self.timestamps['S4'] = ts

    def read_data(self):
        self.data['S1']['Tc1'] = self.netcdf.groups['S1']['Tc'][:, :, 0]
        self.data['S1']['Tc2'] = self.netcdf.groups['S1']['Tc'][:, :, 1]
        self.data['S1']['Tc3'] = self.netcdf.groups['S1']['Tc'][:, :, 2]

        self.data['S2']['Tc1'] = self.netcdf.groups['S2']['Tc'][:, :, 0]
        self.data['S2']['Tc2'] = self.netcdf.groups['S2']['Tc'][:, :, 1]

        self.data['S3']['Tc1'] = self.netcdf.groups['S3']['Tc'][:, :, 0]
        self.data['S3']['Tc2'] = self.netcdf.groups['S3']['Tc'][:, :, 1]
        self.data['S3']['Tc3'] = self.netcdf.groups['S3']['Tc'][:, :, 2]
        self.data['S3']['Tc4'] = self.netcdf.groups['S3']['Tc'][:, :, 3]

        self.data['S4']['Tc1'] = self.netcdf.groups['S4']['Tc'][:, :, 0]
        self.data['S4']['Tc2'] = self.netcdf.groups['S4']['Tc'][:, :, 1]

        self.read_timestamps()

    def add_sids(self, adapt_resolution=True):
        self.stare = {}
        for scan in self.scans:
            self.stare[scan] = pystare.from_latlon2D(lat=self.lat[scan], lon=self.lon[scan],
                                                     adapt_resolution=adapt_resolution)

    def read_sidecar_index(self, sidecar_path=None):
        self.stare = {}
        if not sidecar_path:
            sidecar_path = self.sidecar_path
        ds = starepandas.io.s3.nc4_dataset_wrapper(sidecar_path)
        for scan in self.scans:
            self.stare[scan] = ds[scan]['STARE_index'][:, :].astype(numpy.int64)

    def read_sidecar_cover(self, sidecar_path=None):
        if not sidecar_path:
            sidecar_path = self.sidecar_path
        ds = starepandas.io.s3.nc4_dataset_wrapper(sidecar_path)
        self.stare_cover = ds['STARE_cover'][:].astype(numpy.int64)

    def to_df(self):
        dfs = {}
        for scan in self.scans:
            df = dict()
            if self.lat is not None:
                df['lat'] = self.lat[scan].flatten()
                df['lon'] = self.lon[scan].flatten()
            if self.stare is not None:
                df['stare'] = self.stare[scan].flatten()
            df['timestamp'] = self.timestamps[scan].flatten()
            for key in self.data[scan].keys():
                df[key] = self.data[scan][key].flatten()
            df = starepandas.STAREDataFrame(df)
            dfs[scan] = df
        return dfs


