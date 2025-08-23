from starepandas.io.granules.granule import Granule, Sidecar
import starepandas.io.s3
import datetime
import numpy
import pystare
import os
import netCDF4


class SSMIS(Granule):

    def __init__(self, file_path, sidecar_path=None, scans=['S1', 'S2', 'S3', 'S4']):
        super().__init__(file_path, sidecar_path)
        self.lat = None
        self.lon = None
        self.timestamps = {}
        self.data = {'S1': {}, 'S2': {}, 'S3': {}, 'S4': {}}
        self.stare = None
        self.nom_res = ''
        self.scans = scans
        self.header = {}
        self.file_type = self._determine_file_type()
        self.dataset = self._open_dataset()
        self.parse_header()

    def _determine_file_type(self):
        """Determine if the file is NetCDF4 or HDF5 based on file extension or content."""
        if self.file_path.lower().endswith('.h5') or self.file_path.lower().endswith('.hdf5'):
            return 'hdf5'
        elif self.file_path.lower().endswith('.nc') or self.file_path.lower().endswith('.nc4'):
            return 'netcdf4'
        else:
            # Try to determine by attempting to open as HDF5 first, then NetCDF4
            try:
                if 's3://' == self.file_path[0:5]:
                    s3 = starepandas.io.s3.parse_s3_url(self.file_path)
                    try:
                        import boto3
                        s3_client = boto3.client('s3')
                        test_file = starepandas.io.s3.h5_dataset_from_s3(
                            s3_client, s3['bucket_name'],
                            s3['prefix'] + s3['prefix_end'] + s3['resource']
                        )
                        test_file.close()
                        return 'hdf5'
                    except ImportError:
                        raise ImportError("boto3 is required for S3 HDF5 file access")
                else:
                    try:
                        import h5py
                        with h5py.File(self.file_path, 'r') as test_file:
                            pass
                        return 'hdf5'
                    except ImportError:
                        raise ImportError("h5py is required for HDF5 file access")
            except Exception:
                try:
                    if 's3://' == self.file_path[0:5]:
                        s3 = starepandas.io.s3.parse_s3_url(self.file_path)
                        try:
                            import boto3
                            s3_client = boto3.client('s3')
                            test_file = starepandas.io.s3.nc4_dataset_from_s3(
                                s3_client, s3['bucket_name'],
                                s3['prefix'] + s3['prefix_end'] + s3['resource']
                            )
                            test_file.close()
                            return 'netcdf4'
                        except ImportError:
                            raise ImportError("boto3 is required for S3 NetCDF4 file access")
                    else:
                        try:
                            import netCDF4
                            with netCDF4.Dataset(self.file_path, 'r') as test_file:
                                pass
                            return 'netcdf4'
                        except ImportError:
                            raise ImportError("netCDF4 is required for NetCDF4 file access")
                except Exception:
                    # Default to NetCDF4 if we can't determine
                    return 'netcdf4'

    def _open_dataset(self):
        """Open the dataset based on file type."""
        if self.file_type == 'hdf5':
            if 's3://' == self.file_path[0:5]:
                s3 = starepandas.io.s3.parse_s3_url(self.file_path)
                try:
                    import boto3
                    s3_client = boto3.client('s3')
                    return starepandas.io.s3.h5_dataset_from_s3(
                        s3_client, s3['bucket_name'],
                        s3['prefix'] + s3['prefix_end'] + s3['resource']
                    )
                except ImportError:
                    raise ImportError("boto3 is required for S3 HDF5 file access")
            else:
                try:
                    import h5py
                    return h5py.File(self.file_path, 'r')
                except ImportError:
                    raise ImportError("h5py is required for HDF5 file access")
        else:  # netcdf4
            return starepandas.io.s3.nc4_dataset_wrapper(self.file_path, 'r', format='NETCDF4')

    def parse_header(self):
        self.header = {}
        if self.file_type == 'hdf5':
            # For HDF5, try to get header from attributes
            if 'FileHeader' in self.dataset.attrs:
                header_str = self.dataset.attrs['FileHeader']
                # Handle bytes vs string conversion for HDF5
                if isinstance(header_str, bytes):
                    header_str = header_str.decode('utf-8')
                elif isinstance(header_str, numpy.ndarray):
                    header_str = str(header_str)
            else:
                # Try to find header in global attributes
                header_str = ""
                for key, value in self.dataset.attrs.items():
                    if 'header' in key.lower() or 'metadata' in key.lower():
                        if isinstance(value, bytes):
                            header_str = value.decode('utf-8')
                        else:
                            header_str = str(value)
                        break

                if not header_str:
                    # Create minimal header from available attributes
                    self.header = {}
                    for key, value in self.dataset.attrs.items():
                        if isinstance(value, bytes):
                            self.header[key] = value.decode('utf-8')
                        else:
                            self.header[key] = str(value)
                    self.ts_start = self.header.get('StartGranuleDateTime', '')
                    self.ts_end = self.header.get('StopGranuleDateTime', '')
                    return
        else:  # netcdf4
            try:
                header_str = self.dataset.FileHeader
            except AttributeError:
                # FileHeader attribute not found, create minimal header
                self.header = {}
                self.ts_start = ''
                self.ts_end = ''
                return

        # Parse header string
        if header_str:
            for h in header_str.replace(';', '').strip().split('\n'):
                if '=' in h:
                    key = h.split('=')[0]
                    value = h.split('=')[1]
                    self.header[key] = value
        self.ts_start = self.header.get('StartGranuleDateTime', '')
        self.ts_end = self.header.get('StopGranuleDateTime', '')

    def read_latlon(self):
        self.lat = {}
        self.lon = {}
        for scan in self.scans:
            if self.file_type == 'hdf5':
                self.lat[scan] = self.dataset[scan]['Latitude'][:].astype(numpy.double)
                self.lon[scan] = self.dataset[scan]['Longitude'][:].astype(numpy.double)
            else:  # netcdf4
                self.lat[scan] = self.dataset.groups[scan]['Latitude'][:].astype(numpy.double)
                self.lon[scan] = self.dataset.groups[scan]['Longitude'][:].astype(numpy.double)

    def read_timestamp_scan(self, scan):
        if self.file_type == 'hdf5':
            year = self.dataset[scan]['ScanTime']['Year'][:]
            month = self.dataset[scan]['ScanTime']['Month'][:]
            day = self.dataset[scan]['ScanTime']['DayOfMonth'][:]
            hour = self.dataset[scan]['ScanTime']['Hour'][:]
            minute = self.dataset[scan]['ScanTime']['Minute'][:]
            second = self.dataset[scan]['ScanTime']['Second'][:]
            millisecond = self.dataset[scan]['ScanTime']['MilliSecond'][:]
        else:  # netcdf4
            year = self.dataset.groups[scan]['ScanTime']['Year'][:]
            month = self.dataset.groups[scan]['ScanTime']['Month'][:]
            day = self.dataset.groups[scan]['ScanTime']['DayOfMonth'][:]
            hour = self.dataset.groups[scan]['ScanTime']['Hour'][:]
            minute = self.dataset.groups[scan]['ScanTime']['Minute'][:]
            second = self.dataset.groups[scan]['ScanTime']['Second'][:]
            millisecond = self.dataset.groups[scan]['ScanTime']['MilliSecond'][:]

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
        if 'S1' in self.scans:
            ts = self.read_timestamp_scan('S1')
            ts_len = ts.shape[0]
            ts = numpy.repeat(ts, 90)
            ts = ts.reshape(ts_len, 90)
            self.timestamps['S1'] = ts

        if 'S2' in self.scans:
            ts = self.read_timestamp_scan('S2')
            ts_len = ts.shape[0]
            ts = numpy.repeat(ts, 90)
            ts = ts.reshape(ts_len, 90)
            self.timestamps['S2'] = ts

        if 'S3' in self.scans:
            ts = self.read_timestamp_scan('S3')
            ts_len = ts.shape[0]
            ts = numpy.repeat(ts, 180)
            ts = ts.reshape(ts_len, 180)
            self.timestamps['S3'] = ts

        if 'S4' in self.scans:
            ts = self.read_timestamp_scan('S4')
            ts_len = ts.shape[0]
            ts = numpy.repeat(ts, 180)
            ts = ts.reshape(ts_len, 180)
            self.timestamps['S4'] = ts

    def read_data(self):
        if 'S1' in self.scans:
            if self.file_type == 'hdf5':
                self.data['S1']['Tc1'] = self.dataset['S1']['Tc'][:, :, 0]
                self.data['S1']['Tc2'] = self.dataset['S1']['Tc'][:, :, 1]
                self.data['S1']['Tc3'] = self.dataset['S1']['Tc'][:, :, 2]
            else:  # netcdf4
                self.data['S1']['Tc1'] = self.dataset.groups['S1']['Tc'][:, :, 0]
                self.data['S1']['Tc2'] = self.dataset.groups['S1']['Tc'][:, :, 1]
                self.data['S1']['Tc3'] = self.dataset.groups['S1']['Tc'][:, :, 2]

        if 'S2' in self.scans:
            if self.file_type == 'hdf5':
                self.data['S2']['Tc1'] = self.dataset['S2']['Tc'][:, :, 0]
                self.data['S2']['Tc2'] = self.dataset['S2']['Tc'][:, :, 1]
            else:  # netcdf4
                self.data['S2']['Tc1'] = self.dataset.groups['S2']['Tc'][:, :, 0]
                self.data['S2']['Tc2'] = self.dataset.groups['S2']['Tc'][:, :, 1]

        if 'S3' in self.scans:
            if self.file_type == 'hdf5':
                self.data['S3']['Tc1'] = self.dataset['S3']['Tc'][:, :, 0]
                self.data['S3']['Tc2'] = self.dataset['S3']['Tc'][:, :, 1]
                self.data['S3']['Tc3'] = self.dataset['S3']['Tc'][:, :, 2]
                self.data['S3']['Tc4'] = self.dataset['S3']['Tc'][:, :, 3]
            else:  # netcdf4
                self.data['S3']['Tc1'] = self.dataset.groups['S3']['Tc'][:, :, 0]
                self.data['S3']['Tc2'] = self.dataset.groups['S3']['Tc'][:, :, 1]
                self.data['S3']['Tc3'] = self.dataset.groups['S3']['Tc'][:, :, 2]
                self.data['S3']['Tc4'] = self.dataset.groups['S3']['Tc'][:, :, 3]

        if 'S4' in self.scans:
            if self.file_type == 'hdf5':
                self.data['S4']['Tc1'] = self.dataset['S4']['Tc'][:, :, 0]
                self.data['S4']['Tc2'] = self.dataset['S4']['Tc'][:, :, 1]
            else:  # netcdf4
                self.data['S4']['Tc1'] = self.dataset.groups['S4']['Tc'][:, :, 0]
                self.data['S4']['Tc2'] = self.dataset.groups['S4']['Tc'][:, :, 1]

    def add_sids(self, adapt_resolution=True):
        if self.lat is None:
            raise ValueError("Latitude and longitude data must be loaded before adding SIDs. Call read_latlon() first.")

        self.sids = {}
        for scan in self.scans:
            self.sids[scan] = pystare.from_latlon_2d(lat=self.lat[scan], lon=self.lon[scan],
                                                    adapt_level=adapt_resolution)

    def read_sidecar_latlon(self, sidecar_path=None):
        self.lat = {}
        self.lon = {}
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)

        for scan in self.scans:
            self.lat[scan] = ds[scan]['Latitude'][:].astype(numpy.double)
            self.lon[scan] = ds[scan]['Longitude'][:].astype(numpy.double)

    def read_sidecar_index(self, sidecar_path=None):
        self.sids = {}
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        for scan in self.scans:
            self.sids[scan] = ds[scan]['STARE_index'][:, :].astype(numpy.int64)

    def read_sidecar_cover(self, sidecar_path=None):
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        self.stare_cover = ds['STARE_cover'][:].astype(numpy.int64)

    def to_df(self, xy=False):
        dfs = {}
        for scan in self.scans:
            df = dict()
            if self.lat is not None:
                df['lat'] = self.lat[scan].flatten()
                df['lon'] = self.lon[scan].flatten()
            if self.sids is not None:
                df['sids'] = self.sids[scan].flatten()
            if len(self.timestamps.keys()) > 0:
                df['timestamp'] = self.timestamps[scan].flatten()
            for key in self.data[scan].keys():
                df[key] = self.data[scan][key].flatten()
            df = starepandas.STAREDataFrame(df)
            dfs[scan] = df
        return dfs

    def close(self):
        """Close the dataset file."""
        if hasattr(self, 'dataset') and self.dataset is not None:
            self.dataset.close()

    def create_sidecar(self, n_workers=1, cover_res=None, out_path=None):
        """Create a STARE sidecar file for this SSMIS granule.
        
        Parameters
        ----------
        n_workers : int, optional
            Number of workers for parallel processing. Default is 1.
        cover_res : int, optional
            Resolution for the cover. If None, will be automatically determined.
        out_path : str, optional
            Output path for the sidecar file. If None, uses default naming convention.
            
        Returns
        -------
        Sidecar
            The created sidecar object.
        """
        if self.lat is None or self.lon is None:
            raise ValueError("Latitude and longitude data must be loaded before creating sidecar. Call read_latlon() first.")
            
        sidecar = Sidecar(self.file_path, out_path)
        
        cover_all = []
        for scan in self.scans:
            lons = self.lon[scan]
            lats = self.lat[scan]
            sids = pystare.from_latlon_2d(lat=lats, lon=lons, adapt_level=True)
            
            if not cover_res:
                cover_res = 10  # Use a fixed default cover resolution
            # Clamp cover_res to [0, 27]
            cover_res = max(0, min(27, cover_res))
            
            sids_adapted = pystare.spatial_coerce_resolution(sids, cover_res)
            cover_sids = numpy.unique(sids_adapted)
            
            cover_all.append(cover_sids)
            
            i = lats.shape[0]
            j = lats.shape[1]
            l = cover_sids.size
            
            nom_res = None
            
            sidecar.write_dimensions(i, j, l, nom_res=nom_res, group=scan)
            sidecar.write_lons(lons, nom_res=nom_res, group=scan)
            sidecar.write_lats(lats, nom_res=nom_res, group=scan)
            sidecar.write_sids(sids, nom_res=nom_res, group=scan)
            sidecar.write_cover(cover_sids, nom_res=nom_res, group=scan)
        
        cover_all = numpy.concatenate(cover_all)
        cover_all = numpy.unique(cover_all)
        
        # Only create the 'l' dimension if it does not already exist
        with netCDF4.Dataset(sidecar.file_path, 'a', format='NETCDF4') as ncfile:
            if 'l' not in ncfile.dimensions:
                sidecar.write_dimension('l', cover_all.size)
        sidecar.write_cover(cover_all, nom_res=nom_res)
        
        return sidecar

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


