from starepandas.io.granules.ssmis import SSMIS
import numpy


class ATMS(SSMIS):
    
    def read_timestamps(self):
        self.timestamps = {}
        
        for scan in self.scans:
            ts = self.read_timestamp_scan(scan)
            ts_len = ts.shape[0]
            ts = numpy.repeat(ts, 96)
            ts = ts.reshape(ts_len, 96)
            self.timestamps[scan] = ts
    
    def read_data(self):
        if 'S1' in self.scans:
            self.data['S1']['Tc1'] = self.netcdf.groups['S1']['Tc'][:, :, 0]

        if 'S2' in self.scans:
            self.data['S2']['Tc1'] = self.netcdf.groups['S2']['Tc'][:, :, 0]

        if 'S3' in self.scans:
            self.data['S3']['Tc1'] = self.netcdf.groups['S3']['Tc'][:, :, 0]

        if 'S4' in self.scans:
            self.data['S4']['Tc1'] = self.netcdf.groups['S4']['Tc'][:, :, 0]
            self.data['S4']['Tc2'] = self.netcdf.groups['S4']['Tc'][:, :, 1]
            self.data['S4']['Tc3'] = self.netcdf.groups['S4']['Tc'][:, :, 2]
            self.data['S4']['Tc4'] = self.netcdf.groups['S4']['Tc'][:, :, 3]
            self.data['S4']['Tc5'] = self.netcdf.groups['S4']['Tc'][:, :, 4]
            self.data['S4']['Tc6'] = self.netcdf.groups['S4']['Tc'][:, :, 5]

