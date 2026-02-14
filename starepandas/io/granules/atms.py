from starepandas.io.granules.ssmis import SSMIS
import numpy


class ATMS(SSMIS):
    
    def __init__(self, file_path, sidecar_path=None, scans=['S1', 'S2']):
        """Initialize ATMS reader for 2025 data format.
        
        ATMS 2025 files typically have S1 and S2 scans with different channel structures:
        - S1: Single channel (similar to SSMIS S1)
        - S2: Multiple channels (similar to SSMIS S4)
        """
        super().__init__(file_path, sidecar_path, scans)

    def read_timestamps(self):
        """Read timestamps for ATMS scans."""
        self.timestamps = {}

        for scan in self.scans:
            ts = self.read_timestamp_scan(scan)
            if ts is not None:
                ts_len = ts.shape[0]
# ATMS has different scan line counts than SSMIS
                # Use the actual scan line count from latitude dimension
                if hasattr(self, 'lat') and scan in self.lat and hasattr(self.lat[scan], 'shape'):
                    scan_lines = self.lat[scan].shape[0] if len(self.lat[scan].shape) > 0 else ts_len
                else:
                    scan_lines = ts_len
                
                ts = numpy.repeat(ts, scan_lines)
                ts = ts.reshape(scan_lines, -1)
                # Only take the first column to match timestamp structure
                ts = ts[:, 0] if ts.shape[1] > 0 else ts.flatten()
                self.timestamps[scan] = ts

    def read_data(self):
        """Read brightness temperature data for ATMS scans.
        
        ATMS 2025 format:
        - S1: Single channel temperature data (Tc[:, :, 0])
        - S2: Multiple channel temperature data (Tc[:, :, 0:5])
        """
        if 'S1' in self.scans:
            # S1 has single channel - take first channel
            self.data['S1']['Tc1'] = self.netcdf.groups['S1']['Tc'][:, :, 0]

        if 'S2' in self.scans:
            # S2 has multiple channels - take first 6 channels like SSMIS S4
            tc_data = self.netcdf.groups['S2']['Tc']
            if tc_data.shape[-1] >= 1:
                self.data['S2']['Tc1'] = tc_data[:, :, 0]
            if tc_data.shape[-1] >= 2:
                self.data['S2']['Tc2'] = tc_data[:, :, 1]
            if tc_data.shape[-1] >= 3:
                self.data['S2']['Tc3'] = tc_data[:, :, 2]
            if tc_data.shape[-1] >= 4:
                self.data['S2']['Tc4'] = tc_data[:, :, 3]
            if tc_data.shape[-1] >= 5:
                self.data['S2']['Tc5'] = tc_data[:, :, 4]
            if tc_data.shape[-1] >= 6:
                self.data['S2']['Tc6'] = tc_data[:, :, 5]
