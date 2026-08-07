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
        """Read timestamps for ATMS scans.

        ATMS carries one timestamp per scan line, while ``to_df`` flattens the
        per-pixel grid — so each scan line's timestamp is repeated across the
        scan's pixel dimension, giving the same ``(scan_lines, pixels)`` shape
        as the latitude grid. SSMIS hard-codes that width (90/180); ATMS reads
        it from the file, where it varies by product.

        Read straight from the file rather than from ``self.lat``: callers run
        ``read_timestamps()`` before ``read_latlon()`` (see
        ``starepandas.io.granules.read_granule``), so ``self.lat`` is still
        ``None`` here.
        """
        self.timestamps = {}

        for scan in self.scans:
            ts = self.read_timestamp_scan(scan)
            if ts is None:
                continue
            pixels = self.scan_width(scan)
            self.timestamps[scan] = numpy.repeat(ts, pixels).reshape(ts.shape[0], pixels)

    def scan_width(self, scan):
        """Number of pixels per scan line, from the Latitude dataset's shape.

        Only the shape is touched, so no pixel data is read.
        """
        return self.scan_variable(scan, 'Latitude').shape[1]

    def read_data(self):
        """Read brightness temperature data for ATMS scans.

        Channel count per scan varies across the ATMS products (NPP,
        NOAA-20, NOAA-21), so every scan takes up to the first 6 channels
        it actually has, as ``Tc1``..``Tc6``.
        """
        for scan in self.scans:
            tc = self.scan_variable(scan, 'Tc')
            for channel in range(min(tc.shape[-1], 6)):
                self.data[scan][f'Tc{channel + 1}'] = tc[:, :, channel]

    def scan_variable(self, scan, name):
        """The named variable of a scan group, for either backing file type."""
        if self.file_type == 'hdf5':
            return self.dataset[scan][name]
        return self.dataset.groups[scan][name]
