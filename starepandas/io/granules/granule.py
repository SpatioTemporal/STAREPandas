import glob
import starepandas
import numpy
import pystare


class Granule:

    def __init__(self, file_path, sidecar_path=None):
        self.file_path = file_path
        self.sidecar_path = sidecar_path
        self.data = {}
        self.lat = None
        self.lon = None
        self.sids = None
        self.stare_cover = None
        self.ts_start = None
        self.ts_end = None
        self.nom_res = None
        self.companion_prefix = None

    def guess_sidecar_path(self):
        name = '.'.join(self.file_path.split('.')[0:-1]) + '_stare.nc'
        if 's3://' == self.file_path[0:5]:
            tokens = starepandas.io.s3.parse_s3_url(name)
            names, _ = starepandas.io.s3.s3_glob(name)
            target = '{prefix}/{resource}'.format(prefix=tokens['prefix'], resource=tokens['resource'])
            if target in names:
                return name
            else:
                raise starepandas.io.granules.SidecarNotFoundError(self.file_path)
        else:
            if glob.glob(name):
                return name
            else:
                raise starepandas.io.granules.SidecarNotFoundError(self.file_path)

    def guess_companion_path(self, prefix=None, folder=None):
        if prefix is None:
            prefix = self.companion_prefix
        return starepandas.io.granules.guess_companion_path(self.file_path, prefix=prefix, folder=folder)

    def add_sids(self, adapt_resolution=True):
        self.sids = pystare.from_latlon_2d(lat=self.lat, lon=self.lon, adapt_level=adapt_resolution)

    def read_sidecar_index(self, sidecar_path=None):
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        try:
            self.sids = ds['STARE_index_{}'.format(self.nom_res)][:, :].astype(numpy.int64)
        except IndexError:
            self.sids = ds['STARE_index{}'.format(self.nom_res)][:, :].astype(numpy.int64)

    def read_sidecar_cover(self, sidecar_path=None):
        if sidecar_path:
            scp = sidecar_path
        elif self.sidecar_path:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        self.stare_cover = ds['STARE_cover'][:].astype(numpy.int64)

    def read_sidecar_latlon(self, sidecar_path=None):
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        self.lat = ds['Latitude_{}'.format(self.nom_res)][:, :]
        self.lon = ds['Longitude_{}'.format(self.nom_res)][:, :]

    def to_df(self, xy=False):
        """ Converts the granule object to a dataframe

        Parameters
        -----------
        xy: bool
            If true, add columns for the original array coordinates

        Returns
        --------
        df: STAREDataFrame
            dataframe containing lat, lon, xy, and data; one row per observation

        """
        df = {}

        if self.lat is not None:
            df['lat'] = self.lat.flatten()
            df['lon'] = self.lon.flatten()
        if self.sids is not None:
            df['sids'] = self.sids.flatten()
        if self.ts_start is not None and self.ts_end is not None:
            df['ts_start'] = self.ts_start
            df['ts_end'] = self.ts_end
        if xy:
            if self.lat is not None:
                indices = numpy.indices(self.lat.shape)
            elif self.sids is not None:
                indices = numpy.indices(self.sids.shape)
            else:
                ds = list(self.data.values())[0]
                indices = numpy.indices(ds.shape)
            df['x'] = indices[0].flatten()
            df['y'] = indices[1].flatten()
        for key in self.data.keys():
            # print('saving: ',key)
            df[key] = self.data[key].flatten()
        return starepandas.STAREDataFrame(df)


