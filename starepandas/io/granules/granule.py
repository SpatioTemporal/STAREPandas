import glob
import starepandas
import numpy
import re
import pystare


class Granule:
    
    def __init__(self, file_path, sidecar_path=None):
        self.file_path = file_path
        if sidecar_path:
            self.sidecar_path = sidecar_path
        else:
            self.sidecar_path = self.guess_sidecar_path()
        self.data = {}
        self.lat = None
        self.lon = None
        self.stare = None
        self.companion_prefix = None
    
    def guess_sidecar_path(self):
        name =  '.'.join(self.file_path.split('.')[0:-1]) + '_stare.nc'
        if 's3://' == self.file_path[0:5]:
            tokens = starepandas.io.s3.parse_s3_url(name)
            names,_ = starepandas.io.s3.s3_glob(name)
            target = '{prefix}/{resource}'.format(prefix=tokens['prefix'], resource=tokens['resource'])            
            if target in names:
                return name
        else:
            if glob.glob(name):
                return name
        return None
    
    def guess_companion_path(self, companion_prefix=None, folder=None):
        if not companion_prefix:
            companion_prefix = self.companion_prefix
        return starepandas.guess_companion_path(self.file_path, companion_prefix, folder)

        
    def add_stare(self, adapt_resolution=True):
        if self.lat is None:
            self.read_latlon()
        self.stare = pystare.from_latlon2D(lat=self.lat, lon=self.lon, adapt_resolution=adapt_resolution)        
    
    def read_sidecar_index(self, sidecar_path=None):
        if not sidecar_path:
            sidecar_path = self.sidecar_path
        ds = starepandas.io.s3.nc4_Dataset_wrapper(sidecar_path)
        self.stare = ds['STARE_index_{}'.format(self.nom_res)][:,:].astype(numpy.int64)
        
    def read_sidecar_cover(self, sidecar_path=None):
        if not sidecar_path:
            sidecar_path = self.sidecar_path
        ds = starepandas.io.s3.nc4_Dataset_wrapper(sidecar_path)        
        self.stare_cover = ds['STARE_cover_{}'.format(self.nom_res)][:].astype(numpy.int64)        
    
    def to_df(self):
        df = {}
        if self.lat is not None:
            df['lat']  = self.lat.flatten()
            df['lon'] = self.lon.flatten()
        if self.stare is not None:
            df['stare'] = self.stare.flatten()
        for key in self.data.keys():
            df[key] = self.data[key].flatten()
        return starepandas.STAREDataFrame(df) 