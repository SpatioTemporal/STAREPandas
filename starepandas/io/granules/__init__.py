import re
import glob

from starepandas.io.granules.modis import *
from starepandas.io.granules.viirsl2 import *


class UnsuportedFileError(Exception):
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'cannot handle {}'.format(file_path)
        super().__init__(self.message)
        

def guess_companion_path(granule_path, prefix, folder=None):
        '''
        Tries to find a companion to the granule
        The assumption being that granule file names are composed of
        {Product}.{date}.{time}.{version}.{production_timestamp}.{extension}
        '''
        if not folder:
            folder = '/'.join(granule_path.split('/')[0:-1])
        name = granule_path.split('/')[-1]        
        name_parts = name.split('.')
        date = name_parts[1]
        time = name_parts[2]        
        pattern = folder + '/' + prefix + '.' + date + '.' + time + '*'
        matches = glob.glob(pattern)
        pattern = '.*[^_stare]\.(nc|hdf|HDF5)'
        granules = list(filter(re.compile(pattern).match, matches))        
        if len(matches) < 1:
            print('did not find companion')
            return None
        else:
            return matches[0]


def granule_factory(file_path, sidecar_path=None):
    if re.search('MOD05|MYD05', file_path, re.IGNORECASE):
        granule = Mod05(file_path, sidecar_path)
    elif re.search('MOD09|MYD09', file_path, re.IGNORECASE):
        granule = Mod09(file_path, sidecar_path)
    elif re.search('VNP02DNB|VJ102DNB', file_path, re.IGNORECASE):
        granule = VNP02DNB(file_path, sidecar_path)        
    elif re.search('VNP03DNB|VJ103DNB', file_path, re.IGNORECASE):
        granule = VNP03DNB(file_path, sidecar_path)
    elif re.search('VNP03MOD|VJ103MOD', file_path, re.IGNORECASE):
        granule = VNP03MOD(file_path, sidecar_path)   
    elif re.search('CLDMSK_L2_VIIRS', file_path, re.IGNORECASE):
        granule = CLDMSK_L2_VIIRS(file_path, sidecar_path)
    else:        
        raise UnsuportedFileError(file_path)
        return None
    return granule


def read_granule(file_path, read_latlon=False, sidecar=False, sidecar_path=None, track_first=False, add_stare=False, adapt_resolution=True, **kwargs):
    granule = granule_factory(file_path, sidecar_path)
    
    if read_latlon:
        granule.read_latlon()
        
    granule.read_data()
    
    if sidecar:        
        granule.read_sidecar_index(sidecar_path)
    elif add_stare:        
        granule.add_stare(adapt_resolution)
    
    return granule.to_df()