from starepandas.io.granules.viirsl2 import *
import glob
import re
from .modis import *
from .viirsl2 import *
from .ssmis import SSMIS
from .atms import ATMS


class UnsuportedFileError(Exception):

    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'cannot handle {}'.format(file_path)
        super().__init__(self.message)


class SidecarNotFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'Could not find sidecar for {}'.format(file_path)
        super().__init__(self.message)


def guess_companion_path(granule_path, prefix=None, folder=None):
    """
    Tries to find a companion to the granule.
    The assumption being that granule file names are composed of
    {Product}.{date}.{time}.{version}.{production_timestamp}.{extension}

    Parameters
    -----------
    granule_path: str
        The path of the granule to find the companion for
    prefix: str
        The pri
    """

    if not folder:
        folder = '/'.join(granule_path.split('/')[0:-1])
    name = granule_path.split('/')[-1]
    name_parts = name.split('.')
    date = name_parts[1]
    time = name_parts[2]
    if prefix:
        pattern = '{folder}/{prefix}.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, prefix=prefix, date=date, time=time)
    else:
        pattern = '{folder}/.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, date=date, time=time)
    matches = glob.glob(folder + '/*')
    granules = list(filter(re.compile(pattern).match, matches))
    if len(granules) < 1 or granules[0] == granule_path:
        print('did not find companion')
        return None
    if len(granules) > 1:
        print('more than one possible match found. Specify the prefix!')
        print(granules)
    else:
        return granules[0]

granule_factory_library = {
    'MOD05|MYD05'       : Mod05,
    'MOD09|MYD09'       : Mod09,
    'VNP02DNB|VJ102DNB' : VNP02DNB,
    'VNP03DNB|VJ103DNB' : VNP03DNB,
    'VNP03MOD|VJ103MOD' : VNP03MOD,
    'CLDMSKL2VIIRS'     : CLDMSKL2VIIRS,
    'SSMIS'             : SSMIS,
    'ATMS'              : ATMS
}

def granule_factory(file_path, sidecar_path=None):
    """
    Returns a granule loader from the dictionary starepandas.io.granules.granule_factory_library. The keys in granule_factory_library are regex patterns against which file_path is matched. The values are the classes with constructors of signature (file_path,sidecar). For example:

```
granule_factory_library = {
    'MOD05|MYD05'       : Mod05,
    'MOD09|MYD09'       : Mod09,
    'VNP02DNB|VJ102DNB' : VNP02DNB,
    ...
}
```

    To add a loader for a granule or dataset not presently supported by starepandas, one can add a class implementing the interface defined by starepandas.io.granules.Granule. This can be done by defining a class inheriting Granule or another Granule-derived class (like Modis, Mod09, VIIRSL2, etc.). To see the current list of loaders distributed with starepandas, examine granule_factor_library.

    An example of adding a new loader follows.

```

class VNP02IMG(VIIRSL2):
    "Add loader for VNP02IMG, extending VIIRSL2, which extends Granule. VNP02IMG holds observations. Geolocations are in VNP03IMG, which has its own loader."
    def __init__(self, file_path, sidecar_path=None):
        super(VNP02IMG, self).__init__(file_path, sidecar_path)
        self.companion_prefix = 'VNP03IMG'

    def read_data(self):
        "Read the data of interest."
        for band in ['I04','I05']:
            IMG = self.netcdf.groups['observation_data'][band][:].data
            quality_flags = self.netcdf.groups['observation_data'][band+'_quality_flags'][:].data
            self.data[band+'_observations']  = IMG
            self.data[band+'_quality_flags'] = quality_flags

    def read_latlon(self):
        "Geolocations will be read from VNP03IMG using a separate loader."
        pass

    def read_sidecar_cover(self, sidecar_path=None):
        "Let VNP03IMG loader handle this."
        pass

    def read_sidecar_index(self, sidecar_path=None):
        "Let VNP03IMG loader handle this."
        pass

# 'VNP02IMG is the short name for a Suomi National Polar-orbiting Partnership (SNPP) NASA/VIIRS L1B observation product.
# Add 'VJ102IMG' to the regex pattern for the Joint Polar-orbiting Satellite System (JPSS-1/NOAA20).
starepandas.io.granules.granule_factory_library['VNP02IMG|VJ102IMG']=VNP02IMG

```

Then to load a granule file into a starepandas dataframe you can do something like the following.
```
granule_name="/home/jovyan/data/VNP02IMG.A2021182.0000.001.2021182064359.nc"
vnp02 = starepandas.read_granule(granule_name, sidecar=False, read_latlon=False, add_stare=False)

```

    Please see the examples/user_defined_granule_loader.ipynb in the starepandas distribution, i.e. https://github.com/SpatioTemporal/STAREPandas/tree/master/examples .

    """
    for regex,make_granule in granule_factory_library.items():
        if re.search(regex, file_path, re.IGNORECASE):
            return make_granule(file_path,sidecar_path)
    raise UnsuportedFileError(file_path)
    return None


def read_granule(file_path, latlon=False, sidecar=False, sidecar_path=None, add_stare=False, adapt_resolution=True
                 ,**kwargs):
    """ Reads a granule into a STAREDataFrame

    :param file_path: path of the granule
    :type file_path: string
    :param latlon: toggle whether to read the latitude and longitude variables
    :type latlon: bool
    :param sidecar: toggle whether to read the sidecar file
    :type sidecar: bool
    :param sidecar_path: path of the sidecar file. If not provided, it is assumed to be ${file_path}_stare.nc
    :type sidecar_path: string
    :param add_stare: toggle whether to lookup stare indices
    :type add_stare: bool
    :param adapt_resolution: toggle whether to adapt the resolution
    :type adapt_resolution: bool
    :param kwargs:
    :type kwargs:
    :return: stare dataframe
    :rtype: starepandas.STAREDataFrame

    Examples
    ----------
    >>> fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    >>> modis = starepandas.read_granule(fname, latlon=True, sidecar=True)
    """
    
    granule = granule_factory(file_path, sidecar_path)

    if add_stare:
        latlon = True
        sidecar = False

    if latlon:
        granule.read_latlon()

    granule.read_data()

    if sidecar:
        granule.read_sidecar_index(sidecar_path)
    elif add_stare:
        granule.add_stare(adapt_resolution)

    return granule.to_df()
