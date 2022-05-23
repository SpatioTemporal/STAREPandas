from starepandas.io.granules.viirsl2 import *
import glob
import re
from .modis import *
from .viirsl2 import *
from .ssmis import SSMIS
from .atms import ATMS


class UnsupportedFileError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'cannot handle {}'.format(file_path)
        super().__init__(self.message)


class SidecarNotFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'Could not find sidecar for {}'.format(file_path)
        super().__init__(self.message)


class CompanionNotFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'Could not find companion for {}'.format(file_path)
        super().__init__(self.message)


class MultipleCompanionsFoundError(Exception):
    def __init__(self, file_path):
        self.file_path = file_path
        self.message = 'More than one possible companion found for {}. Specify the prefix'.format(file_path)
        super().__init__(self.message)


def guess_companion_path(granule_path, folder=None, prefix=None):
    """
    Tries to find a companion to the granule.
    The assumption being that granule file names are composed of
    {Product}.{date}.{time}.{version}.{production_timestamp}.{extension}

    Parameters
    -----------
    granule_path: str
        The path of the granule to find the companion for
    folder: str
        the folder to look for companions
    prefix: str
        The prefix of the companion name; e.g. VJ102DNB

    Examples
    ---------
    >>> granule_path = starepandas.datasets.get_path('VNP02DNB.A2020219.0742.001.2020219125654.nc')
    >>> companion_path = guess_companion_path(granule_path, prefix='VNP03DNB')
    >>> companion_name = companion_path.split('/')[-1]
    >>> companion_name
    'VNP03DNB.A2020219.0742.001.2020219124651.nc'
    """

    if folder is None:
        folder = '/'.join(granule_path.split('/')[0:-1])
    name = granule_path.split('/')[-1]
    name_parts = name.split('.')
    date = name_parts[1]
    time = name_parts[2]
    if prefix:
        pattern = '{folder}/*{prefix}.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, prefix=prefix, date=date, time=time)
    else:
        pattern = '{folder}/*.*\\.{date}.{time}\\..*[^_stare]\\.(nc|hdf|HDF5)'
        pattern = pattern.format(folder=folder, date=date, time=time)
    matches = glob.glob(folder + '/*')
    companions = set(filter(re.compile(pattern).match, matches))
    companions = list(companions - set([granule_path]))
    if len(companions) < 1 or companions[0] == granule_path:
        raise CompanionNotFoundError(granule_path)
    if len(companions) > 1:
        raise MultipleCompanionsFoundError(granule_path)
    else:
        return companions[0]


granule_factory_library = {
    'MOD05|MYD05': Mod05,
    'MOD09|MYD09': Mod09,
    'VNP02DNB|VJ102DNB': VNP02DNB,
    'VNP03DNB|VJ103DNB': VNP03DNB,
    'VNP03MOD|VJ103MOD': VNP03MOD,
    'CLDMSKL2VIIRS': CLDMSKL2VIIRS,
    'SSMIS': SSMIS,
    'ATMS': ATMS
}


# Do we need nom_res=None?
def granule_factory(file_path, sidecar_path=None, nom_res=None):
    """
    Returns a granule loader from the dictionary starepandas.io.granules.granule_factory_library.
    The keys in granule_factory_library are regex patterns against which file_path is matched.
    The values are the classes with constructors of signature (file_path,sidecar). For example:

    ```python
        granule_factory_library = { 'MOD05|MYD05'       : Mod05,
                                    'MOD09|MYD09'       : Mod09,
                                    'VNP02DNB|VJ102DNB' : VNP02DNB,
                                    ...}
    ```

    To add a loader for a granule or dataset not presently supported by starepandas,
    one can add a class implementing the interface defined by starepandas.io.granules.Granule.
    This can be done by defining a class inheriting Granule or
    another Granule-derived class (like Modis, Mod09, VIIRSL2, etc.).
    To see the current list of loaders distributed with starepandas, examine granule_factor_library.

    An example of adding a new loader follows.

    ```
    class VNP02IMG(VIIRSL2):
        "Add loader for VNP02IMG, extending VIIRSL2, which extends Granule.
        VNP02IMG holds observations. Geolocations are in VNP03IMG, which has its own loader."

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

    # VNP02IMG is the short name for a Suomi National Polar-orbiting
    # Partnership (SNPP) NASA/VIIRS L1B observation product.
    # Add 'VJ102IMG' to the regex pattern for the Joint Polar-orbiting Satellite System (JPSS-1/NOAA20).

    starepandas.io.granules.granule_factory_library['VNP02IMG|VJ102IMG']=VNP02IMG
    ```

    Then to load a granule file into a starepandas dataframe you can do something like the following.

    ```
    granule_name="/home/jovyan/data/VNP02IMG.A2021182.0000.001.2021182064359.nc"
    vnp02 = starepandas.read_granule(granule_name, sidecar=False, read_latlon=False, add_stare=False)

    ```
    Please see the examples/user_defined_granule_loader.ipynb in the starepandas
    distribution, i.e. https://github.com/SpatioTemporal/STAREPandas/tree/master/examples .

    """

    for regex, make_granule in granule_factory_library.items():
        if re.search(regex, file_path, re.IGNORECASE):
            return make_granule(file_path, sidecar_path)
    raise UnsupportedFileError(file_path)
    return None


def read_granule(file_path,
                 latlon=False,
                 sidecar=False,
                 sidecar_path=None,
                 add_sids=False,
                 adapt_resolution=True,
                 xy=False,
                 nom_res=None,
                 read_timestamp=False,
                 **kwargs):
    """ Reads a granule into a STAREDataFrame

    Parameters
    -----------
    file_path: str
        path of the granule
    latlon: bool
        toggle whether to read the latitude and longitude variables
    sidecar: bool
        toggle whether to read the sidecar file
    sidecar_path: str
        path of the sidecar file. If not provided, it is assumed to be ${file_path}_stare.nc
    add_sids: bool
        toggle whether to lookup stare indices
    adapt_resolution: bool
        toggle whether to adapt the resolution
    xy: bool
        toggle wheather to add array coordinates to the dataframe.
    nom_res: str
        optional; for multi-resolution products, specify which resolution to read
    read_timestamp:
        toggle wheather to read the the timestamp

    Returns
    --------
    df: starepandas.STAREDataFrame
        A dataframe holding the granule data

    Examples
    ----------
    >>> fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    >>> modis = starepandas.read_granule(fname, latlon=True, sidecar=True)
    """

    granule = granule_factory(file_path, sidecar_path, nom_res)

    if add_sids:
        latlon = True
        sidecar = False

    if read_timestamp:
        granule.read_timestamps()

    granule.read_data()

    if latlon:
        if sidecar:
            granule.read_sidecar_latlon()
        else:
            granule.read_latlon()

    if sidecar:
        granule.read_sidecar_index(sidecar_path)
    elif add_sids:
        granule.add_sids(adapt_resolution)

    df = granule.to_df(xy=xy)
    return df
