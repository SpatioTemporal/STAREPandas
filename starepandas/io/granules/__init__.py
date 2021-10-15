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
        granule = CLDMSKL2VIIRS(file_path, sidecar_path)
    elif re.search('SSMIS', file_path, re.IGNORECASE):
        granule = SSMIS(file_path, sidecar_path)
    elif re.search('ATMS', file_path, re.IGNORECASE):
        granule = ATMS(file_path, sidecar_path)
    else:
        raise UnsuportedFileError(file_path)
        return None
    return granule


def read_granule(file_path,
                 latlon=False,
                 sidecar=False,
                 sidecar_path=None,
                 add_sids=False,
                 adapt_resolution=True,
                 xy=False):
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

    Returns
    --------
    df: starepandas.STAREDataFrame
        A dataframe holding the granule data

    Examples
    ----------
    >>> fname = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    >>> modis = starepandas.read_granule(fname, latlon=True, sidecar=True)
    """

    granule = granule_factory(file_path, sidecar_path)

    if add_sids:
        latlon = True
        sidecar = False

    if latlon:
        granule.read_latlon()

    granule.read_data()

    if sidecar:
        granule.read_sidecar_index(sidecar_path)
    elif add_sids:
        granule.add_sids(adapt_resolution)

    df = granule.to_df(xy=xy)
    return df
