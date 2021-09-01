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
    elif re.search('CLDMSKL2VIIRS', file_path, re.IGNORECASE):
        granule = CLDMSKL2VIIRS(file_path, sidecar_path)
    elif re.search('SSMIS', file_path, re.IGNORECASE):
        granule = SSMIS(file_path, sidecar_path)
    elif re.search('ATMS', file_path, re.IGNORECASE):
        granule = ATMS(file_path, sidecar_path)
    else:
        raise UnsuportedFileError(file_path)
        return None
    return granule


def read_granule(file_path, latlon=False, sidecar=False, sidecar_path=None, add_stare=False, adapt_resolution=True,
                 **kwargs):
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
