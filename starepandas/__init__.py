from starepandas.tools import *
from starepandas.io.folder import folder2catalog
from starepandas.io.granules import read_granule
from starepandas.io.granules import guess_companion_path
from starepandas.io.pod import read_pods
from starepandas.io.database import read_sql_table

import starepandas.datasets  

import starepandas.io.s3

import starepandas.io.granules

from .staredataframe import STAREDataFrame

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from . import _version
__version__ = _version.get_versions()['version']
