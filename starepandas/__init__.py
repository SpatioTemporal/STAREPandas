from starepandas.staredataframe import STAREDataFrame
from starepandas.stareseries import STARESeries

from starepandas.tools import stare_join
from starepandas.tools.stare_conversions import *
    
from starepandas.io.granules import * #read_granule, granule_factory, guess_companion_path
from starepandas.io.folder import folder2catalog
from starepandas.io.database import read_sql_table



from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
