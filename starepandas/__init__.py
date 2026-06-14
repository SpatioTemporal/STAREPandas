from starepandas.tools import *
from starepandas.io.folder import folder2catalog
from starepandas.io.granules import read_granule
from starepandas.io.granules import guess_companion_path
from starepandas.io.granules import reconstitute_hdf5_from_s3
from starepandas.io.pod import read_pods
from starepandas.io.database import read_sql_table
from starepandas.io.geotiff import read_geotiff

import starepandas.datasets  

import starepandas.io.s3
import starepandas.io.postgis
import starepandas.io.granules

from .staredataframe import STAREDataFrame

# Task 7: ingest functions live in their own module so the cloud worker
# (Path C, C-2) can import them without pulling in the demo classes.
import starepandas.ingest
from starepandas.ingest import (
    ingest_granules_local,
    ingest_granules_s3,
    clean_s3_prefix,
)

# Path C C-6: client SDK. ``sp.cloud.ingest_granules(...).wait()`` submits an
# ingest job to the deployed REST API and polls it to completion.
import starepandas.cloud as cloud

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from . import _version
__version__ = _version.get_versions()['version']
