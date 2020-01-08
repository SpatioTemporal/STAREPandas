#!/usr/bin/env/python
"""Installation script
"""

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


LONG_DESCRIPTION = """STAREpandas adds STARE support to pandas dataframes.
STAREPandas provides high-level functions for users to explore and interact with STARE.
STARE dataframes represent geometries as sets of STARE
triangles or ”trixels” (analogous to GeoPandas geo-
dataframes which represent geometries as WKT.) In
STARE dataframes, points are represented as STARE trixels at the HTM tree’s leaf level.
Polygons are represented as sets of STARE trixels that cover the polygon. The
trixels are stored as integerized STARE index values.

"""

if os.environ.get("READTHEDOCS", False) == "True":
    INSTALL_REQUIRES = []
else:
    INSTALL_REQUIRES = ["geopandas >= 0.6.0"]

# get all data dirs in the datasets module
data_files = []



setup(
    name="starepandas",
    version='0.1',
    description="STARE pandas extensions",
    license="MIT",
    author="Niklas Griessbaum",
    author_email="griessbaum@ucsb.edu",
    url="http://geopandas.org",
    long_description=LONG_DESCRIPTION,
    packages=[
        "starepandas",
        "starepandas.io",
        "starepandas.tests",
        "starepandas.tools",
    ],
    python_requires=">=3.5",
    install_requires=INSTALL_REQUIRES,
) 



