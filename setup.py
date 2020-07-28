#!/usr/bin/env/python
"""Installation script
"""

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('requirements.txt') as f:
    install_requires = f.read().splitlines()


LONG_DESCRIPTION = """STAREPandas is the STARE pendant to [GeoPandas](https://geopandas.org/). It makes working with geospatial data in python easier. It provides file and database I/O functionality and allows to easily perform STARE based spatial operations that would otherwise require a (STARE-extended) spatial database or a geographic information system"""


# get all data dirs in the datasets module
data_files = []


setup(
    name="starepandas",
    version='0.2',
    description="STARE pandas extensions",
    license="MIT",
    author="Niklas Griessbaum",
    author_email="griessbaum@ucsb.edu",
    url="https://github.com/NiklasPhabian/starepandas",
    long_description=LONG_DESCRIPTION,
    packages=[
        "starepandas",
        "starepandas.io",
        "starepandas.tests",
        "starepandas.tools",
    ],
    python_requires=">=3.5",
    install_requires=install_requires,
) 



