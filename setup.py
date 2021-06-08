#!/usr/bin/env/python

import setuptools

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


# get all data dirs in the datasets module
data_files = []

setuptools.setup(
    name="starepandas",
    version='0.5.0',
    description="STARE pandas extensions",
    license="BSD-3",
    author="Niklas Griessbaum",
    author_email="griessbaum@ucsb.edu",
    url="https://github.com/SpatioTemporal/STAREPandas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    #package_dir={'': ''},
    #packages=setuptools.find_packages(where=''),
    packages=["starepandas", "starepandas.io", "starepandas.tools", "starepandas.datasets"],
    python_requires=">=3.6",
    install_requires=install_requires,
    test_suite='tests'
)

