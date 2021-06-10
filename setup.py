#!/usr/bin/env/python

import setuptools

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


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
    packages=["starepandas", "starepandas.io", "starepandas.io.granules", "starepandas.tools", "starepandas.datasets"],
    python_requires=">=3.6",
    install_requires=install_requires,    
    package_data={"starepandas.datasets": ['*.hdf', '*.nc']},
    test_suite='tests'
)

