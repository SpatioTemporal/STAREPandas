#!/usr/bin/env/python

import setuptools
import versioneer


version = versioneer.get_version(),
cmdclass = versioneer.get_cmdclass()



setuptools.setup(
    version=version[0],   
    #version='0.6.1',
    cmdclass=cmdclass
)

