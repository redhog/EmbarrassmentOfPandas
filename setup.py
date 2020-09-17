#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='EmbarrassmentOfPandas',
    version='0.0.1',
    description='Complex datatypes for pandas',
    long_description='Abstraction on top of pandas to add complex datatypes',
    long_description_content_type="text/markdown",
    author='Egil Moeller',
    author_email='em@emeraldgeo.no',
    url='https://github.com/EMeraldGeo/EmbarrassmentOfPandas',
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas",
    ]
)
