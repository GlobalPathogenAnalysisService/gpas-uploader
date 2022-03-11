#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as f:
    README = f.read()

setup(
    name='gpas_uploader',
    version='1.0.0',
    description='Run extensive validation on a GPAS upload CSV',
    author='Jeff Knaggs, Bede Constantinidies, Zam Iqbal and Philip W Fowler',
    url='https://github.com/GenomePathogenAnalysisService/gpas-uploader',
    long_description = README,
    install_requires=[
        'pandas',
        'pandera',
        'pandarallel',
        'pycountry'
        ],
    scripts=['bin/gpas-upload'],
    packages = ['gpas_uploader'],
    license = 'MIT',
    python_requires='>=3.7',
    package_data={'': ['data/*']},
    include_package_data=True,
    zip_safe=False
    )
