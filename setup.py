#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='pyims',
      description='Library for reading from Aspen IP21 and OSIsoft PI IMS servers',
      version='0.0.4',
      author='Einar S. Idso',
      author_email="eiids@statoil.com",
      packages=['pyims'],
      platforms=['Windows'],
      package_data={'': ['*.md']},
      install_requires=['pandas',
                        'tables']
      )