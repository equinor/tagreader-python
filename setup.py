#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='imsclient',
      description='Library for reading from Aspen IP21 and OSIsoft PI IMS servers',
      version='0.0.2',
      author='Einar S. Idso',
      author_email="eiids@statoil.com",
      packages=['imsclient'],
      exclude_package_data={'imsclient':['__pycache__', '*/__pycache__']},
      platforms=['Windows'],
      package_data={'': ['*.md']},
      install_requires=['pandas',
                        'tables']
      )