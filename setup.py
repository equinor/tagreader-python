#!/usr/bin/env python

from setuptools import setup

version = {}
with open("tagreader/version.py") as f:
    exec(f.read(), version)

setup(
    name="tagreader",
    description="Library for reading from Aspen IP21 and OSIsoft PI IMS servers",
    version=version["__version__"],
    author="Einar S. Idso",
    author_email="eiids@equinor.com",
    packages=["tagreader"],
    platforms=["Windows"],
    package_data={"": ["*.md"]},
    python_requires="~=3.6",
    install_requires=["pandas >=0.23", "tables", "pyodbc"],
)
