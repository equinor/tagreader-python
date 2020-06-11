#!/usr/bin/env python

from setuptools import setup

setup(
    name="tagreader",
    description="Library for reading from Aspen InfoPlus.21 and OSIsoft PI IMS servers",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords=[
        "Aspen InfoPlus.21",
        "OSIsoft PI"
    ],
    url="https://github.com/equinor/tagreader-python",
    author="Einar S. Idsø",
    author_email="eiids@equinor.com",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
    packages=["tagreader"],
    platforms=["Windows"],
    package_data={"": ["*.md"]},
    python_requires="~=3.6",
    setup_requires=["setuptools_scm"],
    use_scm_version={"write_to": "tagreader/version.py"},
    install_requires=[
        "pandas >=0.23",
        "tables",
        "pyodbc",
        "requests",
        "requests_kerberos"
    ],
)
