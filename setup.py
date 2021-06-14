#!/usr/bin/env python

from typing import List
from setuptools import setup

long_description = """
# Tagreader #

Tagreader is a Python package for reading trend data from the OSIsoft
PI and Aspen Infoplus.21 IMS systems. It is intended to be easy to use,
and present as similar interfaces as possible to the backend historians.

Queries can be performed using either ODBC or REST API queries. ODBC
queries require the installation of proprietary drivers from AspenTech
and OSIsoft.

Tagreader outputs trend data as Pandas Dataframes, and uses the HDF5
file format to cache results.

Tagreader has only been tested on Windows platforms, but should also work
elsewhere when using REST APIs.

## Requirements

Python >= 3.6 with the following packages:

  + pandas >= 1.0.0
  + pytables
  + requests
  + requests_kerberos
  + pyodbc (if using ODBC connections, Windows only)

If using ODBC connections, you must also install proprietary drivers for
PI ODBC and/or Aspen IP.21 SQLPlus. These drivers are only available for
Microsoft Windows.

## Installation

To install and/or upgrade:

```
pip install --upgrade tagreader
```

## Usage example ##

```
import tagreader
c = tagreader.IMSClient("mysource", "ip21")
print(c.search("tag*"))
df = c.read_tags(["tag1", "tag2"], "18.06.2020 08:00:00", "18.06.2020 09:00:00", 60)
```

Also see the
[quickstart](https://github.com/equinor/tagreader-python/blob/master/docs/quickstart.ipynb)
document and the
[manual](docs/https://github.com/equinor/tagreader-python/blob/master/docs/manual.md)
for more information.
"""


def get_install_requirements() -> List[str]:
    with open("requirements.in") as f:
        requirements = f.read().splitlines()
        return requirements


setup(
    name="tagreader",
    description="Library for reading from Aspen InfoPlus.21 and OSIsoft PI IMS servers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=["Aspen InfoPlus.21", "OSIsoft PI"],
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
        "Programming Language :: Python :: 3.9",
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
    install_requires=get_install_requirements(),
)
