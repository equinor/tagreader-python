# tagreader-python <!-- omit in toc -->

![GitHub Build Status](https://github.com/equinor/tagreader-python/workflows/Test/badge.svg)
[![Devops Build Status](https://dev.azure.com/EIIDS/tagreader/_apis/build/status/equinor.tagreader-python?branchName=master)](https://dev.azure.com/EIIDS/tagreader/_build/latest?definitionId=5&branchName=master)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/tagreader) 
![PyPI](https://img.shields.io/pypi/v/tagreader) 
[![Downloads](https://pepy.tech/badge/tagreader)](https://pepy.tech/project/tagreader)

Tagreader is a Python package for reading trend data from the OSIsoft
PI and Aspen Infoplus.21 IMS systems. It is intended to be easy to use, 
and present as similar interfaces as possible to the backend historians.

While originally developed for Windows, Tagreader can since release 3.0.0
also be used on Linux and Windows platforms.

Queries can be performed using either REST API (preferred) or ODBC queries.
The use of ODBC queries require installation of proprietary drivers from
AspenTech and OSIsoft that are only available for Windows.

Trend data is output as Pandas Dataframes. The HDF5 file format is used
to cache results.

## Requirements

Python >=3.8 with the following packages:

  + pandas >= 1.0.0
  + tables (*)
  + requests
  + requests-kerberos
  + certifi >= 2023.5.7
  + pyodbc (**)

*) If tables is not installed, caching of fetched data will be disabled.
Tables will be installed alongside Tagreader unless the installation is
known to fail, which is the case for Macs with ARM CPU.

**) If using ODBC connections, you must also install proprietary drivers for
PI ODBC and/or Aspen IP.21 SQLPlus. These drivers are only available for
Microsoft Windows. Pyodbc will therefore not be installed for non-Windows
systems.

## Installation

To install and/or upgrade:

``` 
pip install --upgrade tagreader
```

If you wish to use ODBC connections to the IMS servers, you will also need 
to install some proprietary drivers. There is more information in the 
[manual](docs/manual.md#odbc-drivers). Please note that the web APIs should
normally be preferred.

## Usage example

```
import tagreader
c = tagreader.IMSClient("mysource", "aspenone")
print(c.search("tag*"))
df = c.read_tags(["tag1", "tag2"], "18.06.2020 08:00:00", "18.06.2020 09:00:00", 60)
```

## Documentation

There is a [quickstart](docs/quickstart.ipynb) example file that should get
you going. Also check out the [manual](docs/manual.md) for more information.

## Contributing

All contributions are welcome, including code, bug reports, issues, feature
requests, and documentation. The preferred way of submitting a contribution
is to either create an issue on GitHub or to fork the project and make a pull
request.
