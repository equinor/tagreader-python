# tagreader-python <!-- omit in toc -->

![GitHub Build Status](https://github.com/equinor/tagreader-python/workflows/Test/badge.svg)
[![Devops Build Status](https://dev.azure.com/EIIDS/tagreader/_apis/build/status/equinor.tagreader-python?branchName=master)](https://dev.azure.com/EIIDS/tagreader/_build/latest?definitionId=5&branchName=master)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/tagreader) 
![PyPI](https://img.shields.io/pypi/v/tagreader) 
[![Downloads](https://pepy.tech/badge/tagreader)](https://pepy.tech/project/tagreader)

Tagreader is a Python package for reading trend data from the OSIsoft
PI and Aspen Infoplus.21 IMS systems. It is intended to be easy to use, 
and present as similar interfaces as possible to the backend historians.

Queries can be performed using either ODBC or REST API queries. ODBC
queries require the installation of proprietary drivers from AspenTech
and OSIsoft.

Tagreader outputs trend data as Pandas Dataframes, and uses the HDF5
file format to cache results.

Tagreader currently only works on Windows platforms, but can (and probably
will) be modified to work on Linux using REST APIs.

## Requirements

Python >=3.6 (*) with the following packages:

  + pandas >= 1.0.0
  + tables (*)
  + requests
  + requests-kerberos
  + certifi >= 2020.04.05
  + pyodbc (if using ODBC connections)

If using ODBC connections, you must also install proprietary drivers for
PI ODBC and/or Aspen IP.21 SQLPlus. These drivers are only available for
Microsoft Windows.

*) Please note that for Python 3.9 there are currently no [Windows wheels
available](https://github.com/PyTables/PyTables/issues/823) for the tables 
package. Tagreader will still function, but the cache will be disabled.

## Installation

To install and/or upgrade:

``` 
pip install --upgrade tagreader
```

If you wish to use ODBC connections to the IMS servers, you will also need 
some proprietary drivers. There is more information in the 
[manual](docs/manual.md#odbc-drivers).

## Documentation

There is a [quickstart](docs/quickstart.ipynb) example file that should get
you going. Also check out the [manual](docs/manual.md) for more information.

## Contributing

All contributions are welcome, including code, bug reports, issues, feature
requests, and documentation. The preferred way of submitting a contribution
is to either make an issue on GitHub or by forking the project on GitHub and
making a pull request.
