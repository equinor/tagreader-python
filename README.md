# tagreader-python <!-- omit in toc -->

[![PyPI version](https://badge.fury.io/py/tagreader.svg)](https://badge.fury.io/py/tagreader)
![Build](https://github.com/equinor/tagreader-python/workflows/Build/badge.svg)
[![Build Status](https://dev.azure.com/EIIDS/tagreader/_apis/build/status/equinor.tagreader-python?branchName=master)](https://dev.azure.com/EIIDS/tagreader/_build/latest?definitionId=5&branchName=master)

## Index <!-- omit in toc -->

- [Introduction](#introduction)
- [Requirements](#requirements)
- [Installation](#installation)
- [Documentation](#documentation)
- [Contributing](#contributing)

## Introduction

Tagreader is a Python package for reading trend data from the OSIsoft PI and Aspen Infoplus.21 IMS systems. Tagreader is
intended to be easy to use, and present as similar as possible interfaces to the backend historians.   

Queries are performed using ODBC and proprietary drivers from Aspen and OSIsoft, but code has been structured in such
a way as to allow for other interfaces, e.g. REST APIs, in the future.
  
Tagreader is based on Pandas for Python, and uses the HDF5 file format to cache results. 

## Requirements

* Python >= 3.6 with the following packages:
  + pandas >= 1.0.0
  + pytables
  + pyodbc (if using ODBC connections)
  + requests (if using REST-API connections)
* If using ODBC connections, you must also install proprietary drivers for PI ODBC and/or Aspen IP.21 SQLPlus. These drivers are only available for Microsoft Windows.

## Installation

To install and/or upgrade:

``` 
pip install --upgrade tagreader
```

If you wish to use ODBC connections to the IMS servers, you will also need some proprietary drivers. There is more information in the [manual](docs/manual.md#odbc-drivers).

## Documentation

There is a [quickstart](docs/quickstart.ipynb) example file that should get you going. Also check out the [manual](docs/manual.md) for more information.

## Contributing

All contributions are welcome, including code, bug reports, issues, feature requests, and documentation. The preferred
way of submitting a contribution is to either make an issue on GitHub or by forking the project on GitHub and making a 
pull request.
