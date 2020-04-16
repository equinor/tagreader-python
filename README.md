
# tagreader-python #

![Build](https://github.com/equinor/tagreader-python/workflows/Build/badge.svg)
[![Build Status](https://dev.azure.com/EIIDS/tagreader/_apis/build/status/equinor.tagreader-python?branchName=master)](https://dev.azure.com/EIIDS/tagreader/_build/latest?definitionId=5&branchName=master)

## Index ##

* [Introduction](#introduction)
* [Requirements](#requirements)
* [Installation](#installation)
  * [ODBC Drivers](#odbc-drivers)
* [Uninstallation](#uninstallation)
* [Contributing](#contributing)
* [Usage examples](#usage-examples)

## Introduction ##

Tagreader is a Python package for reading trend data from the OSIsoft PI and Aspen Infoplus.21 IMS systems. Tagreader is
intended to be easy to use, and present as similar as possible interfaces to the backend historians.   

Queries are performed using ODBC and proprietary drivers from Aspen and OSIsoft, but code has been structured in such
a way as to allow for other interfaces, e.g. REST APIs, in the future.
  
Tagreader is based on Pandas for Python, and uses the HDF5 file format to cache results. 

## Requirements ##

* Python >= 3.6 with the following packages:
  * pandas >= 0.23
  * pytables
  * pyodbc
* PI ODBC driver and/or Aspen IP.21 SQLPlus ODBC driver
* Microsoft Windows (Sorry. This is due to the proprietary ODBC drivers for OSIsoft PI and Aspen IP.21)
 
## Installation ##

To install and/or upgrade:
```
pip install --upgrade tagreader
```

### ODBC Drivers ###

If you work in Equinor, you can find further information and links to download the drivers on our 
[wiki](https://wiki.equinor.com/wiki/index.php/tagreader-python).

If you do not work in Equinor: In order to fetch data from OSIsoft PI or Aspen InfoPlus.21, you need to obtain and
install proprietary ODBC drivers. It is typically not sufficient to install the desktop applications from Aspen or 
OSIsoft, since these normally don't come packaged with 64-bit ODBC drivers. Check with your employer/organisation 
whether the ODBC drivers are available for you. If not, you may be able to obtain them directly from the vendors. 

## Uninstallation ##

```
pip uninstall tagreader
```

## Contributing ##

All contributions are welcome, including code, bug reports, issues, feature requests, and documentation. The preferred
way of submitting a contribution is to either make an issue on GitHub or by forking the project on GitHub and making a 
pull request.
  
## Usage examples ##
TBW
