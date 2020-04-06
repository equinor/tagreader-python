
# tagreader-python #

## Index ##

* [Introduction](#introduction)
* [Requirements](#requirements)
* [Installation](#installation)
  * [ODBC Drivers](#odbc-drivers)
* [Uninstallation](#uninstallation)
* [Contributing](#contributing)
* [Usage examples](#usage-examples)

## Introduction ##

Tagreader is a Python package for reading trend data from the OSIsoft PI and Aspen IP21 IMS systems. Tagreader is
intended to be easy to use, and present as similar as possible interfaces to the backend historians.   

Queries are performed using ODBC and proprietary drivers from Aspen and OSIsoft, but code has been structured in such
a way as to allow for other interfaces, e.g. REST APIs, in the future.
  
Tagreader is based on Pandas for Python, and uses the HDF5 file format to cache results. 

## Requirements ##

* Python >= 3.6 with the following packages:
  * pandas >= 0.23
  * pytables
  * pyodbc
* PI ODBC driver and/or Aspen IP21 SQLPlus ODBC driver
* Microsoft Windows (Sorry. This is due to the proprietary ODBC drivers for OSIsoft PI and Aspen IP21)
 
## Installation ##

Tagreader will in the not-so-distant future be made available from PyPi. Until then, installation can be performed
as follows:

* Download the package contents from GitHub using either a tagged release or
 latest committed code. The latter may be less stable than tagged releases.
 Zip is the easiest for most users.  
* Extract the zip-file anywhere, e.g. `C:\Appl\`. 
* Open a command-line with python/pip in path.
* Activate your preferred environment or create a new environment. 
* Navigate to where you extracted the package and enter the top directory (e.g `C:\Appl\tagreader\`)
* Install the package, letting pip resolve dependencies: `pip install .` 

### ODBC Drivers ###

If you work in Equinor, you can find further information and links to download the drivers on our 
[wiki](https://wiki.equinor.com/wiki/index.php/tagreader).

If you do not work in Equinor: In order to fetch data from OSISoft PI or Aspen IP21, you need to obtain and install
proprietary ODBC drivers. Check with your employer/organisation whether these are available. If not, you may be able
to obtain them directly from the vendors.

## Uninstallation ##

 * Activate the relevant environment 
 * `pip uninstall tagreader`

## Contributing ##

All contributions are welcome, including code, bug reports, issues, feature requests, and documentation. The preferred
way of submitting a contribution is to either make an issue on GitHub or by forking the project on GitHub and making a 
pull request.
  
## Usage examples ##
TBW