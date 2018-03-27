=========
IMSclient
=========

Introduction
============

IMSclient is a Python package for reading trend data from the OSIsoft PI and Aspen IP21 IMS systems.

It is based on the Pandas package for Python, and uses the HDF5 file format to cache results.

Requirements
============

 * Python >= 3.6
 * Pandas
 * PI ODBC driver and/or Aspen IP21 ODBC driver
 
 Installation
 ============
 
 TBW
 
 Aspen IP21
 ----------
 This section applies to users who need to obtain data from Aspen IP21. We assume the user has  already applied for access to "Aspen Process Explorer" from http://accessit.statoil.no for one or more assets and installed Aspen Process Explorer using "Statoil Applications".
 
 It may be possible to use a 32-bit database driver with 32-bit Python, but for those of us that prefer current-century technology we need to install the 64-bit Aspen database toolbox:
  	- Download the installation files from a folder named "AspenODBC" in https://git.statoil.no/SIGMA/SIGMAinstallFiles (zip-file https://git.statoil.no/SIGMA/SIGMAinstallFiles/repository/archive.zip?ref=master). 
  	    *Alternatively, if you have access, the files can also be found on  \\statoil.net\dfs\common\TPD\RD_Data\Process Control\Software Tools\AspenODBC).
	- Start the installation of Aspen ODBC from the downloaded fileset and select "repair"
	    * When prompted for license server, select "next"
	    * When prompted for user, write /SYSTEM/. Leave password blank.
		* When prompted for Aspen Configuration Manager, just press "next" without changing anything
		* If prompted for visual studio 2008 runtime libraries, just ignore and press "ok".
		* Do something else - this step can take a few hours to complete.
 
 Usage
 =====
 
 TWB