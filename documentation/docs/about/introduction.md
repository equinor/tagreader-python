---
sidebar_position: 1
---
# Introduction

Tagreader is a Python package for reading trend data from the OSIsoft PI and Aspen Infoplus.21 IMS systems. It is 
intended to be easy to use, and present as similar interfaces as possible to the backend historians.

While originally developed for Windows, Tagreader can since release 3.0.0 also be used on Linux and Windows platforms.

## Installation
You can install tagreader directly into your project from pypi by using pip
or another package manager. 

```shell"
pip install tagreader
```

Please see the [installation](/quickstart) page for more extensive documentation
## Usage
Queries in tagreader can be performed using either REST API (preferred) or ODBC queries. The use of ODBC queries require installation 
of proprietary drivers from AspenTech and OSIsoft that are only available for Windows.

For more information on setup and usage of tagreader check out: [Usage](/usage)

For examples on the usage of tagreader check out: [Examples](/examples)
## Contribute
As Tagreader is an open source project, all contributions are welcome. This includes code, bug reports, issues, feature requests, and documentation. The preferred 
way of submitting a contribution is to either create an issue on GitHub or to fork the project and make a pull request.

For starting contributing, see the [contribute section](../contribute/how-to-start-contributing.md).