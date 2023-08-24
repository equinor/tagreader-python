---
sidebar_position: 1
---

# Installation

## Requirements
Python >=3.8 with the following packages:

* pandas >= 1.0.0
* requests
* requests-kerberos
* certifi >= 2023.5.7
* diskcache
* pyodbc (If using ODBC connection)

:::info  ODBC Connection
If using ODBC connections, you must also install proprietary drivers for PI ODBC and/or Aspen IP.21 SQLPlus. These 
drivers are only available for Microsoft Windows. Pyodbc will therefore not be installed for non-Windows systems.
:::

## installation
To install and/or upgrade:

```shell
pip install --upgrade tagreader
```
If you wish to use ODBC connections to the IMS servers, you will also need to install some proprietary drivers. 
See [ODBC drivers](docs/about/setup/odbc-drivers) for more information. 
Please note that the web APIs should normally be preferred.