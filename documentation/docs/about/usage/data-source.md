---
sidebar_position: 2
---

# Data sources

Tagreader supports connecting to PI and IP.21 servers using both ODBC and Web API interfaces. When calling certain 
methods, the user will need to tell tagreader which system and which connection method to use. This input argument is 
called `imstype` , and can be one of the following case-insensitive strings:

* `pi` : For connecting to OSISoft PI via ODBC.
* `piwebapi` : For connecting to OSISoft PI Web API
* `ip21` : For connecting to AspenTech InfoPlus.21 via ODBC
* `aspenone` : For connecting to AspenTech Process Data REST Web API

## Listing available data sources

The method `tagreader.list_sources()` can query for available PI and IP.21 servers available through both ODBC and Web 
API. Input arguments:

* `imstype` : The name of the IMS type to query. Valid values: `pi` , `ip21` , `piwebapi` and `aspenone`.

The following input arguments are only relevant when calling `list_sources()` with a Web API `imstype` ( `piwebapi` or 
`aspenone` ):

* `url` (optional): Path to server root, e.g. _"https://aspenone/ProcessData/AtProcessDataREST.dll"_ or 
_"https://piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` if 
* `imstype` is `piwebapi` or `aspenone` .
* `verifySSL` (optional): Whether to verify SSL certificate sent from server. **Default**: `True`.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth objects 
that work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth.

When called with `imstype` set to `pi` , `list_sources()` will search the registry at 
*HKEY_CURRENT_USER\Software\AspenTech\ADSA\Caches\AspenADSA\{username}* for available PI servers. Similarly, 
if called with `imstype` set to `ip21` , *HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\PISystem\PI-SDK* will be searched 
for available IP.21 servers. Servers found through the registry are normally servers to which the user is authorized, 
and does not necessarily include all available data sources in the organization.

**Example:**

``` python
from tagreader import list_sources
list_sources("ip21")
list_sources("piwebapi")
```

When called with `imstype` set to `piwebapi` or `aspenone` , `list_sources()` will connect to the web server URL and 
query for the available list of data sources. This list is normally the complete set of data sources available on the 
server, and does not indicate whether the user is authorized to query the source or not.

When querying Equinor Web API for data sources, `list_sources()` should require no input argument except 
`imstype="piwebapi"` or `imstype="aspenone"`. For non-Equinor servers, `url` will need to be specified, as may `auth` 
and `verifySSL` .