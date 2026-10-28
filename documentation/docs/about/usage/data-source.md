---
sidebar_position: 2
---

# Data sources

Tagreader supports connecting to PI and IP.21 servers using a Web API interfaces. When calling certain
methods, the user will need to tell tagreader which system and which connection method to use. This input argument is
called `imstype` , and can be one of the following case-insensitive strings:

* `piwebapi` : For connecting to OSISoft PI Web API
* `aspenone` : For connecting to AspenTech Process Data REST Web API

## Listing available data sources

The method `tagreader.list_sources()` can query for available PI and IP.21 servers available. Input arguments:

* `imstype` : The name of the IMS type to query. Valid values: `piwebapi` and `aspenone`.

The following input arguments are only relevant when calling `list_sources()` with a Web API `imstype` ( `piwebapi` or
`aspenone` ):

* `url` (optional): Path to server root, e.g. _"https://aspenone/ProcessData/AtProcessDataREST.dll"_ or
_"https://piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` if
`imstype` is `piwebapi` or `aspenone`.
* `verify_ssl` (optional): Whether to verify the server certificate, or a path to a trusted CA bundle. **Default**: `True`.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth objects
that work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth.

The legacy `pi`, `ip21`, and `aspen` types selected ODBC clients and are no longer
supported. Data sources are discovered through the REST service, not the Windows registry.

**Example:**

``` python
from tagreader import list_sources
list_sources("aspenone")
list_sources("piwebapi")
```

When called with `imstype` set to `piwebapi` or `aspenone` , `list_sources()` will connect to the web server URL and
query for the available list of data sources. This list is normally the complete set of data sources available on the
server, and does not indicate whether the user is authorized to query the source or not.

When querying Equinor Web API for data sources, `list_sources()` should require no input argument except
`imstype="piwebapi"` or `imstype="aspenone"`. For non-Equinor servers, `url` will need to be specified, as may `auth`
and `verify_ssl` .