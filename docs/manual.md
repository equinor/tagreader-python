# Tagreader-python <!-- omit in toc -->

Tagreader is a Python package for reading trend data from the OSISoft PI and AspenTech InfoPlus.21 IMS systems. It can communicate with PI using ODBC or PI Web API, and with IP.21 using ODBC or Process Data REST Web API.

The ODBC connections require proprietary drivers that are unfortunately only available for Windows. The handlers for Web APIs use the Python requests library, and should therefore also work for other platforms.

Tagreader is intended to be easy to use, and present the same interface to the user regardless of IMS system and connection method.

# Index <!-- omit in toc -->

- [Requirements](#requirements)
- [Installation](#installation)
- [Before getting started](#before-getting-started)
- [Importing the module](#importing-the-module)
- [Listing available data sources](#listing-available-data-sources)
  - [ODBC](#odbc)
  - [Web API](#web-api)
- [The Client](#the-client)
  - [Creating a client](#creating-a-client)
  - [Connecting to data source](#connecting-to-data-source)
    - [Adding host certificate](#adding-host-certificate)
- [Searching for tags](#searching-for-tags)
- [Reading data](#reading-data)
  - [Selecting what to read](#selecting-what-to-read)
  - [Caching results](#caching-results)
  - [Time zones](#time-zones)
- [Fetching metadata](#fetching-metadata)
  - [get_units()](#get_units)
  - [get_description()](#get_description)

# Requirements

* Python >= 3.6 with the following packages:
  + pandas >= 1.0.0
  + pytables
  + pyodbc (if using ODBC connections)
  + requests (if using REST-API connections)
* If using ODBC connections, you must also install proprietary drivers for PI ODBC and/or Aspen IP.21 SQLPlus. These drivers are only available for Microsoft Windows.

# Installation

To install and/or upgrade:

``` 
pip install --upgrade tagreader
```

# Before getting started

It is highly recommended to go through the [quickstart.ipynb](quickstart) example. The quickstart has references to relevant sections in this manual.

# Importing the module

The module is imported with

``` python
import tagreader
```

# Listing available data sources

Tagreader contains four methods to list data sources, one for each combination of IMS system (PI or IP.21) and connection method (ODBC or Web API).

## ODBC

These two methods are only available in Windows. Both methods will search specific parts of the Windows registry for configured data sources, which is normally all data sources to which the user is authorized, and return them as a list:

* `list_pi_sources()` searches *HKEY_CURRENT_USER\Software\AspenTech\ADSA\Caches\AspenADSA\{username}*
* `list_aspen_sources()` searches *HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\PISystem\PI-SDK* <br/>

Usage example:

``` python
list_pi_sources()
```

## Web API

These two methods connect to the web server URLs and query for the available list of data sources. This list is normally the complete set of data sources available on the server, and does not indicate whether the user is authorized to query the source or not.

* `list_pi_sources_web()`
* `list_aspen_sources_web()` <br/>

If querying Equinor servers for data sources, these methods should require no input arguments. For non-Equinor servers, the following input arguments can be used: 

* `url` (optional): Path to server root, e.g. _"https:<span>//aspenone/ProcessData/AtProcessDataREST.dll"_ or _"https:<span>//piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` .
* `verifySSL` (optional): Whether to verify SSL certificate sent from server. **Default**: False.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth objects that work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth.

Examples:

``` python
print(list_pi_sources_web())
```

``` python
import getpass
from requests_ntlm import HttpNtlmAuth
user = "statoil.net\\" + getpass.getuser()
pwd = getpass.getpass()
auth = HttpNtlmAuth(user, pwd)
url = "https://aspenone/ProcessData/AtProcessDataREST.dll"

print(list_aspen_sources_web(url=url, auth=auth, verifySSL=False))
```

# The Client

The client handles interactions between the user and the data source. It shall present as similar as possible interface to the user, regardless of the type of data source that is used. Communication with the data source is performed by the handler object, which is attached to the client upon client creation. 

## Creating a client

A connection to a data source is prepared by creating an instance of `tagreader.IMSClient` with the following input arguments:

* `datasource` : Name of data source
* `imstype` : Indicates the type of data source that is requested, and therefore determines which handler type to use. Valid values are:
  + `pi` : For connecting to OSISoft PI via ODBC.
  + `ip21` : For connecting to AspenTech InfoPlus.21 via ODBC 
  + `piwebapi` : For connecting to OSISoft PI Web API
  + `aspenone` : For connecting to AspenTech Process Data REST Web API

  Note that ODBC connections require that [pyodbc](https://pypi.org/project/pyodbc/) is installed, while REST API connections require the [requests](https://requests.readthedocs.io/en/master/) module.

* `tz` (optional): Time zone naive time stamps will be interpreted as belonging to this time zone. Similarly, the returned data points will be localized to this time zone. **Default**: _"Europe/Oslo"_.

The following input arguments can be used when connecting to either `piwebapi` or to `aspenone` . None of these should be necessary to supply when connecting to Equinor servers:

* `url` (optional): Path to server root, e.g. _"https:<span>//aspenone/ProcessData/AtProcessDataREST.dll"_ or _"https:<span>//piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` .
* `verifySSL` (optional): Whether to verify SSL certificate sent from server. **Default**: False.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth objects that work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth.

### IMSTypes <!-- omit in toc -->

The imstypes `pi` and `ip21` will attempt to connect to a PI or IP.21 data source, respectively, using ODBC. 

Imstypes `piwebapi` and `aspenone` will attempt to connect to a PI Web API or an Aspentech Process Data REST Web API host, respectively.

## Connecting to data source

After creating the client as described above, connect to the server with the `connect()` method.

**Example**

Connecting to the PINO PI data source using ODBC:

``` python
c = tagreader.IMSClient("PINO", "pi")
c.connect()
```

Connecting to the Peregrino IP.21 data source using AspenTech Process Data REST Web API, specifying URL (not necessary), using NTLM authentication instead of default Kerberos, ignoring the server host's certificate, and specifying that all naive time stamps as well as the returned data shall use Rio local time:

``` python
import getpass
from requests_ntlm import HttpNtlmAuth
user = "statoil.net\\" + getpass.getuser()
pwd = getpass.getpass()
auth = HttpNtlmAuth(user, pwd)
c = tagreader.IMSClient("PER", "aspenone", tz="Brazil/East", verifySSL=False,
    url="https://ws2679.statoil.net/ProcessData/AtProcessDataREST.dll")
c.connect()
```

### Adding host certificate

For the two Web APIs, it may be a good idea to add the server certificate to your certificate chain to allow SSL validation. The procedure described below is based on https://incognitjoe.github.io/adding-certs-to-requests.html. It requires the use of [git-bash](https://git-scm.com/downloads) (or another way to run openssl). There may be better ways - please let me know if you find one.
1. Visit the server (e.g. https://pivision.equinor.com) in Google Chrome.
2. Click the padlock icon immediately to the left of the URL and select *Certificate (valid)->Certification Path->Statoil Root CA->View Certificate->Details*. 
3. Click *Copy to File...* and export the certificate as *DER encoded binary X.509 (.CER)* to *certificate.cer*.
4. Open *git-bash* and navigate to the directory where you stored the file.
5. Convert the file: `openssl x509 -inform der -in certificate.cer -out certificate.pem`.

We now need to add the certificate to the certificate chain. 

1.  Activate your Python virtual environment and install *certifi* if not already installed: `pip install certifi`.
2.  Run the following Python snippet to add the certificate to the chain:
``` python
import certifi
cafile = certifi.where()
with open('certificate.pem', 'rb') as infile:
  customca = infile.read()
with open(cafile, 'ab') as outfile:
  outfile.write(customca)
```

# Searching for tags

The client method `search()` can be used to search for tags using either tag name, tag description or both. 

Supply at least one of the following arguments:

* `tag` : Name of tag
* `desc` : Description of tag

If both arguments are provided, the both must match. 

`*` can be used as wildcard. 

**Examples**

``` python
c = tagreader.IMSClient("PINO", "pi")
c.connect()
c.search("cd*158")
c.search(desc="*reactor*")
c.search(tag="BA:*", desc="*Temperature*")
```

# Reading data

Data is read by calling the client method `read()` with the following input arguments:

* `tags` : List of tagnames. Wildcards are not allowed.

  Tags with maps (relevant for some InfoPlus.21 servers) can be on the form `'tag;map'` , e.g. `'109-HIC005;CS A_AUTO'` .

* `start_time` : Start of time period. 
* `stop_time` : End of time period. 

  Both `start_time` and `stop time` can be either string (which will be interpreted by [pandas. Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) or datetime object.

* `ts` : The interval between samples when querying interpolated or aggregated data.
* `read_type` (optional): What kind of data to read. **Default*** Interpolated. 

## Selecting what to read

By specifying the optional parameter `read_type` to `read_tags()`, it is possible to specify what kind of data should be returned. The default query method is interpolated. All valid values for `read_type` are defined in the `utils.ReaderType` class (mirrored for convenience as `tagreader.ReaderType`), although not all are currently implemented. Below is the list of implemented read types. 

* `INT` : The raw data points are interpolated so that one new data point is generated at each step of length `ts` starting at `start_time` and ending at or less than `ts` seconds before `stop_time` .
* The following aggregated read types perform a weighted calculation of the raw data within each interval. Where relevant, time-weighted calculations are used. Returned time stamps are anchored at the beginning of each interval. So for the 60 seconds long interval between 08:11:00 and 08:12:00, the time stamp will be 08:11:00.
  + `MIN` : The minimum value.
  + `MAX` : The maximum value.
  + `AVG` : The average value.
  + `VAR` : The variance.
  + `STD` : The standard deviation.
  + `RNG` : The range (max-min).

**Examples**

Read interpolated data for the provided tag with 3-minute intervals between the two time stamps:
``` python 
c = tagreader. IMSClient("PINO", "pi")
c.connect()
df = c.read(['BA: ACTIVE.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180)
```
Read the average value for the two provided tags within each 3-minute interval between the two time stamps:
``` python
df = c.read(['BA: CONC.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180, read_type=tagreader.ReaderType.AVG)
``` 

## Caching results

By default a cache-file using the HDF5 file format will be attached to the client upon client creation. Whenever `read_tags()` is called, the cache is queried for existing data. Any data that is not already in the cache will be queried from the data source. The cache can significantly speed up queries, and it is therefore recommended to always keep it enabled. The cache file will be created on use.

Data in the cache never expires. If the data for some reason becomes invalid, then the cache and data source will no longer produce the same data set. An existing cache file can safely be deleted at any time, at least as long as there is no ongoing query.

If, for any reason, you want to disable the cache, simply set it to `None` . This can be done at any time, but is normally done before connecting to the server, like this:

``` python
c = tagreader.IMSClient("PINO", "pi")
c.cache = None
c.connect()
```

## Time zones

It is important to understand how Tagreader uses and interprets time zones. Queries to the backend servers are always performed in UTC time, and return data is also always in UTC. However, it is usually not convenient to ensure all time stamps are in UTC time. The client and handlers therefore have functionality for converting between UTC and user-specified time zones.

There are two levels of determining which time zone input arguments should be interpreted as, and which time zone return data should be converted to:

1. Time zone aware input arguments will use their corresponding time zone.
2. Time zone naive input arguments are assumed to have time zone as provided by the client. 

The client-provided time zone can be specified with the optional `tz` argument (string, e.g. "*US/Central*") during client creation. If it is not specified, then the default value *Europe/Oslo* is used. Note that for the most common use case where Equinor employees want to fetch data from Norwegian assets and display them with Norwegian time stamps, nothing needs to be done.

**Example (advanced usage)**

An employee in Houston is contacted by her collague in Brazil about an event that she needs to investigate. The colleague identified the time of the event at July 20th 2020 at 15:05:00 Rio time. The Houston employee wishes to extract interpolated data with 60-second intervals and display the data in her local time zone. She also wishes to send the data to her Norwegian colleague with datestamps in Norwegian time. One way of doing this is :

``` python
import tagreader
from datetime import datetime, timedelta
from dateutil import tz
c = tagreader.IMSClient("PINO", "pi", tz="US/Central")  # Force output data to Houston time
c.connect()
tzinfo = tz.gettz("Brazil/East")  # Generate timezone object for Rio local time
event_time = datetime(2020, 7, 20, 15, 5, 0, tzinfo=tzinfo)
start_time = event_time - timedelta(minutes=30)
end_time = event_time + timedelta(minutes=10)
df = c.read_tags(["BA:CONC.1"], start_time, end_time, ts=60)
df_to_Norway = df.tz_convert("Europe/Oslo")  # Create a copy of the dataframe with Norwegian time stamps
```

# Fetching metadata

Two client methods have been created to fetch basic metadata for one or more tags.

## get_units()

Fetches the engineering unit(s) for the tag(s) provided. The argument `tags` can be either a single tagname as string, or a list of tagnames.

## get_description()
Fetches the description(s) for the tag(s) provided. The argument `tags` can be either a single tagname as string, or a list of tagnames.

**Example**:
``` python
tags = ["BA:ACTIVE.1", "BA:LEVEL.1", "BA:CONC.1"]
units = c.get_units(tags)
desc = c.get_descriptions(tags)
tag = "BA:CONC.1"
df[tag].plot(grid=True, title=desc[tag]).set_ylabel(units[tag])
```
