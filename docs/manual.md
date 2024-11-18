# Tagreader-python <!-- omit in toc -->

Tagreader is a Python package for reading trend data from the OSIsoft PI and AspenTech InfoPlus.21 IMS systems. It can communicate with PI Web API, and with IP.21 using Process Data REST Web API.

Tagreader is intended to be easy to use, and present the same interface to the user regardless of IMS system and connection method.

# Index <!-- omit in toc -->

- [Requirements](#requirements)
- [Before getting started](#before-getting-started)
- [Installation](#installation)
  - [Adding host certificates](#adding-host-certificates)
    - [For Equinor users](#for-equinor-users)
    - [For non-Equinor users](#for-non-equinor-users)
- [Importing the module](#importing-the-module)
- [IMS types](#ims-types)
- [Listing available data sources](#listing-available-data-sources)
- [The Client](#the-client)
  - [Creating a client](#creating-a-client)
  - [Connecting to data source](#connecting-to-data-source)
- [Searching for tags](#searching-for-tags)
- [Reading data](#reading-data)
  - [Selecting what to read](#selecting-what-to-read)
  - [Status information](#status-information)
  - [Caching results](#caching-results)
  - [Time zones](#time-zones)
- [Fetching metadata](#fetching-metadata)
  - [get_units()](#getunits)
  - [get_description()](#getdescription)
- [Performing raw queries](#performing-raw-queries)

# Requirements

Python >= 3.8 with the following packages:

  + pandas >= 1.0.0
  + diskcache
  + requests
  + requests_kerberos

# Before getting started

It is highly recommended to go through the [quickstart](../examples/quickstart.ipynb) example. It contains references to relevant sections in this manual.

# Installation

To install and/or upgrade:

```
pip install --upgrade tagreader
```

## Adding host certificates

### For Equinor users

***Note**: Since v2.7.0 the procedure described below will be automatically performed on Equinor hosts when importing the tagreader module. It should therefore no longer be necessary to perform this step manually.*

The Web APIs are queried with the `requests` package. `requests` does not utilize the system certificate store, but instead relies on the `certifi` bundle. In order to avoid SSL verification errors, we need to either turn off SSL verification (optional input argument `verifySSL=False` for relevant function calls) or, preferably, add the certificate to the `certifi` bundle. To do this, simply activate the virtual environment where you installed `tagreader`, and run the following snippet:

``` python
from tagreader.utils import add_equinor_root_certificate
    add_equinor_root_certificate()
```

The output should inform you that the certificate was successfully added. This needs to be repeated whenever certifi is upgraded in your python virtual environment. It is safe to run more than once: If the function detects that the certificate has already been added to your current certifi installation, the certificate will not be duplicated.

### For non-Equinor users

If you run info SSL verification errors and prefer to not set `verifySSL=False` , you can try the procedure outlined [here](https://incognitjoe.github.io/adding-certs-to-requests.html).

# Importing the module

The module is imported with

``` python
import tagreader
```

# IMS types

Tagreader supports connecting to PI and IP.21 servers using Web API interfaces. When calling certain methods, the user will need to tell tagreader which system and which connection method to use. This input argument is called `imstype` , and can be one of the following case-insensitive strings:

* `piwebapi` : For connecting to OSISoft PI Web API
* `aspenone` : For connecting to AspenTech Process Data REST Web API

# Listing available data sources

The method `tagreader.list_sources()` can query for available PI and IP.21 servers available through Web API. Input arguments:

* `imstype` (optional) : The name of the [IMS type](#ims-types) to query. Valid values: `piwebapi` and `aspenone`.

The following input arguments are only relevant when calling `list_sources()` with a Web API `imstype` ( `piwebapi` or `aspenone` ):

* `url` (optional): Path to server root, e.g. _"https:<span>//aspenone/ProcessData/AtProcessDataREST.dll"_ or _"https:<span>//piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` if `imstype` is `piwebapi` or `aspenone` .
* `verifySSL` (optional): Whether to verify SSL certificate sent from server. **Default**: `True`.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth objects that work with Equinor servers. If not connecting to an Equinor server, you may have to create your own auth.

**Example:**

``` python
from tagreader import list_sources
list_sources("aspenone")
list_sources("piwebapi")
```

When called with `imstype` set to `piwebapi` or `aspenone`, `list_sources()` will connect to the web server URL and query for the available list of data sources. This list is normally the complete set of data sources available on the server, and does not indicate whether the user is authorized to query the source or not.

When querying Equinor Web API for data sources, `list_sources()` should require no input argument except `imstype="piwebapi"` or `imstype="aspenone"`. For non-Equinor servers, `url` will need to be specified, as may `auth` and `verifySSL` .

# The Client

The client presents the interface for communicating with the data source to the user. The interface shall be as unified as possible, regardless of the IMS type that is used. A handler object specifically designed for each IMS type is attached to the client when the client is created. The handler is responsible for handling the communication and data interpretation between the server and the client object.

## Creating a client

A connection to a data source is prepared by creating an instance of `tagreader.IMSClient` with the following input arguments:

* `datasource` : Name of data source
* `imstype` (optional): The name of the [IMS type](#ims-types) to query. Indicates the type of data source that is requested, and therefore determines which handler type to use. Valid values are `piwebapi` and `aspenone`. If not provided it will search the availble sources and find the type.
* `tz` (optional): Time zone naive time stamps will be interpreted as belonging to this time zone. Similarly, the returned data points will be localized to this time zone. **Default**: _"Europe/Oslo"_.

The following input arguments can be used when connecting to either `piwebapi` or to `aspenone`. None of these should be necessary to supply when connecting to Equinor servers.

* `url` (optional): Path to server root, e.g. _"https:<span>//aspenone/ProcessData/AtProcessDataREST.dll"_ or _"https:<span>//piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype` .
* `verifySSL` (optional): Whether to verify SSL certificate sent from server. **Default**: `True`.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth object that works with Equinor servers.

## Connecting to data source

After creating the client as described above, connect to the server with the `connect()` method.

**Example**

Connecting to the PINO PI data source using PI webapi:

``` python
c = tagreader.IMSClient("PINO")
```

Connecting to the Peregrino IP.21 data source using AspenTech Process Data REST Web API, specifying that all naive time stamps as well as the returned data shall use Rio local time, and using the local endpoint in Brazil:

``` python
c = tagreader.IMSClient(datasource="PER",
                        imstype="aspenone",
                        tz="Brazil/East",
                        url="https://aspenone-per.equinor.com/ProcessExplorer/ProcessData/AtProcessDataREST.dll")
c.connect()
```

Connecting to some other AspenTech Web API URL using NTLM authentication instead of default Kerberos and ignoring the server's host certificate:

``` python
import getpass
from requests_ntlm import HttpNtlmAuth
user = "mydomain\\" + getpass.getuser()
pwd = getpass.getpass()
auth = HttpNtlmAuth(user, pwd)
c = tagreader.IMSClient(datasource="myplant",
                        url="https://api.mycompany.com/aspenone",
                        imstype="aspenone",
                        auth=auth,
                        verifySSL=False)
c.connect()
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
c = tagreader.IMSClient("PINO")
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
* `end_time` : End of time period.

  Both `start_time` and `end_time` can be either datetime object or string. Strings are interpreted by the [Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) method from Pandas. Both timestamps can be left out when `read_type = ReaderType.SNAPSHOT` . However, when using either of the Web APIs, `end_time` provides the time at which the snapshot is taken.

* `ts` : The interval between samples when querying interpolated or aggregated data. Ignored and can be left out when `read_type = ReaderType.SNAPSHOT` . **Default** 60 seconds.
* `read_type` (optional): What kind of data to read. More info immediately below. **Default** Interpolated.
* `get_status` (optonal): When set to `True` will fetch status information in addition to values. **Default** `False`.

## Selecting what to read

By specifying the optional parameter `read_type` to `read()` , it is possible to specify what kind of data should be returned. The default query method is interpolated. All valid values for `read_type` are defined in the `utils.ReaderType` class (mirrored for convenience as `tagreader.ReaderType` ), although not all are currently implemented. Below is the list of implemented read types.

* `INT` : The raw data points are interpolated so that one new data point is generated at each step of length `ts` starting at `start_time` and ending at or less than `ts` seconds before `end_time` .
* The following aggregated read types perform a weighted calculation of the raw data within each interval. Where relevant, time-weighted calculations are used. Returned time stamps are anchored at the beginning of each interval. So for the 60 seconds long interval between 08:11:00 and 08:12:00, the time stamp will be 08:11:00.
  + `MIN` : The minimum value.
  + `MAX` : The maximum value.
  + `AVG` : The average value.
  + `VAR` : The variance.
  + `STD` : The standard deviation.
  + `RNG` : The range (max-min).
* `RAW` : Returns actual data points stored in the database.
* `SNAPSHOT` : Returns the last recorded value. Only one tag can be read at a time. When using either of the Web API based handlers, providing `end_time` is possible in which case a snapshot at the specific time is returned.

**Examples**

Read interpolated data for the provided tag with 3-minute intervals between the two time stamps:

``` python
import tagreader
c = tagreader.IMSClient("PINO")
c.connect()
df = c.read(['BA:ACTIVE.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180)

```

Read the average value for the two provided tags within each 3-minute interval between the two time stamps:

``` python
df = c.read(['BA:CONC.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180, read_type=tagreader.ReaderType.AVG)
```

## Status information

The optional parameter `get_status` was added to `IMSClient.read()` in release 2.6.0. If set to `True`, the resulting dataframe will be expanded with one additional column per tag. The column contains integer numbers that indicate the status, or quality, of the returned values.

In an effort to unify the status value for all IMS types, the following schema based on AspenTech was selected:

0: Good
1: Suspect
2: Bad
4: Good/Modified
5: Suspect/Modified
6: Bad/Modified

The status value is obtained differently for the four IMS types:
* Aspen Web API: Read directly from the `l` ("Level") field in the json output.
* Aspen ODBC: Read directly from the `status` field in the table.
* PI Web API: Calculated as `Questionable` + 2 * (1 - `Good`) + 4 * `Substituted`.
* PI ODBC: Calculated as `questionable` + 2 * (`status` != 0) + 4 * `substituted`. `status` is 0 for good, positive or negative for various reasons for being bad.

For the two PI IMS types, it is assumed that `Questionable` is never `True` if `Good` is `False` or `status != 0`. This may be an incorrect assumption with resulting erroneous status value.

In summary, here is the resulting status value from tagreader for different combinations of status field values from the IMS types:

| tagreader | Aspen Web API | Aspen ODBC | PI Web API                                                        | PI ODBC                                                          |
| :-------: | :-----------: | :--------: | ----------------------------------------------------------------- | ---------------------------------------------------------------- |
|     0     |     l = 0     | status = 0 | Good = True<br /> Questionable = False<br /> Substituted = False  | status = 0<br /> questionable = False<br /> substituted = False  |
|     1     |     l = 1     | status = 1 | Good = True<br /> Questionable = True<br /> Substituted = False   | status = 0<br /> questionable = True<br /> substituted = False   |
|     2     |     l = 2     | status = 2 | Good = False<br /> Questionable = False<br /> Substituted = False | status != 0<br /> questionable = False<br /> substituted = False |
|     4     |     l = 4     | status = 4 | Good = True<br /> Questionable = False<br /> Substituted = True   | status = 0<br /> questionable = False<br /> substituted = True   |
|     5     |     l = 5     | status = 5 | Good = True<br /> Questionable = True<br /> Substituted = True    | status = 0<br /> questionable = True<br /> substituted = True    |
|     6     |     l = 6     | status = 6 | Good = False<br /> Questionable = False<br /> Substituted = True  | status != 0<br /> questionable = False<br /> substituted = True  |

Please keep in mind when using `get_status`:
* This is an experimental feature. It may work as intended, or it may result in erroneous status values in some cases. If that happens, please create an issue.
* Both how fetching status is activated and how it is returned may be changed at a later time.

## Caching results

By default, a cache-file using the SQLite file format will be attached to the client upon client creation. Whenever `IMSClient.read()` is called, the cache is queried for existing data. Any data that is not already in the cache will be queried from the data source. The cache can significantly speed up queries, and it is therefore recommended to always keep it enabled. The cache file will be created on use.

Data in the cache never expires. If the data for some reason becomes invalid, then the cache and data source will no longer produce the same data set. An existing cache file can safely be deleted at any time, at least as long as there is no ongoing query.

If, for any reason, you want to disable the cache, simply set it to `None` . This can be done at any time, but is normally done before connecting to the server, like this:

``` python
c = tagreader.IMSClient("PINO")
c.cache = None
c.connect()
```

Snapshots ( `read_type = ReaderType.SNAPSHOT` ) are of course never cached.

**Note**: Raw `read_type = ReaderType.RAW` data values are currently not cached pending a rewrite of the caching mechanisms.
**Note**: Cache will be default off from version 5.

## Time zones

It is important to understand how Tagreader uses and interprets time zones. Queries to the backend servers are always performed in UTC time, and return data is also always in UTC. However, it is usually not convenient to ensure all time stamps are in UTC time. The client and handlers therefore have functionality for converting between UTC and user-specified time zones.

There are two levels of determining which time zone input arguments should be interpreted as, and which time zone return data should be converted to:

1. Time zone aware input arguments will use their corresponding time zone.
2. Time zone naive input arguments are assumed to have time zone as provided by the client.

The client-provided time zone can be specified with the optional `tz` argument (string, e.g. "*US/Central*") during client creation. If it is not specified, then the default value *Europe/Oslo* is used. Note that for the most common use case where Equinor employees want to fetch data from Norwegian assets and display them with Norwegian time stamps, nothing needs to be done.

*Note:* It is a good idea to update the `pytz` package rather frequently (at least twice per year) to ensure that time zone information is up to date. `pip install --upgrade pytz` .

**Example (advanced usage)**

An employee in Houston is contacted by her colleague in Brazil about an event that she needs to investigate. The colleague identified the time of the event at July 20th 2020 at 15:05:00 Rio time. The Houston employee wishes to extract interpolated data with 60-second intervals and display the data in her local time zone. She also wishes to send the data to her Norwegian colleague with datestamps in Norwegian time. One way of doing this is :

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
df = c.read(["BA:CONC.1"], start_time, end_time, ts=60)
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
