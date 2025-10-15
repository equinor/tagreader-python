---
sidebar_position: 1
---
# Basic usage

The module is imported with

``` python
import tagreader
```

## The Client

The client presents the interface for communicating with the data source to the user. The interface shall be as unified
as possible, regardless of the IMS type that is used. A handler object specifically designed for each IMS type is
attached to the client when the client is created. The handler is responsible for handling the communication and data
interpretation between the server and the client object.

:::info SSL verification

Equinor root certificates are automatically added when using an Equinor Managed computer, which allow SSL verification.

For non-Equinor users: If you run info SSL verification errors and prefer to not set `verify_ssl=False` ,
you can try the procedure outlined [here](https://incognitjoe.github.io/adding-certs-to-requests.html).
:::

:::info ODBC support
Tagreader as of version 5 does no longer support ODBC clients, which has been deprecated in favor of REST services.
To use ODBC, please refer to [Tagreader v4 on PyPI](https://pypi.org/project/tagreader/#history).
Versioned documentation is available in the source code on [GitHub Releases](https://github.com/equinor/tagreader-python/releases).

Use at your own discretion.
:::

## Creating a client

A connection to a data source is prepared by creating an instance of `tagreader.IMSClient` with the following input
arguments:

* `datasource` : Name of data source
* `imstype` : The name of the [IMS type](/docs/about/usage/data-source) to query. Indicates the type of data source
that is requested, and therefore determines which handler type to use. Valid values are
`piwebapi` and `aspenone`.

* `tz` (optional): Time zone naive time stamps will be interpreted as belonging to this time zone. Similarly,
the returned data points will be localized to this time zone. **Default**: _"Europe/Oslo"_.

The following input arguments can be used when connecting to either `piwebapi` or to `aspenone`. None of these
should be necessary to supply when connecting to Equinor servers.

* `url` (optional): Path to server root, e.g. _"https://aspenone/ProcessData/AtProcessDataREST.dll"_
or _"https://piwebapi/piwebapi"_. **Default**: Path to Equinor server corresponding to selected `imstype`.
* `verify_ssl` (optional): Whether to verify SSL certificate sent from server. **Default**: `True`.
* `auth` (optional): Auth object to pass to the server for authentication. **Default**: Kerberos-based auth object
that works with Equinor servers.
* `cache` (optional): [Cache](caching.md) data locally in order to avoid re-reading the same data multiple times.

## Connecting to data source

After creating the client as described above, connect to the server with the `connect()` method.

**Example**

Connecting to the PINO PI data source using REST Web API:

``` python
c = tagreader.IMSClient("PINO", "piwebapi")
c.connect()
```

Connecting to the Peregrino IP.21 data source using AspenTech Process Data REST Web API, specifying that all naive time
stamps as well as the returned data shall use Rio local time, and using the local endpoint in Brazil:

``` python
c = tagreader.IMSClient(datasource="PER",
                        imstype="aspenone",
                        tz="Brazil/East",
                        url="https://aspenone-per.equinor.com/ProcessExplorer/ProcessData/AtProcessDataREST.dll")
c.connect()
```

Connecting to some other AspenTech Web API URL using NTLM authentication instead of default Kerberos and ignoring the
server's host certificate:

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
                        verify_ssl=False)
c.connect()
```

## Searching for tags

The client method `search()` can be used to search for tags using either tag name, tag description or both.

Supply at least one of the following arguments:

* `tag` : Name of tag
* `desc` : Description of tag

If both arguments are provided, the both must match.

`*` can be used as wildcard.

**Examples**

``` python
c = tagreader.IMSClient("PINO", "piwebapi")
c.connect()
c.search("cd*158")
c.search(desc="*reactor*")
c.search(tag="BA:*", desc="*Temperature*")
```

## Reading data

Data is read by calling the client method `read()` with the following input arguments:

* `tags` : List of tagnames. Wildcards are not allowed.

  Tags with maps (relevant for some InfoPlus.21 servers) can be on the form `'tag;map'` , e.g. `'109-HIC005;CS A_AUTO'`.

* `start_time` : Start of time period.
* `end_time` : End of time period.

  Both `start_time` and `end_time` can be either datetime object or string. Strings are interpreted by the
* [Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html) method from Pandas.
Both timestamps can be left out when `read_type = ReaderType.SNAPSHOT` . However, when using either of the Web APIs, `end_time` provides the time at which the snapshot is taken.

* `ts` : The interval between samples when querying interpolated or aggregated data. Ignored and can be left out when
`read_type = ReaderType.SNAPSHOT` . **Default** 60 seconds.
* `read_type` (optional): What kind of data to read. More info immediately below. **Default** Interpolated.
* `get_status` (optonal): When set to `True` will fetch status information in addition to values. **Default** `False`.

## Selecting what to read

By specifying the optional parameter `read_type` to `read()` , it is possible to specify what kind of data should be
returned. The default query method is interpolated. All valid values for `read_type` are defined in the
`utils.ReaderType` class (mirrored for convenience as `tagreader.ReaderType` ), although not all are currently
implemented. Below is the list of implemented read types.

* `INT` : The raw data points are interpolated so that one new data point is generated at each step of length `ts`
* starting at `start_time` and ending at or less than `ts` seconds before `end_time` .
* The following aggregated read types perform a weighted calculation of the raw data within each interval.
Where relevant, time-weighted calculations are used. Returned time stamps are anchored at the beginning of each
interval. So for the 60 seconds long interval between 08:11:00 and 08:12:00, the time stamp will be 08:11:00.
  + `MIN` : The minimum value.
  + `MAX` : The maximum value.
  + `AVG` : The average value.
  + `VAR` : The variance.
  + `STD` : The standard deviation.
  + `RNG` : The range (max-min).
* `RAW` : Returns actual data points stored in the database.
* `SNAPSHOT` : Returns the last recorded value. Only one tag can be read at a time. When using either of the Web API
based handlers, providing `end_time` is possible in which case a snapshot at the specific time is returned.

**Examples**

Read interpolated data for the provided tag with 3-minute intervals between the two time stamps:

``` python
c = tagreader.IMSClient("PINO", "piwebapi")
c.connect()
df = c.read(['BA:ACTIVE.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180)

```

Read the average value for the two provided tags within each 3-minute interval between the two time stamps:

``` python
df = c.read(['BA:CONC.1'], '05-Jan-2020 08:00:00', '05/01/20 11:30am', 180, read_type=tagreader.ReaderType.AVG)
```

## Status information

The optional parameter `get_status` was added to `IMSClient.read()` in release 2.6.0. If set to `True`, the resulting
dataframe will be expanded with one additional column per tag. The column contains integer numbers that indicate the
status, or quality, of the returned values.

In an effort to unify the status value for all IMS types, the following schema based on AspenTech was selected:

0: Good
1: Suspect
2: Bad
4: Good/Modified
5: Suspect/Modified
6: Bad/Modified

The status value is obtained differently for the four IMS types:
* Aspen Web API: Read directly from the `l` ("Level") field in the json output.
* PI Web API: Calculated as `Questionable` + 2 * (1 - `Good`) + 4 * `Substituted`.
negative for various reasons for being bad.

For the two PI IMS types, it is assumed that `Questionable` is never `True` if `Good` is `False` or `status != 0`.
This may be an incorrect assumption with resulting erroneous status value.

In summary, here is the resulting status value from tagreader for different combinations of status field values from
the IMS types:

| tagreader | Aspen Web API | PI Web API                                                         |
|:---------:|:-------------:|:-------------------------------------------------------------------|
|     0     |     l = 0     | Good = True<br /> Questionable = False<br /> Substituted = False   |
|     1     |     l = 1     | Good = True<br /> Questionable = True<br /> Substituted = False    |
|     2     |     l = 2     | Good = False<br /> Questionable = False<br /> Substituted = False  |
|     4     |     l = 4     | Good = True<br /> Questionable = False<br /> Substituted = True    |
|     5     |     l = 5     | Good = True<br /> Questionable = True<br /> Substituted = True     |
|     6     |     l = 6     | Good = False<br /> Questionable = False<br /> Substituted = True   |

Please keep in mind when using `get_status`:
* This is an experimental feature. It may work as intended, or it may result in erroneous status values in some cases.
If that happens, please create an issue.
* Both how fetching status is activated and how it is returned may be changed at a later time.
