---
sidebar_position: 6
---

# Time zones

It is important to understand how Tagreader uses and interprets time zones. Queries to the backend servers are always
performed in UTC time, and return data is also always in UTC. However, it is usually not convenient to ensure all time
stamps are in UTC time. The client and handlers therefore have functionality for converting between UTC and
user-specified time zones.

There are two levels of determining which time zone input arguments should be interpreted as, and which time zone
return data should be converted to:

1. Time zone aware input arguments will use their corresponding time zone.
2. Time zone naive input arguments are assumed to have time zone as provided by the client.

The client-provided time zone can be specified with the optional `tz` argument (string, e.g. "*US/Central*") during
client creation. If it is not specified, then the default value *Europe/Oslo* is used. Note that for the most common
use case where Equinor employees want to fetch data from Norwegian assets and display them with Norwegian time stamps,
nothing needs to be done.

*Note:* It is a good idea to update the `pytz` package rather frequently (at least twice per year) to ensure that time
zone information is up-to-date. `pip install --upgrade pytz` .

**Example (advanced usage)**

An employee in Houston is contacted by her colleague in Brazil about an event that she needs to investigate.
The colleague identified the time of the event at July 20th 2020 at 15:05:00 Rio time. The Houston employee wishes to
extract interpolated data with 60-second intervals and display the data in her local time zone. She also wishes to send
the data to her Norwegian colleague with datestamps in Norwegian time. One way of doing this is :

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