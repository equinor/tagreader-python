# Quickstart
# This script provides a quick demonstration of the basic usage of tagreader. It will show you the steps from importing the package to fetching data and making a plot. Some cells contain links to more details that can be found in the [manual](../docs/manual.md).

# Start by importing the package:
from tagreader import IMSClient, list_sources

# If we don't know the name of the data source, we can check which PI (piwebapi) and IP.21 (aspenone) servers we have access to via Web API ([more details](https://equinor.github.io/tagreader-python/docs/about/usage/data-source)):
print(list_sources("aspenone"))

# Let's establish a web API connection to PINO.
c = IMSClient(datasource="TRB")

# After connecting, we can search for a tag ([more details](../docs/manual.md#searching-for-tags)):
tags = list(map(str, c.search("AverageC*", return_desc=False)))

# Selecting three of the tags found above, we can read values for a duration of 3.5 hours starting January 5th at 8 in the morning with 3-minute (180-seconds) intervals. The default query method is interpolated, but several other methods are available by providing the `read_type` argument. Timestamps are parsed using [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html), and can therefore be provided in many different string formats. [More details](../docs/manual.md#reading_data)
df = c.read(
    tags,
    "05-Jan-2024 08:00:00",
    "15/01/24 11:30am",
    180,
)
# *Note*: Tags with maps (relevant for some InfoPlus.21 servers) can be specified on the form `'tag;map'`, e.g. `'17B-HIC192;CS A_AUTO'`.

# The result from the last call is a Pandas dataframe, and can be manipulated as such:
print(df.tail())
print(df["AverageCPUTimeTest"].size)
print(df["AverageCPUTimeVals"].loc["2024-01-05 11:24:00"])
print(max(df["AverageCPUTimeVals"]))

# Sometimes it can be handy to obtain the unit and description for the tags:
units = c.get_units(tags)
desc = c.get_descriptions(tags)
print(units)
print(desc)

# Requires matplotlib to be installed.
# tag = tags[0]
# df[tag].plot(grid=True, title=desc[tag]).set_ylabel(units[tag])
