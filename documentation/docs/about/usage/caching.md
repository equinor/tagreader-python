---
sidebar_position: 4
---

# Caching results

It is possible to cache data locally using SQLite-files that will be attached to the client upon client creation. Whenever
`IMSClient.read()` is called, the cache is queried for existing data. Any data that is not already in the cache will be
queried from the data source. The cache can significantly speed up queries, and it is therefore recommended to always
have it enabled. The cache file will be created on use.

Data in the cache never expires. If the data for some reason becomes invalid, then the cache and data source will no
longer produce the same data set. An existing cache file can safely be deleted at any time, at least as long as there
is no ongoing query.

If, for any reason, you want to disable the cache, simply set it to the default value `None`.

``` python
c = tagreader.IMSClient("PINO", "pi", cache=None)
c.connect()
```

If you want to cache data, we recommend using the provided SmartCache like this:

``` python
from pathlib import Path
from tagreader.cache import SmartCache

c = tagreader.IMSClient("PINO", "pi", cache=SmartCache(path=Path(".cache"))
c.connect()
```

Snapshots ( `read_type = ReaderType.SNAPSHOT` ) are of course never cached.

**Note**: Raw `read_type = ReaderType.RAW` data values are currently not cached pending a rewrite of the caching
mechanisms.