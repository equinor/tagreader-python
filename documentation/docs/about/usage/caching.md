---
sidebar_position: 4
---

# Caching results

By default, a cache-file using the SQLite file format will be attached to the client upon client creation. Whenever 
`IMSClient.read()` is called, the cache is queried for existing data. Any data that is not already in the cache will be 
queried from the data source. The cache can significantly speed up queries, and it is therefore recommended to always 
keep it enabled. The cache file will be created on use.

Data in the cache never expires. If the data for some reason becomes invalid, then the cache and data source will no 
longer produce the same data set. An existing cache file can safely be deleted at any time, at least as long as there 
is no ongoing query.

If, for any reason, you want to disable the cache, simply set it to `None` . This can be done at any time, but is 
normally done before connecting to the server, like this:

``` python
c = tagreader.IMSClient("PINO", "pi")
c.cache = None
c.connect()
```

Snapshots ( `read_type = ReaderType.SNAPSHOT` ) are of course never cached.

**Note**: Raw `read_type = ReaderType.RAW` data values are currently not cached pending a rewrite of the caching 
mechanisms.
**Note**: Cache will be default off from version 5.