# 0.0.5
* Now properly handles DST (Daylight savings time), both to and from. 
* Introduced time zones. Start- and stop time for queries are always performed relative to server 
time, so we have to know where the server is located. The default time zone is "Europe/Oslo", but 
this can be changed for e.g. Peregrino by specifying `tz="America/Sao_Paulo"` upon initialization 
of the client.
* Cache files produced with versions < 0.0.5 may produce inconsistent data and should be deleted.
* Queries should always return data for exactly `start_time <= time <= stop_time`. Off-by-one data 
(e.g. no data for `stop_time` or data for `stop_time+ts`) is a bug (except for PI queries with 
`stop_time` close to DST changes, in which case PI may act silly).
* Interpolated queries for IP.21 now return values after last valid datapoint.
 
# 0.0.4 
* Fixed cache sorting issue when reading across two segments that were previously written in nonchronological order. 

# 0.0.3
* Added support for tags with mappings (relevant for SNA and SNB) by specifying tags on the form `tag;map`
* Fixed NaturalNameWarning when using '.' in tagnames

# 0.0.2
* Initial public release. Supports the most common tag read queries for IP.21 and PI. 