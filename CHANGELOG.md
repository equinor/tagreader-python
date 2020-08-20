# Changelog
 
## [1.1.0] - 2020-06-18
Improved handling of tags with maps for Aspen IP.21.

### Fixed
- Allow tag search with tagname and description.
- Fix fetching of correct unit for tags with map.
- Fix fetching of correct description for tags with map.

## [1.0.1] - 2020-04-16
### Fixed
- Fix FutureWarning regarding use of keep_tz=False in DateTimeIndex.to_series()

## [1.0.0] - 2020-04-16
First release on PyPI as `tagreader`.
### Changed
- Rename from pyIMS to tagreader to avoid name collision on PyPI.
- Move lots of tests to a separate private repository. Those tests need to be run on-site with functioning server connection to PI/IP.21 servers, and are currently handled in Azure Pipelines.

## [0.2.0] - 2019-10-08
Last release under the name `pyIMS`.
### Changed
- Major rewrite to accommodate for other types of connections than ODBC in the future.
- **Breaking**: It is now necessary to specify imstype (`pi` or `aspen`) when initiating a client.
- Update to new default DAS server address. 

### Fixed
- Force keep_tz=False to suppress FutureWarning in Pandas >= 0.24.
- Fix reading of digital states from PI. 

### Added
- Add get_units() and get_descriptions() methods to fetch engineering units and description for
specified tags. 
- Enable tag search by description.
- Package version made available in `pyims.__version__`.

## [0.0.8] - 2019-01-17
### Changed
- Improve documentation.
- Add commonly requested documentation, including docstring, for specifying query method for 
read_tags.

## [0.0.7] - 2018-06-18
### Fixed
- Fix bug where timezone was locked to Europe/Oslo.
- Discovered that Pandas >=0.23 is required. 0.22 causes a weird timezone bug during DST folds when 
using cache.

## [0.0.6] - 2018-06-08
### Added
- Aspen now supports avg, min, max, rng, std and var queries. These have timestamps in 
the middle of the period.

### Changed
- Start preparation for supporting more read types for both Aspen and PI.
- Define max_rows = 100000 for PI to (hopefully) avoid timeouts.

### Fixed
- Fix indexing issue (duplicate indices appeared in some cases, but are now handled).

## [0.0.5] - 2018-06-07
### Added
- Introduce time zones. Start- and stop time for queries are always performed relative to server 
time, so we have to know where the server is located. The default time zone is "Europe/Oslo", but 
this can be changed for e.g. Peregrino by specifying `tz="America/Sao_Paulo"` upon initialization 
of the client.

### Changed
- Cache files produced with versions < 0.0.5 must be deleted since they are not compatible the time zones.
- Interpolated queries for IP.21 now return extrapolated values after last valid datapoint.

### Fixed
- Properly handle transitions to and from DST (Daylight savings time). 
- Queries should now always return data for exactly `start_time <= time <= stop_time`. Off-by-one data 
(e.g. no data for `stop_time` or data for `stop_time+ts`) is a bug (except for PI queries with 
`stop_time` close to DST changes, in which case PI may act silly).

## [0.0.4] - 2018-06-01
### Fixed
- Fix cache sorting issue when reading across two segments that were previously written in 
nonchronological order. 

## [0.0.3] - 2018-05-30
### Added 
- Add support for tags with mappings (relevant for SNA and SNB) by specifying tags on the form `tag;map`
- Fix NaturalNameWarning when using '.' in tagnames

## 0.0.2 - 2018-04-13
Initial release. Supports the most common tag read queries for IP.21 and PI. 

[1.1.0]: https://github.com/equinor/tagreader-python/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/equinor/tagreader-python/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/equinor/tagreader-python/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/equinor/tagreader-python/compare/v0.0.8...v0.2.0
[0.0.8]: https://github.com/equinor/tagreader-python/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/equinor/tagreader-python/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/equinor/tagreader-python/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/equinor/tagreader-python/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/equinor/tagreader-python/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/equinor/tagreader-python/compare/v0.0.2...v0.0.3
