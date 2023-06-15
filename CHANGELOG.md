# Changelog

This changelog is deprecated. All changes are documented under [releases](https://github.com/equinor/tagreader-python/releases).

## [4.1.1](https://github.com/equinor/tagreader-python/compare/v4.1.0...v4.1.1) (2023-06-14)


### 📚 Documentation

* add homepage and repo to pyproject.toml ([5e90bee](https://github.com/equinor/tagreader-python/commit/5e90beedead882948d0f0b6db189f23bbc71f814))

## [4.1.0](https://github.com/equinor/tagreader-python/compare/v4.0.3...v4.1.0) (2023-06-14)


### 🔨 Refactor

* make code pass mypy and add strict type validation ([#190](https://github.com/equinor/tagreader-python/issues/190)) ([9efc942](https://github.com/equinor/tagreader-python/commit/9efc942eb6580e8c629e0a4bb5b2bb24af871f6f))


### ✨ Features

* add persistent web-id cache for PIWebAPI ([64eb76c](https://github.com/equinor/tagreader-python/commit/64eb76cd90e9e2d4091ebf97b716be53c9bde7ec))
* use diskcache as caching backend ([f7f7a5f](https://github.com/equinor/tagreader-python/commit/f7f7a5ff0edefd21eeb60b41dbd94d6726cd0caf)), closes [#205](https://github.com/equinor/tagreader-python/issues/205)


### 🐛 Bug Fixes

* do not throw error when start and end time is submitted when using SNAPSHOT and ODBC connectors ([#213](https://github.com/equinor/tagreader-python/issues/213)) ([4e9a4c9](https://github.com/equinor/tagreader-python/commit/4e9a4c9011a05be3f73fa5719ef94ea30afe38e0))


### 👷 CI/CD

* correct PyPi test upload ([8217af6](https://github.com/equinor/tagreader-python/commit/8217af6bd6587c9ed6380f132b74abe297f143f6))

## [4.0.3](https://github.com/equinor/tagreader-python/compare/v4.0.2...v4.0.3) (2023-06-09)


### 🔨 Refactor

* Create a common base class for web handlers ([#195](https://github.com/equinor/tagreader-python/issues/195)) ([87dd8ce](https://github.com/equinor/tagreader-python/commit/87dd8ce83861d1d7b23a4830493941717e26ed1d))
* Move examples from docs/ to examples/ ([#197](https://github.com/equinor/tagreader-python/issues/197)) ([2fc7b03](https://github.com/equinor/tagreader-python/commit/2fc7b03b9f907bc1c7b7910eab676beac2c420b9))
* Move versioning to __version__.py ([#196](https://github.com/equinor/tagreader-python/issues/196)) ([c814738](https://github.com/equinor/tagreader-python/commit/c814738c1b3f2a6d9a1155943402bd731b1a3d91))


### 🧹 Chores

* Enforce typing and validate using mypy to make the code more robust ([#199](https://github.com/equinor/tagreader-python/issues/199)) ([6212bb2](https://github.com/equinor/tagreader-python/commit/6212bb20a7bca8730dd5b90085d1b62f0c10b155))


### 📦 Build system

* Don't install pytables for ARM64 ([fe9d553](https://github.com/equinor/tagreader-python/commit/fe9d5539d27c46240ed972dd5d4a8594cfe84879))

## [4.0.2](https://github.com/equinor/tagreader-python/compare/v4.0.1...v4.0.2) (2023-06-05)


### 👷 CI/CD

* correct name for PyPi upload ([85fd96a](https://github.com/equinor/tagreader-python/commit/85fd96afe8630b4748de4b0471c9e92c64ead8ca))
* correct PyPi upload secret ([a59ecff](https://github.com/equinor/tagreader-python/commit/a59ecff99dbd85f7637c6aee9da24f1eb080365b))

## [4.0.1](https://github.com/equinor/tagreader-python/compare/v4.0.0...v4.0.1) (2023-06-05)


### 📦 Build system

* **deps:** update dependencies ([d9954f9](https://github.com/equinor/tagreader-python/commit/d9954f9eb0f78dd60fa339baf6ce3fcdf72fadac))

## [4.0.0](https://github.com/equinor/tagreader-python/compare/v3.0.2...v4.0.0) (2023-05-24)


### ⚠ BREAKING CHANGES

* Python 3.7 is no longer supported due to end-of-life

### 🧪 Tests

* fix tests, don't raise error if package tables is not installed ([#145](https://github.com/equinor/tagreader-python/issues/145)) ([df84ec9](https://github.com/equinor/tagreader-python/commit/df84ec994a23d452a9b7a5575aa76ed0e9470d73))


### ✨ Features

* auto_detect ims type if not provided ([#130](https://github.com/equinor/tagreader-python/issues/130)) ([d53e314](https://github.com/equinor/tagreader-python/commit/d53e31418db78b0015cdcc887c7e2247c6e1b740))
* remove duplicate tags ([#121](https://github.com/equinor/tagreader-python/issues/121)) ([c297990](https://github.com/equinor/tagreader-python/commit/c297990a4bb59d717a26ef2f8a6003734b5dc7c5))


### 🔨 Refactor

* get url to aspen/pi endpoints from function ([#144](https://github.com/equinor/tagreader-python/issues/144)) ([4e22b5c](https://github.com/equinor/tagreader-python/commit/4e22b5c2d67a9e165a786cb1f2e168da7f9294c0))


### 🐛 Bug Fixes

* catch exception when request does not return json ([#133](https://github.com/equinor/tagreader-python/issues/133)) ([c81d811](https://github.com/equinor/tagreader-python/commit/c81d8115cfe1677160839f3c72787184b569ffff))
* don't call unavailable functions on mac ([#129](https://github.com/equinor/tagreader-python/issues/129)) ([c6f363c](https://github.com/equinor/tagreader-python/commit/c6f363cfa8dc9627c33df6bd589fb51838861deb))
* timestamp parsing for Pandas 2.0 ([#177](https://github.com/equinor/tagreader-python/issues/177)) ([bdf0217](https://github.com/equinor/tagreader-python/commit/bdf0217ddcce09ee94e4da07c21e533eaef502ab))


### 👷 CI/CD

* add dependabot config ([f800f37](https://github.com/equinor/tagreader-python/commit/f800f37584892cfac061a9ada44897d30e394ebb))
* add pr-name-validator ([79f2982](https://github.com/equinor/tagreader-python/commit/79f298284d5758acbcf8afa3733ca90e1999a204))
* add release-please ([45370d1](https://github.com/equinor/tagreader-python/commit/45370d1147b354c6bbfa9e999c4a682a10d83c86))
* fix release please ([7c0ea48](https://github.com/equinor/tagreader-python/commit/7c0ea487bc0b28bb6b88d787e6362bba5b6e2475))
* test and lint pull requests ([#178](https://github.com/equinor/tagreader-python/issues/178)) ([2e63a29](https://github.com/equinor/tagreader-python/commit/2e63a2927f5d289f0b9921169ee989b6bd70f5d7))
* use Poetry for packaging and dependency management ([922353b](https://github.com/equinor/tagreader-python/commit/922353bf8f2fc127a3c2a4d19558108c82754f59)), closes [#185](https://github.com/equinor/tagreader-python/issues/185)


### 🧹 Chores

* align docs with requirements ([2c4f5d6](https://github.com/equinor/tagreader-python/commit/2c4f5d677c8a622174935e0f02e6d373e1cb36a9))
* bump notebook from 6.4.10 to 6.4.12 ([#117](https://github.com/equinor/tagreader-python/issues/117)) ([02c088a](https://github.com/equinor/tagreader-python/commit/02c088abeb7de63735f1578d6685632c96760b6d))
* **deps:** bump actions/checkout from 2 to 3 ([#166](https://github.com/equinor/tagreader-python/issues/166)) ([53bd904](https://github.com/equinor/tagreader-python/commit/53bd90428c51988a9df849ec21374ca1b0e7fe81))
* **deps:** bump actions/setup-python from 1 to 4 ([#165](https://github.com/equinor/tagreader-python/issues/165)) ([07a9caa](https://github.com/equinor/tagreader-python/commit/07a9caa480491c13978730cb0b70144a3f870909))
* scheduled weekly dependency update for week 08 ([#163](https://github.com/equinor/tagreader-python/issues/163)) ([02645ee](https://github.com/equinor/tagreader-python/commit/02645eee7418ea800646024f366caefc4aacace4))

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
