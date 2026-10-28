---
sidebar_position: 1
---
# Introduction

Tagreader is a Python package for reading timeseries data from the OSIsoft PI and Aspen Infoplus.21
Information Manufacturing Systems (IMS) systems. It is intended to be easy to use, and present as similar interfaces
as possible to the backend historians.

## System requirements
Tagreader requires Python 3.9.6 or newer on Windows, Linux, or macOS. Reading
historian data also requires access to a PI Web API or AspenOne REST service and
the appropriate authentication.

## Installation
You can install tagreader directly into your project from pypi by using pip
or another package manager.

```shell
pip install tagreader
```

Dependencies are installed automatically. The complete requirements are maintained
in [`pyproject.toml`](https://github.com/equinor/tagreader-python/blob/main/pyproject.toml).

## Usage
Tagreader easy to use for both Equinor internal IMS services, and non-internal usage. For non-internal usage
you simply need to provide the corresponding IMS service URLs and IMSType. See [data source](usage/data-source.md) for details.

### Usage example
```python
import tagreader
c = tagreader.IMSClient("mysource", "aspenone")
print(c.search("tag*"))
df = c.read(["tag1", "tag2"], "2020-06-18 08:00:00", "2020-06-18 09:00:00", 60)
```

### Examples

See the [quickstart script](https://github.com/equinor/tagreader-python/blob/main/examples/quickstart.py)
for a runnable example. Tagreader can also be used from Jupyter; install Jupyter
separately in your environment. There is no `notebooks` package extra in the current release.

For more details, see the [Usage section](/docs/about/usage/basic-usage).

## Contribute
As Tagreader is an open source project, all contributions are welcome. This includes code, bug reports, issues,
feature requests, and documentation. The preferred way of submitting a contribution is to either create an issue on
GitHub or to fork the project and make a pull request.

For starting contributing, see the [contribute section](../contribute/how-to-start-contributing.md)

