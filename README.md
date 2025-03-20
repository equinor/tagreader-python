# tagreader-python <!-- omit in toc -->

![GitHub Build Status](https://github.com/equinor/tagreader-python/workflows/Test/badge.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/tagreader)
![PyPI](https://img.shields.io/pypi/v/tagreader)
[![Downloads](https://pepy.tech/badge/tagreader)](https://pepy.tech/project/tagreader)

Tagreader is a Python package for reading timeseries data from the OSIsoft PI and Aspen Infoplus.21
Information Management Systems (IMS). It is intended to be easy to use, and present as similar interfaces
as possible to the backend plant historians.

## Installation
You can install tagreader directly into your project from pypi by using pip
or another package manager. Supports Python version 3.9.2 and above.

```shell
pip install tagreader
```

## Usage
Tagreader is easy to use for both Equinor internal IMS services, and non-internal usage. For non-internal usage
you simply need to provide the corresponding IMS service URLs and IMSType.
See [data source](https://equinor.github.io/tagreader-python/docs/about/usage/data-source) for details.

### Usage example
```python
import tagreader
c = tagreader.IMSClient("mysource", "aspenone")
print(c.search("tag*"))
df = c.read_tags(["tag1", "tag2"], "18.06.2020 08:00:00", "18.06.2020 09:00:00", 60)
```

Note, you can add a timeout argument to the search method in order to avoid long-running search queries.

### Jupyter Notebook Quickstart
Jupyter Notebook examples can be found in /examples. In order to run these examples, you need to install the
optional dependencies.

```shell
pip install tagreader[notebooks]
```

The quickstart Jupyter Notebook can be found [here](https://github.com/equinor/tagreader-python/blob/main/examples/quickstart.ipynb)

For more details, see the [Tagreader Docs](https://equinor.github.io/tagreader-python/).

## Documentation
The full documentation can be found in [Tagreader Docs](https://equinor.github.io/tagreader-python/)

## Contribute
To starting contributing, please see [Tagreader Docs - Contribute](https://equinor.github.io/tagreader-python/docs/contribute/overview)
