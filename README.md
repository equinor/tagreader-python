# tagreader-python <!-- omit in toc -->

![GitHub Build Status](https://github.com/equinor/tagreader-python/workflows/Test/badge.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/tagreader)
![PyPI](https://img.shields.io/pypi/v/tagreader)
[![Downloads](https://pepy.tech/badge/tagreader)](https://pepy.tech/project/tagreader)

Tagreader is a Python package for reading timeseries data from the OSIsoft PI and Aspen Infoplus.21
Information Manufacturing Systems (IMS) systems. It is intended to be easy to use, and present as similar interfaces
as possible to the backend historians.

## Installation
You can install tagreader directly into your project from pypi by using pip
or another package manager. The only requirement is Python version 3.8 or above.

```shell"
pip install tagreader
```

The following are required and will be installed:

* pandas
* requests
* requests-kerberos
* certifi
* diskcache

## Usage
Tagreader easy to use for both Equinor internal IMS services, and non-internal usage. For non-internal usage
you simply need to provide the corresponding IMS service URLs and IMSType.
See [data source](https://equinor.github.io/tagreader-python/docs/about/usage/data-source) for details.

### Usage example
```python
import tagreader
c = tagreader.IMSClient("mysource", "aspenone")
print(c.search("tag*"))
df = c.read_tags(["tag1", "tag2"], "18.06.2020 08:00:00", "18.06.2020 09:00:00", 60)
```

### Jupyter Notebook Quickstart
Jupyter Notebook examples can be found in /examples. In order to run these examples, you need to install the
optional dependencies.

```shell
pip install tagreader[notebooks]
```

The quickstart Jupyter Notebook can be found [here](https://github.com/equinor/tagreader-python/blob/main/examples/quickstart.ipynb)

For more details, see the [Tagreader Docs](https://equinor.github.io/tagreader-python/).

## Contribute
As Tagreader is an open source project, all contributions are welcome. This includes code, bug reports, issues,
feature requests, and documentation. The preferred way of submitting a contribution is to either create an issue on
GitHub or to fork the project and make a pull request.

For starting contributing, see the [contribute section](https://equinor.github.io/tagreader-python/docs/contribute/overview)
