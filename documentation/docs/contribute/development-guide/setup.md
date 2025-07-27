# Setup

```mdx-code-block
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
```

## Prerequisites

To work with the tagreader code you'll need to install:

Python >=3.8 with the following packages:

* pandas >= 1.0.0
* requests
* requests-kerberos
* certifi >= 2023.5.7
* diskcache
* pyodbc (If using ODBC connection)

:::info  ODBC Connection
If using ODBC connections, you must also install proprietary drivers for PI ODBC and/or Aspen IP.21 SQLPlus. These
drivers are only available for Microsoft Windows. Pyodbc will therefore not be installed for non-Windows systems.
:::

## Pre-commit

When contributing to this project, pre-commits are necessary, as they run certain tests, sanitisers, and formatters.

The project provides a `.pre-commit-config.yaml` file that is used to set up git _pre-commit hooks_.

On commit locally, code is automatically formatted and checked for security vulnerabilities using pre-commit git hooks.

### Installation

To initialize pre-commit in your local repository, run

```shell
pre-commit install
```

This tells pre-commit to run for this repository on every commit.

### Usage

Pre-commit will run on every commit, but can also be run manually on all files:

```shell
pre-commit run --all-files
```

Pre-commit tests can be skipped on commits with `git commit --no-verify`.

:::caution
If you have to skip the pre-commit tests, you're probably doing something you're not supposed to, and whoever commits after you might have to fix your "mistakes". Consider updating the pre-commit hooks if your code is non-compliant.
:::

### Install Poetry

Poetry is used to manage Python package dependencies.

```shell
$ pip install poetry
```

The installation instructions can be found [here](https://python-poetry.org/docs/#installation).

### Install packages

```shell
$ poetry install
```
