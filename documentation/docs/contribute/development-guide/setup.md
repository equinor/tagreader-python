# Setup

## Prerequisites

Install Git, Python 3.9.6 or newer, and [uv](https://docs.astral.sh/uv/getting-started/installation/).
The package's Python requirement and dependencies are defined in
[`pyproject.toml`](https://github.com/equinor/tagreader-python/blob/main/pyproject.toml).
Use the Python versions covered by the current CI workflows when checking compatibility.

Tagreader supports PI Web API and AspenOne REST connections. ODBC clients and
their proprietary drivers are not supported in current releases.

## Install the development environment

Clone the repository and install its locked dependencies, including development tools:

```shell
git clone https://github.com/equinor/tagreader-python.git
cd tagreader-python
uv sync --locked
```

Run tools with `uv run` so they use the project environment. On Linux, building
the Kerberos dependency may require development headers, such as `libkrb5-dev`
and a C compiler on Ubuntu. Access to a historian also requires the appropriate
network connection, permissions, and authentication; installing the package does
not grant access.

## Pre-commit

When contributing to this project, pre-commits are necessary, as they run certain tests, sanitisers, and formatters.

The project provides a `.pre-commit-config.yaml` file that is used to set up git _pre-commit hooks_.

On commit locally, code is automatically formatted and checked for security vulnerabilities using pre-commit git hooks.

### Installation

To initialize pre-commit in your local repository, run

```shell
uv run pre-commit install
```

This tells pre-commit to run for this repository on every commit.

### Usage

Pre-commit will run on every commit, but can also be run manually on all files:

```shell
uv run pre-commit run --all-files
```

Pre-commit tests can be skipped on commits with `git commit --no-verify`.

:::caution
If you have to skip the pre-commit tests, you're probably doing something you're not supposed to, and whoever commits after you might have to fix your "mistakes". Consider updating the pre-commit hooks if your code is non-compliant.
:::

## Next steps

See [Testing](testing.md) for offline and live-service checks, and
[Upgrading](upgrading.md) for dependency and lockfile maintenance.
