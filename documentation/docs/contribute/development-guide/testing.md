# Testing

Install the development environment as described in [Setup](setup.md), then run:

```shell
uv run --locked pytest
```

The `*_connect.py` modules contact real PI and Aspen services. These tests need
network access and valid authentication and can fail because a service, proxy,
or certificate is unavailable. Do not treat those failures as offline regressions.

To run only tests that do not require live historian connections:

```shell
uv run --locked pytest --ignore=tests/test_AspenHandlerREST_connect.py --ignore=tests/test_PIHandlerREST_connect.py
```

For a focused check, pass the relevant test module, for example:

```shell
uv run --locked pytest tests/test_utils.py
uv run pre-commit run --all-files
```

The Azure pipeline additionally installs `pytest-cov` and `pytest-azurepipelines`
to publish coverage and test results. They are not part of the default development
dependency group. To generate coverage locally without editing project dependencies:

```shell
uv run --with pytest-cov pytest --junitxml=junit/test-results.xml --cov=tagreader --cov-report=xml
```

Keep handler tests in the matching PI or Aspen module. Never include credentials,
cookies, or authenticated service responses in committed test data.