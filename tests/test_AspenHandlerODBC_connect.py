import os
from typing import Generator

import pandas as pd
import pytest

from tagreader.clients import IMSClient, list_sources
from tagreader.utils import is_windows

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)

import pyodbc

from tagreader.odbc_handlers import list_aspen_sources

is_GITHUB_ACTIONS = "GITHUB_ACTION" in os.environ

if is_GITHUB_ACTIONS:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

SOURCE = "SNA"
TAGS = ["ATCAI", "ATCMIXTIME1"]
START_TIME = "2018-05-01 10:00:00"
STOP_TIME = "2018-05-01 11:00:00"


@pytest.fixture  # type: ignore[misc]
def client() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=SOURCE, imstype="ip21")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


def test_list_all_aspen_sources() -> None:
    res = list_aspen_sources()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspen() -> None:
    res = list_sources(imstype="aspen")
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_query_sql(client: IMSClient) -> None:
    query = "SELECT name, ip_description FROM ip_analogdef WHERE name LIKE 'ATC%'"
    res = client.query_sql(query=query, parse=True)
    assert isinstance(res, pd.DataFrame)
    assert res.shape[0] >= 1
    assert res.shape[1] == 2
    res = client.query_sql(query=query, parse=False)
    assert isinstance(res, pyodbc.Cursor)
    rows = res.fetchall()
    assert len(rows) >= 1
    assert len(rows[0]) == 2
