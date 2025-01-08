import os
from datetime import datetime, timedelta
from typing import Generator

import pytest
from pytest import raises

from tagreader.clients import IMSClient, list_sources
from tagreader.utils import IMSType
from tagreader.web_handlers import (
    AspenHandlerWeb,
    get_verify_ssl,
    list_aspenone_sources,
)

is_GITHUB_ACTIONS = "GITHUB_ACTION" in os.environ
is_AZURE_PIPELINE = "TF_BUILD" in os.environ

if is_GITHUB_ACTIONS:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

VERIFY_SSL = False if is_AZURE_PIPELINE else get_verify_ssl()

SOURCE = "TRB"
TAG = "xxx"
FAKE_TAG = "so_random_it_cant_exist"
START_TIME = datetime(2023, 5, 1, 10, 0, 0)
STOP_TIME = datetime(2023, 5, 1, 11, 0, 0)
SAMPLE_TIME = timedelta(seconds=60)


@pytest.fixture  # type: ignore[misc]
def client() -> Generator[IMSClient, None, None]:
    c = IMSClient(
        datasource=SOURCE,
        imstype="aspenone",
        verifySSL=bool(VERIFY_SSL),
    )
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture  # type: ignore[misc]
def aspen_handler() -> Generator[AspenHandlerWeb, None, None]:
    h = AspenHandlerWeb(
        datasource=SOURCE, verify_ssl=bool(VERIFY_SSL), auth=None, url=None, options={}
    )
    yield h


def test_list_all_aspen_one_sources() -> None:
    res = list_aspenone_sources(verify_ssl=bool(VERIFY_SSL), auth=None, url=None)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspen_one() -> None:
    res = list_sources(imstype=IMSType.ASPENONE, verifySSL=bool(VERIFY_SSL))
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_verify_connection(aspen_handler: AspenHandlerWeb) -> None:
    assert aspen_handler.verify_connection(SOURCE) is True
    assert aspen_handler.verify_connection("some_random_stuff_here") is False


def test_search_tag(client: IMSClient) -> None:
    res = client.search(tag=FAKE_TAG, desc=None)
    assert 0 == len(res)

    res = client.search(tag="AverageCPUTimeVals", desc=None)
    assert res == [("AverageCPUTimeVals", "Average CPU Time")]

    res = client.search(tag="Aspen*", desc=None, return_desc=False)
    assert len(res) < 5
    assert isinstance(res, list)
    assert isinstance(res[0], str)

    res = client.search(tag="Aspen*", desc=None)
    assert len(res) < 5
    assert isinstance(res, list)
    assert isinstance(res[0], tuple)

    res = client.search("AspenCalcTrigger1")
    assert res == [("AspenCalcTrigger1", "")]
    res = client.search("AspenCalcTrigger1", desc=None)
    assert res == [("AspenCalcTrigger1", "")]

    res = client.search("AverageCPUTimeVals", "*CPU*")
    assert res == [("AverageCPUTimeVals", "Average CPU Time")]
    with pytest.raises(ValueError):
        _ = client.search(desc="Sine Input")  # noqa

    with pytest.raises(ValueError):
        res = client.search("")

    with pytest.raises(ValueError):
        _ = client.search(
            desc="Sine Input"
        )  # noqa    res = client.search(tag="ATCM*", return_desc=False)´


def test_read_unknown_tag(client: IMSClient) -> None:
    df = client.read(tags=[FAKE_TAG], start_time=START_TIME, end_time=STOP_TIME)
    assert len(df.index) == 0
    df = client.read(tags=[TAG, FAKE_TAG], start_time=START_TIME, end_time=STOP_TIME)
    assert len(df.index) > 0
    assert len(df.columns == 1)


def test_get_units(client: IMSClient) -> None:
    d = client.get_units(FAKE_TAG)
    assert isinstance(d, dict)
    assert len(d.items()) == 0


def test_get_desc(client: IMSClient) -> None:
    d = client.get_descriptions(FAKE_TAG)
    assert isinstance(d, dict)
    assert len(d.items()) == 0


def test_query_sql(client: IMSClient) -> None:
    # The % causes WC_E_SYNTAX error in result. Tried "everything" but no go.
    # Leaving it for now.
    # query = "SELECT name, ip_description FROM ip_analogdef WHERE name LIKE 'ATC%'"
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = client.query_sql(query=query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = client.query_sql(query=query, parse=True)
        assert isinstance(res, str)
    client.handler.initialize_connection_string()
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = client.query_sql(query=query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = client.query_sql(query=query, parse=True)
        assert isinstance(res, str)
