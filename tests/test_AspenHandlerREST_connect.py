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

SOURCE = "SNA"
TAG = "ATCAI"
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
    res = client.search(tag="so_specific_it_cannot_possibly_exist", desc=None)
    assert 0 == len(res)

    res = client.search(tag="ATCAI", desc=None)
    assert res == [("ATCAI", "Sine Input")]

    res = client.search(tag="ATCM*", desc=None)
    assert 5 <= len(res)

    [taglist, desclist] = zip(*res)
    assert "ATCMIXTIME1" in taglist
    assert desclist[taglist.index("ATCMIXTIME1")] == "MIX TANK 1 TIMER"

    res = client.search(tag="ATCM*", desc=None)
    assert 5 <= len(res)
    assert isinstance(res, list)
    assert isinstance(res[0], tuple)

    res = client.search("AspenCalcTrigger1", desc=None)
    assert res == [("AspenCalcTrigger1", "")]

    res = client.search("ATC*", "Sine*")
    assert res == [("ATCAI", "Sine Input")]
    with pytest.raises(ValueError):
        _ = client.search(desc="Sine Input")  # noqa

    res = client.search(tag="ATCM*", return_desc=False)
    assert 5 <= len(res)
    assert isinstance(res, list)
    assert isinstance(res[0], str)

    res = client.search("AspenCalcTrigger1")
    assert res == [("AspenCalcTrigger1", "")]
    res = client.search("AspenCalcTrigger1", desc=None)
    assert res == [("AspenCalcTrigger1", "")]

    res = client.search("ATC*", "Sine*")
    assert res == [("ATCAI", "Sine Input")]

    with pytest.raises(ValueError):
        res = client.search("")

    with pytest.raises(ValueError):
        _ = client.search(
            desc="Sine Input"
        )  # noqa    res = client.search(tag="ATCM*", return_desc=False)´


def test_read_unknown_tag(client: IMSClient) -> None:
    df = client.read(
        tags=["so_random_it_cant_exist"], start_time=START_TIME, end_time=STOP_TIME
    )
    assert len(df.index) == 0
    df = client.read(
        tags=[TAG, "so_random_it_cant_exist"], start_time=START_TIME, end_time=STOP_TIME
    )
    assert len(df.index) > 0
    assert len(df.columns == 1)


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
    client.handler.initialize_connection_string(host="SNA-IMS.statoil.net")
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = client.query_sql(query=query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = client.query_sql(query=query, parse=True)
        assert isinstance(res, str)
