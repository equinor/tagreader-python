import os
from datetime import datetime, timedelta
from typing import Generator

import pytest
from pytest import raises

from tagreader.clients import IMSClient, list_sources
from tagreader.utils import IMSType
from tagreader.web_handlers import AspenHandlerWeb, get_verifySSL, list_aspenone_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

VERIFY_SSL = False if is_AZUREPIPELINE else get_verifySSL()

SOURCE = "SNA"
TAG = "ATCAI"
START_TIME = datetime(2018, 5, 1, 10, 0, 0)
STOP_TIME = datetime(2018, 5, 1, 11, 0, 0)
SAMPLE_TIME = timedelta(seconds=60)


@pytest.fixture  # type: ignore[misc]
def Client() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=SOURCE, imstype="aspenone", verifySSL=bool(VERIFY_SSL))
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture  # type: ignore[misc]
def aspen_handler() -> Generator[AspenHandlerWeb, None, None]:
    h = AspenHandlerWeb(
        datasource=SOURCE, verifySSL=bool(VERIFY_SSL), auth=None, url=None, options={}
    )
    yield h


def test_list_all_aspenone_sources() -> None:
    res = list_aspenone_sources(verifySSL=bool(VERIFY_SSL), auth=None, url=None)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspenone() -> None:
    res = list_sources(imstype=IMSType.ASPENONE, verifySSL=bool(VERIFY_SSL))
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_verify_connection(aspen_handler: AspenHandlerWeb) -> None:
    assert aspen_handler.verify_connection(SOURCE) is True
    assert aspen_handler.verify_connection("somerandomstuffhere") is False


def test_search_tag(Client: IMSClient) -> None:
    res = Client.search(tag="sospecificitcannotpossiblyexist", desc=None)
    assert 0 == len(res)
    res = Client.search(tag="ATCAI", desc=None)
    assert res == [("ATCAI", "Sine Input")]
    res = Client.search(tag="ATCM*", desc=None)
    assert 5 <= len(res)
    [taglist, desclist] = zip(*res)
    assert "ATCMIXTIME1" in taglist
    assert desclist[taglist.index("ATCMIXTIME1")] == "MIX TANK 1 TIMER"
    res = Client.search(tag="ATCM*", desc=None)
    assert 5 <= len(res)
    res = Client.search("AspenCalcTrigger1", desc=None)
    assert res == [("AspenCalcTrigger1", "")]
    res = Client.search("ATC*", "Sine*")
    assert res == [("ATCAI", "Sine Input")]
    with pytest.raises(ValueError):
        _ = Client.search(desc="Sine Input")  # noqa


def test_read_unknown_tag(Client: IMSClient) -> None:
    df = Client.read(
        tags=["sorandomitcantexist"], start_time=START_TIME, end_time=STOP_TIME
    )
    assert len(df.index) == 0
    df = Client.read(
        tags=[TAG, "sorandomitcantexist"], start_time=START_TIME, end_time=STOP_TIME
    )
    assert len(df.index) > 0
    assert len(df.columns == 1)


def test_query_sql(Client: IMSClient) -> None:
    # The % causes WC_E_SYNTAX error in result. Tried "everything" but no go.
    # Leaving it for now.
    # query = "SELECT name, ip_description FROM ip_analogdef WHERE name LIKE 'ATC%'"
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = Client.query_sql(query=query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = Client.query_sql(query=query, parse=True)
        assert isinstance(res, str)
    Client.handler.initialize_connectionstring(host="SNA-IMS.statoil.net")
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = Client.query_sql(query=query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = Client.query_sql(query=query, parse=True)
        assert isinstance(res, str)
