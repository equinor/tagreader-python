import pytest
import os
import pandas as pd

from tagreader.clients import IMSClient, list_sources
from tagreader.web_handlers import (
    list_aspenone_sources,
    AspenHandlerWeb,
)

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

URL = r"https://aspenone-qa.equinor.com/ProcessData/AtProcessDataREST.dll"
SOURCE = "SNA"
TAG = "ATCAI"

START_TIME = "2018-05-01 10:00:00"
STOP_TIME = "2018-05-01 11:00:00"
SAMPLE_TIME = 60


@pytest.fixture()
def Client():
    c = IMSClient(SOURCE, imstype="aspenone", url=URL, verifySSL=False)
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture()
def AspenHandler():
    h = AspenHandlerWeb(datasource=SOURCE, url=URL, verifySSL=False)
    yield h


def test_list_all_aspenone_sources():
    res = list_aspenone_sources(url=URL, verifySSL=False)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspenone():
    res = list_sources("aspenone", url=URL, verifySSL=False)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_verify_connection(AspenHandler):
    assert AspenHandler.verify_connection(SOURCE) is True
    assert AspenHandler.verify_connection("somerandomstuffhere") is False


def test_search_tag(Client):
    res = Client.search("sospecificitcannotpossiblyexist")
    assert 0 == len(res)
    res = Client.search("ATCAI")
    assert res == [("ATCAI", "Sine Input")]
    res = Client.search("ATCM*")
    assert 5 <= len(res)
    [taglist, desclist] = zip(*res)
    assert "ATCMIXTIME1" in taglist
    assert desclist[taglist.index("ATCMIXTIME1")] == "MIX TANK 1 TIMER"
    res = Client.search(tag="ATCM*")
    assert 5 <= len(res)
    res = Client.search("AspenCalcTrigger1")
    assert res == [("AspenCalcTrigger1", "")]
    res = Client.search("ATC*", "Sine*")
    assert res == [("ATCAI", "Sine Input")]
    with pytest.raises(ValueError):
        res = Client.search(desc="Sine Input")


def test_read_unknown_tag(Client):
    with pytest.warns(UserWarning):
        df = Client.read(["sorandomitcantexist"], START_TIME, STOP_TIME)
    assert len(df.index) == 0
    with pytest.warns(UserWarning):
        df = Client.read([TAG, "sorandomitcantexist"], START_TIME, STOP_TIME)
    assert len(df.index) > 0
    assert len(df.columns == 1)
