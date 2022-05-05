import os

import pytest
import urllib3
from pytest import raises
from tagreader.clients import IMSClient, list_sources
from tagreader.web_handlers import AspenHandlerWeb, get_verifySSL, list_aspenone_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

verifySSL = False if is_AZUREPIPELINE else get_verifySSL()
if is_AZUREPIPELINE:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOURCE = "SNA"
TAG = "ATCAI"
START_TIME = "2018-05-01 10:00:00"
STOP_TIME = "2018-05-01 11:00:00"
SAMPLE_TIME = 60


@pytest.fixture()
def Client():
    c = IMSClient(SOURCE, imstype="aspenone", verifySSL=verifySSL)
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture()
def AspenHandler():
    h = AspenHandlerWeb(datasource=SOURCE, verifySSL=verifySSL)
    yield h


def test_list_all_aspenone_sources():
    res = list_aspenone_sources(verifySSL=verifySSL)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspenone():
    res = list_sources("aspenone", verifySSL=verifySSL)
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


def test_query_sql(Client):
    # The % causes WC_E_SYNTAX error in result. Tried "everything" but no go.
    # Leaving it for now.
    # query = "SELECT name, ip_description FROM ip_analogdef WHERE name LIKE 'ATC%'"
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = Client.query_sql(query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = Client.query_sql(query, parse=True)
        assert isinstance(res, str)
    Client.handler.initialize_connectionstring(host="SNA-IMS.statoil.net")
    query = "Select name, ip_description from ip_analogdef where name = 'atcai'"
    res = Client.query_sql(query, parse=False)
    print(res)
    assert isinstance(res, str)
    with raises(NotImplementedError):
        res = Client.query_sql(query, parse=True)
        assert isinstance(res, str)
