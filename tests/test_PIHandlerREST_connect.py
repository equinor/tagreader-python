import pytest
import os
import pandas as pd
from tagreader.utils import datestr_to_datetime, ReaderType
from tagreader.web_handlers import (
    list_pi_servers,
    PIHandlerWeb,
)
from tagreader.clients import IMSClient

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )


BASE_URL = "https://pivision.equinor.com/piwebapi"
ASSET = "PINO"
TAGS = {
    "Float32": "BA:CONC.1",
    "Digital": "BA:ACTIVE.1",
    "Int32": "CDEP158",
}

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture()
def Client():
    c = IMSClient(ASSET, imstype="piweb")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(ASSET + ".h5"):
        os.remove(ASSET + ".h5")


@pytest.fixture()
def PIHandler():
    h = PIHandlerWeb(server=ASSET)
    h.webidcache["alreadyknowntag"] = "knownwebid"
    yield h


def test_list_all_pi_servers():
    res = list_pi_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_escape_chars():
    assert (
        PIHandlerWeb.escape('+-&|(){}[]^"~*:\\') == r"\+\-\&\|\(\)\{\}\[\]\^\"\~*\:\\"
    )


def test_verify_connection(PIHandler):
    assert PIHandler.verify_connection("PIMAM") is True
    assert PIHandler.verify_connection("somerandomstuffhere") is False


def test_generate_search_query():
    assert PIHandlerWeb.generate_search_query("SINUSOID") == {"q": "name:SINUSOID"}
    assert PIHandlerWeb.generate_search_query(r"BA:*.1", server="PINO") == {
        "q": r"name:BA\:*.1",
        "scope": "pi:PINO",
    }
    assert PIHandlerWeb.generate_search_query(tag="BA:*.1") == {
        "q": r"name:BA\:*.1",
    }
    assert PIHandlerWeb.generate_search_query(desc="Concentration Reactor 1") == {
        "q": r"description:Concentration\ Reactor\ 1",
    }
    assert PIHandlerWeb.generate_search_query(
        tag="BA:*.1", desc="Concentration Reactor 1"
    ) == {"q": r"name:BA\:*.1 AND description:Concentration\ Reactor\ 1"}


def test_search_tag(Client):
    res = Client.search_tag("SINUSOID")
    assert 1 == len(res)
    res = Client.search_tag("BA:*.1")
    assert 5 == len(res)
    [taglist, desclist] = zip(*res)
    assert "BA:CONC.1" in taglist
    assert desclist[taglist.index("BA:CONC.1")] == "Concentration Reactor 1"
    res = Client.search_tag(tag="BA:*.1")
    assert 5 == len(res)
    res = Client.search_tag(desc="Concentration Reactor 1")
    assert 1 == len(res)
    res = Client.search_tag("BA*.1", "*Active*")
    assert 1 == len(res)
    res = Client.search_tag("BAS*")
    assert 30 < len(res)


def test_tag_to_webid(PIHandler):
    res = PIHandler.tag_to_webid("SINUSOID")
    assert isinstance(res, str)
    assert len(res) >= 20
    with pytest.raises(AssertionError):
        res = PIHandler.tag_to_webid("SINUSOID*")
    with pytest.raises(AssertionError):
        res = PIHandler.tag_to_webid("somerandomgarbage")


@pytest.mark.parametrize(
    "read_type",
    [
#        pytest.param("RAW", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param(
#            "SHAPEPRESERVING", marks=pytest.mark.skip(reason="Not implemented")
#        ),
        "INT",
        "MIN",
        "MAX",
        "RNG",
        "AVG",
        "STD",
        "VAR",
#        pytest.param("COUNT", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param("GOOD", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param("BAD", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param("TOTAL", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param("SUM", marks=pytest.mark.skip(reason="Not implemented")),
#        pytest.param("SNAPSHOT", marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_generate_read_query(PIHandler, read_type):  # TODO: Move away from test*connect
    starttime = datestr_to_datetime(START_TIME)
    stoptime = datestr_to_datetime(STOP_TIME)
    ts = pd.Timedelta(SAMPLE_TIME, unit="s")

    (url, params) = PIHandler.generate_read_query(
        PIHandler.tag_to_webid("alreadyknowntag"),
        starttime,
        stoptime,
        ts,
        getattr(ReaderType, read_type),
    )
    assert params["startTime"] == "01-Apr-20 11:05:00"
    assert params["endTime"] == "01-Apr-20 12:05:00"

    if read_type == "INT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/interpolated"
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/summary"
        assert {
            "AVG": "Average",
            "MIN": "Minimum",
            "MAX": "Maximum",
            "RNG": "Range",
            "STD": "StdDev",
            "VAR": "StdDev",
        }.get(read_type) == params["summaryType"]


@pytest.mark.parametrize(
    ("read_type", "size"),
    [
#         pytest.param("RAW", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param(
#             "SHAPEPRESERVING", 0, marks=pytest.mark.skip(reason="Not implemented")
#         ),
        ("INT", 61),
#         ("MIN", 61),
#         ("MAX", 61),
#         ("RNG", 61),
#         ("AVG", 61),
#         ("VAR", 61),
#         ("STD", 61),
#         pytest.param("COUNT", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param("GOOD", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param("BAD", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param("TOTAL", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param("SUM", 0, marks=pytest.mark.skip(reason="Not implemented")),
#         pytest.param("SNAPSHOT", 0, marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_read(Client, read_type, size):
    df = Client.read_tags(
        TAGS["Float32"], START_TIME, STOP_TIME, SAMPLE_TIME, getattr(ReaderType, read_type)
    )
    assert df.size == size
