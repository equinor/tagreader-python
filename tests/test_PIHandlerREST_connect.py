import pytest
import os

from tagreader.web_handlers import list_pi_servers
from tagreader.clients import IMSClient
from tagreader.web_handlers import PIHandlerWeb


is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )
asset = "PINO"

tags = {
    "Float32": "BA:CONC.1",
    "Digital": "BA:ACTIVE.1",
    "Int32": "CDEP158",
}

interval = ["2020-04-01 11:05:00", "2020-04-01 12:05:00"]


@pytest.fixture()
def Client():
    c = IMSClient(asset, "piweb")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(asset + ".h5"):
        os.remove(asset + ".h5")


def test_escape_chars():
    assert (
        PIHandlerWeb.escape('+-&|(){}[]^"~*:\\') == r"\+\-\&\|\(\)\{\}\[\]\^\"\~*\:\\"
    )


def test_generate_search_query():
    assert PIHandlerWeb.generate_search_query("SINUSOID") == {
        "q": "name:SINUSOID",
        "scope": "pi:PINO",
    }
    assert PIHandlerWeb.generate_search_query(r"BA:*.1") == {
        "q": r"name:BA\:*.1",
        "scope": "pi:PINO",
    }
    assert PIHandlerWeb.generate_search_query(tag="BA:*.1") == {
        "q": r"name:BA\:*.1",
        "scope": "pi:PINO",
    }
    assert PIHandlerWeb.generate_search_query(desc="Concentration Reactor 1") == {
        "q": r"description:Concentration\ Reactor\ 1",
        "scope": "pi:PINO",
    }
    assert PIHandlerWeb.generate_search_query(
        tag="BA:*.1", desc="Concentration Reactor 1"
    ) == {
        "q": r"name:BA\:*.1 AND description:Concentration\ Reactor\ 1",
        "scope": "pi:PINO",
    }


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


def test_list_all_pi_servers():
    res = list_pi_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)
