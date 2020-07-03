import pytest
import os
import pandas as pd
from tagreader.utils import ReaderType
from tagreader.web_handlers import (
    list_pi_sources,
    PIHandlerWeb,
)
from tagreader.clients import IMSClient

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

verifySSL = not is_AZUREPIPELINE  # Certificate unavailable there

BASE_URL = "https://piwebapi.equinor.com/piwebapi"
SOURCE = "PINO"
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
    c = IMSClient(SOURCE, imstype="piweb", verifySSL=verifySSL)
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture()
def PIHandler():
    h = PIHandlerWeb(datasource=SOURCE, verifySSL=verifySSL)
    h.webidcache["alreadyknowntag"] = "knownwebid"
    yield h


def test_list_all_pi_sources():
    res = list_pi_sources()
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_verify_connection(PIHandler):
    assert PIHandler.verify_connection("PIMAM") is True
    assert PIHandler.verify_connection("somerandomstuffhere") is False


def test_search_tag(Client):
    res = Client.search_tag("SINUSOID")
    assert 1 == len(res)
    res = Client.search_tag("BA:*.1")
    assert 5 <= len(res)
    [taglist, desclist] = zip(*res)
    assert "BA:CONC.1" in taglist
    assert desclist[taglist.index("BA:CONC.1")] == "Concentration Reactor 1"
    res = Client.search_tag(tag="BA:*.1")
    assert 5 <= len(res)
    res = Client.search_tag(desc="Concentration Reactor 1")
    assert 1 <= len(res)
    res = Client.search_tag("BA*.1", "*Active*")
    assert 1 <= len(res)


def test_tag_to_webid(PIHandler):
    res = PIHandler.tag_to_webid("SINUSOID")
    assert isinstance(res, str)
    assert len(res) >= 20
    with pytest.raises(AssertionError):
        res = PIHandler.tag_to_webid("SINUSOID*")
    with pytest.raises(AssertionError):
        res = PIHandler.tag_to_webid("somerandomgarbage")


@pytest.mark.parametrize(
    ("read_type", "size"),
    [
        # pytest.param("RAW", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param(
        #      "SHAPEPRESERVING", 0, marks=pytest.mark.skip(reason="Not implemented")
        # ),
        ("INT", 61),
        ("MIN", 60),
        ("MAX", 60),
        ("RNG", 60),
        ("AVG", 60),
        ("VAR", 60),
        ("STD", 60),
        # pytest.param("COUNT", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SNAPSHOT", 0, marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_read(Client, read_type, size):
    df = Client.read_tags(
        TAGS["Float32"],
        START_TIME,
        STOP_TIME,
        SAMPLE_TIME,
        getattr(ReaderType, read_type),
    )
    assert df.shape == (size, 1)
    assert df.index[size - 1] == df.index[0] + (size - 1) * pd.Timedelta(
        SAMPLE_TIME, unit="s"
    )


def test_read_only_invalid_data_yields_nan_for_invalid(Client):
    tag = TAGS["Float32"]
    df = Client.read_tags(tag, "2012-10-09 10:30:00", "2012-10-09 11:00:00", 600)
    assert df.shape == (4, 1)
    assert df[tag].isna().all()


def test_read_invalid_data_mixed_with_valid_yields_nan_for_invalid(Client):
    tag = TAGS["Float32"]
    df = Client.read_tags(tag, "2012-10-09 11:00:00", "2012-10-09 11:30:00", 600)
    assert df.shape == (4, 1)
    assert df[tag].iloc[[0, 1]].isna().all()
    assert df[tag].iloc[[2, 3]].notnull().all()


def test_digitalread_is_one_or_zero(Client):
    tag = TAGS["Digital"]
    df = Client.read_tags(tag, START_TIME, STOP_TIME, SAMPLE_TIME, ReaderType.INT)
    assert df[tag].max() == 1
    assert df[tag].min() == 0
    assert df[tag].isin([0, 1]).all()


def test_get_unit(Client):
    res = Client.get_units(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "DEG. C"
    assert res[TAGS["Digital"]] == "STATE"
    assert res[TAGS["Int32"]] == ""


def test_get_description(Client):
    res = Client.get_descriptions(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "Concentration Reactor 1"
    assert res[TAGS["Digital"]] == "Batch Active Reactor 1"
    assert res[TAGS["Int32"]] == "Light Naphtha End Point"


def test_from_DST_folds_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = Client.read_tags([tag], interval[0], interval[1], 600)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df[tag].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size == 5
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1
    )


def test_to_DST_skips_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2018-03-25 00:30:00", "2018-03-25 03:30:00"]
    df = Client.read_tags([tag], interval[0], interval[1], 600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6
    )
