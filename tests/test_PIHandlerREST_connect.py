import os
import sys

import pandas as pd
import pytest
from tagreader.clients import IMSClient, list_sources
from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.web_handlers import PIHandlerWeb, get_verifySSL, list_piwebapi_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

verifySSL = False if is_AZUREPIPELINE else get_verifySSL()

BASE_URL = "https://piwebapi.equinor.com/piwebapi"
SOURCE = "PIMAM"
TAGS = {
    "Float32": "CDT158",  # BA:CONC.1
    "Digital": "CDM158",  # BA:ACTIVE.1
    "Int32": "CDEP158",
}

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture()
def Client():
    c = IMSClient(SOURCE, imstype="piwebapi", verifySSL=verifySSL)
    c.cache = None
    c.connect()
    c.handler._max_rows = 1000  # For the long raw test
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture()
def PIHandler():
    h = PIHandlerWeb(datasource=SOURCE, verifySSL=verifySSL)
    h.webidcache["alreadyknowntag"] = "knownwebid"
    yield h


def test_list_all_piwebapi_sources():
    res = list_piwebapi_sources(verifySSL=verifySSL)
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_list_sources_piwebapi():
    res = list_sources("piwebapi", verifySSL=verifySSL)
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_verify_connection(PIHandler):
    assert PIHandler.verify_connection("PIMAM") is True
    assert PIHandler.verify_connection("somerandomstuffhere") is False


def test_search_tag(Client):
    res = Client.search("SINUSOID")
    assert 1 == len(res)
    res = Client.search("BA:*.1")
    assert 5 <= len(res)
    [taglist, desclist] = zip(*res)
    assert "BA:CONC.1" in taglist
    assert desclist[taglist.index("BA:CONC.1")] == "Concentration Reactor 1"
    res = Client.search(tag="BA:*.1")
    assert 5 <= len(res)
    res = Client.search(desc="Concentration Reactor 1")
    assert 1 <= len(res)
    res = Client.search("BA*.1", "*Active*")
    assert 1 <= len(res)


def test_tag_to_webid(PIHandler):
    res = PIHandler.tag_to_webid("SINUSOID")
    assert isinstance(res, str)
    assert len(res) >= 20
    with pytest.raises(AssertionError):
        res = PIHandler.tag_to_webid("SINUSOID*")
    with pytest.warns():
        res = PIHandler.tag_to_webid("somerandomgarbage")


@pytest.mark.parametrize(
    ("read_type", "size"),
    [
        ("RAW", 10),
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
        ("SNAPSHOT", 1),
    ],
)
def test_read(Client, read_type, size):
    if read_type == "SNAPSHOT":
        df = Client.read(
            TAGS["Float32"],
            read_type=getattr(ReaderType, read_type),
        )
    else:
        df = Client.read(
            TAGS["Float32"],
            start_time=START_TIME,
            end_time=STOP_TIME,
            ts=SAMPLE_TIME,
            read_type=getattr(ReaderType, read_type),
        )

    if read_type not in ["SNAPSHOT", "RAW"]:
        assert df.shape == (size, 1)
        assert df.index[0] == ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] == df.index[0] + (size - 1) * pd.Timedelta(
            SAMPLE_TIME, unit="s"
        )
    elif read_type in "RAW":
        # Weirdness for test-tag which can have two different results,
        # apparently depending on the day of the week, mood, lunar cycle...
        assert df.shape == (size, 1) or df.shape == (size - 1, 1)
        assert df.index[0] >= ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] <= ensure_datetime_with_tz(STOP_TIME)


def test_read_with_status(Client):
    df = Client.read(
        TAGS["Float32"],
        start_time=START_TIME,
        end_time=STOP_TIME,
        ts=SAMPLE_TIME,
        read_type=ReaderType.RAW,
        get_status=True,
    )
    assert df.shape == (10, 2)
    assert df[TAGS["Float32"] + "::status"].iloc[0] == 0


def test_read_raw_long(Client):
    df = Client.read(
        TAGS["Float32"],
        start_time=START_TIME,
        end_time="2020-04-11 20:00:00",
        read_type=ReaderType.RAW,
    )
    assert len(df) > 1000


def test_read_only_invalid_data_yields_nan_for_invalid(Client):
    tag = TAGS["Float32"]
    df = Client.read(tag, "2012-10-09 10:30:00", "2012-10-09 11:00:00", 600)
    assert df.shape == (4, 1)
    assert df[tag].isna().all()


def test_read_invalid_data_mixed_with_valid_yields_nan_for_invalid(Client):
    # Hint, found first valid datapoint for tag
    tag = TAGS["Float32"]
    df = Client.read(tag, "2018-04-23 15:20:00", "2018-04-23 15:50:00", 600)
    assert df.shape == (4, 1)
    assert df[tag].iloc[[0, 1]].isna().all()
    assert df[tag].iloc[[2, 3]].notnull().all()


def test_digitalread_yields_integers(Client):
    tag = TAGS["Digital"]
    df = Client.read(
        tag, start_time=START_TIME, end_time=STOP_TIME, ts=600, read_type=ReaderType.INT
    )
    assert all(x.is_integer() for x in df[tag])


def test_get_unit(Client):
    res = Client.get_units(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "DEG. C"
    assert res[TAGS["Digital"]] == "STATE"
    assert res[TAGS["Int32"]] == ""


def test_get_description(Client):
    res = Client.get_descriptions(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "Atmospheric Tower OH Vapor"
    assert res[TAGS["Digital"]] == "Light Naphtha End Point Control"
    assert res[TAGS["Int32"]] == "Light Naphtha End Point"


def test_from_DST_folds_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = Client.read([tag], interval[0], interval[1], 600)
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
    df = Client.read([tag], interval[0], interval[1], 600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6
    )


# def test_read_unknown_tag(Client):
#     with pytest.warns(UserWarning):
#         df = Client.read(["sorandomitcantexist"], START_TIME, STOP_TIME)
#     assert len(df.index) == 0
#     with pytest.warns(UserWarning):
#         df = Client.read(
#             [TAGS["Float32"], "sorandomitcantexist"], START_TIME, STOP_TIME
#         )
#     assert len(df.index) > 0
#     assert len(df.columns == 1)


def test_tags_with_no_data_included_in_results(Client):
    df = Client.read([TAGS["Float32"]], "2099-01-01 00:00:00", "2099-01-02 00:00:00")
    assert len(df.columns) == 1


def test_tags_raw_with_no_data_included_in_results(Client):
    df = Client.read(
        [TAGS["Float32"]],
        "2099-01-01 00:00:00",
        "2099-01-02 00:00:00",
        read_type=ReaderType.RAW,
    )
    assert df.empty


# def test_connect_no_pytables():
#     with pytest.warns(UserWarning):
#         c = IMSClient(datasource="whatever", host="host", imstype="piwebapi")
#         c.connect()
