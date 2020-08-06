import pytest
import pandas as pd
from tagreader.utils import ensure_datetime_with_tz, ReaderType
from tagreader.web_handlers import PIHandlerWeb

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture()
def PIHandler():
    h = PIHandlerWeb(datasource="sourcename")
    h.webidcache["alreadyknowntag"] = "knownwebid"
    yield h


def test_escape_chars():
    assert (
        PIHandlerWeb.escape('+-&|(){}[]^"~*:\\') == r"\+\-\&\|\(\)\{\}\[\]\^\"\~*\:\\"
    )


def test_generate_search_query():
    assert PIHandlerWeb.generate_search_query("SINUSOID") == {"q": "name:SINUSOID"}
    assert PIHandlerWeb.generate_search_query(r"BA:*.1", datasource="sourcename") == {
        "q": r"name:BA\:*.1",
        "scope": "pi:sourcename",
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


def test_is_summary(PIHandler):
    assert PIHandler._is_summary(ReaderType.AVG)
    assert PIHandler._is_summary(ReaderType.MIN)
    assert PIHandler._is_summary(ReaderType.MAX)
    assert PIHandler._is_summary(ReaderType.RNG)
    assert PIHandler._is_summary(ReaderType.STD)
    assert PIHandler._is_summary(ReaderType.VAR)
    assert not PIHandler._is_summary(ReaderType.RAW)
    assert not PIHandler._is_summary(ReaderType.SHAPEPRESERVING)
    assert not PIHandler._is_summary(ReaderType.INT)
    assert not PIHandler._is_summary(ReaderType.GOOD)
    assert not PIHandler._is_summary(ReaderType.BAD)
    assert not PIHandler._is_summary(ReaderType.SNAPSHOT)


@pytest.mark.parametrize(
    "read_type",
    [
        # pytest.param("RAW", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param(
        #     "SHAPEPRESERVING", marks=pytest.mark.skip(reason="Not implemented")
        # ),
        "INT",
        "MIN",
        "MAX",
        "RNG",
        "AVG",
        "STD",
        "VAR",
        # pytest.param("COUNT", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SNAPSHOT", marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_generate_read_query(PIHandler, read_type):  # TODO: Move away from test*connect
    starttime = ensure_datetime_with_tz(START_TIME)
    stoptime = ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(SAMPLE_TIME, unit="s")

    (url, params) = PIHandler.generate_read_query(
        PIHandler.tag_to_webid("alreadyknowntag"),
        starttime,
        stoptime,
        ts,
        getattr(ReaderType, read_type),
    )
    assert params["startTime"] == "01-Apr-20 09:05:00"
    assert params["endTime"] == "01-Apr-20 10:05:00"

    if read_type == "INT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/interpolated"
        assert (
            params["selectedFields"] == "Links;Items.Timestamp;Items.Value;Items.Good"
        )
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/summary"
        assert (
            params["selectedFields"]
            == "Links;Items.Value.Timestamp;Items.Value.Value;Items.Value.Good"
        )
        assert {
            "AVG": "Average",
            "MIN": "Minimum",
            "MAX": "Maximum",
            "RNG": "Range",
            "STD": "StdDev",
            "VAR": "StdDev",
        }.get(read_type) == params["summaryType"]
        assert params["summaryDuration"] == f"{SAMPLE_TIME}s"
