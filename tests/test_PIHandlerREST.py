from datetime import timedelta
from typing import Generator

import pytest

from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.web_handlers import PIHandlerWeb

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture  # type: ignore[misc]
def PIHandler() -> Generator[PIHandlerWeb, None, None]:
    h = PIHandlerWeb(
        datasource="sourcename", auth=None, options={}, url=None, verifySSL=True
    )
    h.webidcache["alreadyknowntag"] = "knownwebid"
    yield h


def test_escape_chars() -> None:
    assert (
        PIHandlerWeb.escape('+-&|(){}[]^"~*:\\') == r"\+\-\&\|\(\)\{\}\[\]\^\"\~*\:\\"
    )


def test_generate_search_query() -> None:
    assert PIHandlerWeb.generate_search_query(
        tag="SINUSOID", desc=None, datasource=None
    ) == {"q": "name:SINUSOID"}
    assert PIHandlerWeb.generate_search_query(
        tag=r"BA:*.1", desc=None, datasource="sourcename"
    ) == {
        "q": r"name:BA\:*.1",
        "scope": "pi:sourcename",
    }
    assert PIHandlerWeb.generate_search_query(
        tag="BA:*.1", datasource=None, desc=None
    ) == {
        "q": r"name:BA\:*.1",
    }
    assert PIHandlerWeb.generate_search_query(
        desc="Concentration Reactor 1", datasource=None, tag=None
    ) == {
        "q": r"description:Concentration\ Reactor\ 1",
    }
    assert PIHandlerWeb.generate_search_query(
        tag="BA:*.1", desc="Concentration Reactor 1", datasource=None
    ) == {"q": r"name:BA\:*.1 AND description:Concentration\ Reactor\ 1"}


def test_is_summary(PIHandler: PIHandlerWeb) -> None:
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


@pytest.mark.parametrize(  # type: ignore[misc]
    "read_type",
    [
        "RAW",
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
        "SNAPSHOT",
    ],
)
def test_generate_read_query(PIHandler: PIHandlerWeb, read_type: str) -> None:
    starttime = ensure_datetime_with_tz(START_TIME)
    stoptime = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    (url, params) = PIHandler.generate_read_query(
        tag=PIHandler.tag_to_webid(tag="alreadyknowntag"),  # type: ignore[arg-type]
        start_time=starttime,
        stop_time=stoptime,
        sample_time=ts,
        read_type=getattr(ReaderType, read_type),
        metadata=None,
    )
    if read_type != "SNAPSHOT":
        assert params["startTime"] == "01-Apr-20 09:05:00"
        assert params["endTime"] == "01-Apr-20 10:05:00"
        assert params["timeZone"] == "UTC"

    if read_type == "INT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/interpolated"
        assert params["selectedFields"] == "Links;Items.Timestamp;Items.Value"
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/summary"
        assert (
            params["selectedFields"] == "Links;Items.Value.Timestamp;Items.Value.Value"
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
    elif read_type == "SNAPSHOT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/value"
        assert params["selectedFields"] == "Timestamp;Value"
        assert len(params) == 3
    elif read_type == "RAW":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/recorded"
        assert params["selectedFields"] == "Links;Items.Timestamp;Items.Value"
        assert params["maxCount"] == 10000  # type: ignore[comparison-overlap]


@pytest.mark.parametrize(  # type: ignore[misc]
    "read_type",
    [
        "RAW",
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
        "SNAPSHOT",
    ],
)
def test_generate_read_query_with_status(
    PIHandler: PIHandlerWeb, read_type: str
) -> None:
    starttime = ensure_datetime_with_tz(START_TIME)
    stoptime = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    (url, params) = PIHandler.generate_read_query(
        tag=PIHandler.tag_to_webid("alreadyknowntag"),  # type: ignore[arg-type]
        start_time=starttime,
        stop_time=stoptime,
        sample_time=ts,
        read_type=getattr(ReaderType, read_type),
        get_status=True,
        metadata=None,
    )
    if read_type != "SNAPSHOT":
        assert params["startTime"] == "01-Apr-20 09:05:00"
        assert params["endTime"] == "01-Apr-20 10:05:00"
        assert params["timeZone"] == "UTC"

    if read_type == "INT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/interpolated"
        assert params["selectedFields"] == (
            "Links;Items.Timestamp;Items.Value;"
            "Items.Good;Items.Questionable;Items.Substituted"
        )
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/summary"
        assert params["selectedFields"] == (
            "Links;Items.Value.Timestamp;Items.Value.Value;"
            "Items.Value.Good;Items.Value.Questionable;Items.Value.Substituted"
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
    elif read_type == "SNAPSHOT":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/value"
        assert (
            params["selectedFields"] == "Timestamp;Value;Good;Questionable;Substituted"
        )
        assert len(params) == 3
    elif read_type == "RAW":
        assert url == f"streams/{PIHandler.webidcache['alreadyknowntag']}/recorded"
        assert params["selectedFields"] == (
            "Links;Items.Timestamp;Items.Value;"
            "Items.Good;Items.Questionable;Items.Substituted"
        )
        assert params["maxCount"] == 10000  # type: ignore[comparison-overlap]


def test_genreadquery_long_sampletime(PIHandler: PIHandlerWeb) -> None:
    starttime = ensure_datetime_with_tz(START_TIME)
    stoptime = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=86410)

    (url, params) = PIHandler.generate_read_query(
        tag=PIHandler.tag_to_webid("alreadyknowntag"),  # type: ignore[arg-type]
        start_time=starttime,
        stop_time=stoptime,
        sample_time=ts,
        read_type=ReaderType.INT,
        metadata=None,
    )
    assert params["interval"] == f"{86410}s"
