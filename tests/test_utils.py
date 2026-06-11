import datetime
import os
from zoneinfo import ZoneInfo

import pandas as pd

from tagreader.utils import ensure_datetime_with_tz, is_equinor, urljoin

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ


def test_ensure_is_datetime_string() -> None:
    assert ensure_datetime_with_tz("10. jan. 2018 13:45:15") == datetime.datetime(
        2018, 1, 10, 13, 45, 15, tzinfo=ZoneInfo("Europe/Oslo")
    )
    assert ensure_datetime_with_tz("01.02.03 00:00:00") == datetime.datetime(
        2003, 2, 1, 0, 0, 0, tzinfo=ZoneInfo("Europe/Oslo")
    )
    assert ensure_datetime_with_tz("02.01.03 00:00:00") == ensure_datetime_with_tz(
        "2003-02-01 0:00:00am"
    )
    assert ensure_datetime_with_tz(
        "02.01.03 00:00:00", ZoneInfo("America/Sao_Paulo")
    ) == datetime.datetime(2003, 1, 2, 0, 0, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert ensure_datetime_with_tz(
        "02.01.03 00:00:00", ZoneInfo("America/Sao_Paulo")
    ) == datetime.datetime(2003, 1, 2, 0, 0, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert ensure_datetime_with_tz(
        datetime.datetime(2003, 1, 2, 0, 0, 0, tzinfo=ZoneInfo("America/Sao_Paulo")),
        ZoneInfo("America/Sao_Paulo"),
    ) == datetime.datetime(2003, 1, 2, 0, 0, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))


def test_ensure_is_datetime_pd_timestamp() -> None:
    ts = datetime.datetime(2018, 1, 10, 13, 45, 15)
    ts_with_tz = datetime.datetime(
        2018, 1, 10, 13, 45, 15, tzinfo=ZoneInfo("Europe/Oslo")
    )
    assert ensure_datetime_with_tz(ts_with_tz) == ts_with_tz
    assert ensure_datetime_with_tz(ts) == ts_with_tz


def test_ensure_is_datetime_datetime() -> None:
    dt = datetime.datetime(2018, 1, 10, 13, 45, 15)
    dt_with_tz = datetime.datetime(
        2018, 1, 10, 13, 45, 15, tzinfo=ZoneInfo("Europe/Oslo")
    )

    assert ensure_datetime_with_tz(dt_with_tz) == dt_with_tz
    assert ensure_datetime_with_tz(dt) == dt_with_tz


def test_urljoin() -> None:
    assert urljoin("https://some.where/to", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "go/") == "https://some.where/to/go/"


def test_equinor() -> None:
    if is_GITHUBACTION:
        assert is_equinor() is False
    else:
        assert is_equinor() is True
