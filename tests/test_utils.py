import datetime
import os

import pandas as pd
from pytz import timezone
from tagreader.utils import ensure_datetime_with_tz, is_equinor, urljoin

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ


def test_ensure_is_datetime_string():
    assert ensure_datetime_with_tz("10. jan. 2018 13:45:15") == timezone(
        "Europe/Oslo"
    ).localize(datetime.datetime(2018, 1, 10, 13, 45, 15))
    assert ensure_datetime_with_tz("02.01.03 00:00:00") == timezone(
        "Europe/Oslo"
    ).localize(datetime.datetime(2003, 1, 2, 0, 0, 0))
    assert ensure_datetime_with_tz("02.01.03 00:00:00") == ensure_datetime_with_tz(
        "2003-02-01 0:00:00am"
    )
    assert ensure_datetime_with_tz(
        "02.01.03 00:00:00", "America/Sao_Paulo"
    ) == timezone("America/Sao_Paulo").localize(datetime.datetime(2003, 1, 2, 0, 0, 0))
    assert ensure_datetime_with_tz("02.01.03 00:00:00", tz="Brazil/East") == timezone(
        "Brazil/East"
    ).localize(datetime.datetime(2003, 1, 2, 0, 0, 0))
    assert ensure_datetime_with_tz(
        timezone("Brazil/East").localize(datetime.datetime(2003, 1, 2, 0, 0, 0)),
        tz="Europe/Oslo",
    ) == timezone("Brazil/East").localize(datetime.datetime(2003, 1, 2, 0, 0, 0))


def test_ensure_is_datetime_pd_timestamp():
    ts = pd.Timestamp(2018, 1, 10, 13, 45, 15)
    ts_with_tz = timezone("Europe/Oslo").localize(ts)
    assert ensure_datetime_with_tz(ts_with_tz) == ts_with_tz
    assert ensure_datetime_with_tz(ts) == ts_with_tz


def test_ensure_is_datetime_datetime():
    dt = datetime.datetime(2018, 1, 10, 13, 45, 15)
    dt_with_tz = timezone("Europe/Oslo").localize(dt)

    assert ensure_datetime_with_tz(dt_with_tz) == dt_with_tz
    assert ensure_datetime_with_tz(dt) == dt_with_tz


def test_urljoin():
    assert urljoin("https://some.where/to", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "go/") == "https://some.where/to/go/"


def test_equinor():
    if is_GITHUBACTION:
        assert is_equinor() is False
    else:
        assert is_equinor() is True
