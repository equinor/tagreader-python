import datetime
import os
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest

import tagreader.utils as utils
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


def test_ensure_datetime_with_tz_uses_localize_when_available() -> None:
    class LocalizeTimezone(datetime.tzinfo):
        def __init__(self, offset_hours: int) -> None:
            self._offset = datetime.timedelta(hours=offset_hours)

        def utcoffset(
            self, dt: Optional[datetime.datetime]
        ) -> Optional[datetime.timedelta]:
            return self._offset

        def dst(self, dt: Optional[datetime.datetime]) -> Optional[datetime.timedelta]:
            return datetime.timedelta(0)

        def tzname(self, dt: Optional[datetime.datetime]) -> Optional[str]:
            return "LOCALIZE_TZ"

        def localize(self, dt: datetime.datetime) -> datetime.datetime:
            return dt.replace(tzinfo=self)

    dt = datetime.datetime(2020, 3, 10, 12, 0, 0)
    tz = LocalizeTimezone(offset_hours=2)

    converted = ensure_datetime_with_tz(dt, tz)

    assert converted.tzinfo is tz
    assert converted.utcoffset() == datetime.timedelta(hours=2)


def test_ensure_datetime_with_tz_falls_back_to_utc_without_tzdb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_zoneinfo_not_found(*args: object, **kwargs: object) -> ZoneInfo:
        raise ZoneInfoNotFoundError("No time zone found")

    monkeypatch.setattr(utils, "ZoneInfo", raise_zoneinfo_not_found)

    converted = ensure_datetime_with_tz(datetime.datetime(2020, 3, 10, 12, 0, 0))

    assert converted.tzinfo == datetime.timezone.utc


def test_ensure_datetime_with_tz_logs_warning_without_tzdb(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def raise_zoneinfo_not_found(*args: object, **kwargs: object) -> ZoneInfo:
        raise ZoneInfoNotFoundError("No time zone found")

    monkeypatch.setattr(utils, "ZoneInfo", raise_zoneinfo_not_found)
    caplog.clear()

    ensure_datetime_with_tz(datetime.datetime(2020, 3, 10, 12, 0, 0))

    assert "Unable to load timezone 'Europe/Oslo'. Falling back to UTC." in caplog.text


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
