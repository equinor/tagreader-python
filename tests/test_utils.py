import datetime
from pytz import timezone

from tagreader.utils import (
    ensure_datetime_with_tz,
    urljoin,
)


def test_ensure_is_datetime():
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


def test_urljoin():
    assert urljoin("https://some.where/to", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to/", "/go") == "https://some.where/to/go"
    assert urljoin("https://some.where/to", "go/") == "https://some.where/to/go/"
