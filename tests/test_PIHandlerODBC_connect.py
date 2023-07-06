import os
from datetime import datetime, timedelta
from typing import Generator

import pandas as pd
import pytest

from tagreader.clients import IMSClient, list_sources
from tagreader.utils import ReaderType, ensure_datetime_with_tz, is_windows

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)

import pyodbc

from tagreader.odbc_handlers import list_pi_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

SOURCE = "PIMAM"

TAGS = {
    "Float32": "CDT158",  # BA:CONC.1
    "Digital": "CDM158",  # BA:ACTIVE.1
    "Int32": "CDEP158",
}

START_TIME = datetime(2020, 4, 1, 11, 5, 0)
STOP_TIME = datetime(2020, 4, 1, 12, 5, 0)
SAMPLE_TIME = timedelta(seconds=60)


@pytest.fixture  # type: ignore[misc]
def client() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=SOURCE, imstype="pi")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


def test_list_all_pi_sources() -> None:
    res = list_pi_sources()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r)


def test_list_sources_pi() -> None:
    res = list_sources(imstype="pi")
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r)


def test_search_tag(client: IMSClient) -> None:
    res = client.search(tag="SINUSOID")
    assert 1 == len(res)
    res = client.search(tag="SIN*")
    assert 3 <= len(res)
    [taglist, desclist] = zip(*res)
    assert "SINUSOIDU" in taglist
    assert desclist[taglist.index("SINUSOID")] == "12 Hour Sine Wave"
    res = client.search(tag=None, desc="12 Hour Sine Wave")
    assert 1 <= len(res)
    res = client.search("SINUSOID", desc="*Sine*")
    assert 1 <= len(res)


@pytest.mark.parametrize(  # type: ignore[misc]
    ("read_type", "size"),
    [
        ("RAW", 10),
        # pytest.param(
        #     "SHAPEPRESERVING", 0, marks=pytest.mark.skip(reason="Not implemented")
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
def test_read(client: IMSClient, read_type: str, size: int) -> None:
    if read_type == "SNAPSHOT":
        df = client.read(
            tags=TAGS["Float32"],
            read_type=getattr(ReaderType, read_type),
            start_time=None,
            end_time=None,
        )
    else:
        df = client.read(
            tags=TAGS["Float32"],
            start_time=START_TIME,
            end_time=STOP_TIME,
            ts=SAMPLE_TIME,
            read_type=getattr(ReaderType, read_type),
        )
    assert df.shape == (size, 1)
    if read_type not in ["SNAPSHOT", "RAW"]:
        assert df.index[0] == ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] == df.index[0] + (size - 1) * SAMPLE_TIME  # type: ignore[operator]
    elif read_type in "RAW":
        assert df.index[0] >= ensure_datetime_with_tz(START_TIME)  # type: ignore[operator]
        assert df.index[-1] <= ensure_datetime_with_tz(STOP_TIME)  # type: ignore[operator]


def test_read_with_status(client: IMSClient) -> None:
    df = client.read(
        tags=TAGS["Float32"],
        start_time=START_TIME,
        end_time=STOP_TIME,
        ts=SAMPLE_TIME,
        read_type=ReaderType.RAW,
        get_status=True,
    )
    assert df.shape == (10, 2)
    assert df[TAGS["Float32"] + "::status"].iloc[0] == 0


def test_digitalread_yields_integers(client: IMSClient) -> None:
    tag = TAGS["Digital"]
    df = client.read(
        tags=tag,
        start_time=START_TIME,
        end_time=STOP_TIME,
        ts=600,
        read_type=ReaderType.INT,
    )
    assert all(x.is_integer() for x in df[tag])


def test_get_unit(client: IMSClient) -> None:
    res = client.get_units(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "DEG. C"
    assert res[TAGS["Digital"]] == "STATE"
    assert res[TAGS["Int32"]] == ""


def test_get_description(client: IMSClient) -> None:
    res = client.get_descriptions(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "Atmospheric Tower OH Vapor"
    assert res[TAGS["Digital"]] == "Light Naphtha End Point Control"
    assert res[TAGS["Int32"]] == "Light Naphtha End Point"


def test_from_dst_folds_time(client: IMSClient) -> None:
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = client.read(tags=[tag], start_time=interval[0], end_time=interval[1], ts=600)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df[tag].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size == 5  # type: ignore[misc]
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1  # type: ignore[misc]
    )


def test_to_dst_skips_time(client: IMSClient) -> None:
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2018-03-25 00:30:00", "2018-03-25 03:30:00"]
    df = client.read(tags=[tag], start_time=interval[0], end_time=interval[1], ts=600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6  # type: ignore[misc]
    )


def test_tags_with_no_data_included_in_results(client: IMSClient) -> None:
    df = client.read(
        tags=[TAGS["Float32"]],
        start_time="2099-01-01 00:00:00",
        end_time="2099-01-02 00:00:00",
    )
    assert len(df.columns) == 1


def test_query_sql(client: IMSClient) -> None:
    tag = TAGS["Float32"]
    query = f"SELECT descriptor, engunits FROM pipoint.pipoint2 WHERE tag='{tag}'"
    res = client.query_sql(query=query, parse=True)
    assert isinstance(res, pd.DataFrame)
    assert res.shape[0] >= 1
    assert res.shape[1] == 2
    res = client.query_sql(query=query, parse=False)
    assert isinstance(res, pyodbc.Cursor)
    rows = res.fetchall()
    assert len(rows) >= 1
    assert len(rows[0]) == 2
