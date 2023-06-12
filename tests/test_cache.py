import os
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from tagreader.cache import SmartCache, safe_tagname
from tagreader.utils import ReaderType

os.environ["NUMEXPR_MAX_THREADS"] = "8"


@pytest.fixture  # type: ignore[misc]
def data() -> Generator[pd.DataFrame, None, None]:
    length = 10
    df_total = pd.DataFrame(
        {"tag1": range(0, length)},
        index=pd.date_range(
            start="2018-01-18 05:00:00", freq="60s", periods=length, name="time"
        ),
    )
    yield df_total


@pytest.fixture  # type: ignore[misc]
def cache(tmp_path: Path) -> Generator[SmartCache, None, None]:
    cache = SmartCache(directory=tmp_path)
    yield cache


def test_safe_tagname() -> None:
    assert safe_tagname("ASGB.tt-___56_ _%_/_") == "ASGB_tt___56____"


def test_key_path(cache: SmartCache) -> None:
    pass


def test_cache_single_store_and_fetch(cache: SmartCache, data: pd.DataFrame) -> None:
    cache.store(df=data, readtype=ReaderType.INT)
    df_read = cache.fetch(tagname="tag1", readtype=ReaderType.INT, ts=60)
    pd.testing.assert_frame_equal(data, df_read)


def test_cache_multiple_store_single_fetch(
    cache: SmartCache, data: pd.DataFrame
) -> None:
    df1 = data[0:3]
    df2 = data[2:10]
    cache.store(df=df1, readtype=ReaderType.INT)
    cache.store(df=df2, readtype=ReaderType.INT)
    df_read = cache.fetch(tagname="tag1", readtype=ReaderType.INT, ts=60)
    pd.testing.assert_frame_equal(df_read, data)


def test_interval_reads(cache: SmartCache, data: pd.DataFrame) -> None:
    cache.store(df=data, readtype=ReaderType.INT)
    start_time_oob = pd.to_datetime("2018-01-18 04:55:00")
    start_time = pd.to_datetime("2018-01-18 05:05:00")
    stop_time = pd.to_datetime("2018-01-18 05:08:00")
    stop_time_oob = pd.to_datetime("2018-01-18 06:00:00")

    df_read = cache.fetch(
        tagname="tag1", readtype=ReaderType.INT, ts=60, start_time=start_time
    )
    pd.testing.assert_frame_equal(data[start_time:], df_read)  # type: ignore[misc]
    df_read = cache.fetch(
        tagname="tag1", readtype=ReaderType.INT, ts=60, stop_time=stop_time
    )
    pd.testing.assert_frame_equal(data[:stop_time], df_read)  # type: ignore[misc]
    df_read = cache.fetch(
        tagname="tag1", readtype=ReaderType.INT, ts=60, start_time=start_time_oob
    )
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch(
        tagname="tag1", readtype=ReaderType.INT, ts=60, stop_time=stop_time_oob
    )
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch(
        tagname="tag1",
        readtype=ReaderType.INT,
        ts=60,
        start_time=start_time,
        stop_time=stop_time,
    )
    pd.testing.assert_frame_equal(data[start_time:stop_time], df_read)  # type: ignore[misc]


def test_store_empty_df(cache: SmartCache, data: pd.DataFrame) -> None:
    # Empty dataframes should not be stored (note: df full of NaN is not empty!)
    cache.store(df=data, readtype=ReaderType.INT)
    df = pd.DataFrame({"tag1": []})
    cache.store(
        df=df, readtype=ReaderType.INT, ts=60
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch(tagname="tag1", readtype=ReaderType.INT, ts=60)
    pd.testing.assert_frame_equal(data, df_read)


def test_store_metadata(cache: SmartCache) -> None:
    cache.put_metadata("tag1", {"unit": "%", "desc": "Some description"})
    cache.put_metadata("tag1", {"max": 60})
    r = cache.get_metadata("tag1", "unit")
    assert isinstance(r, dict)
    assert "%" == r["unit"]
    r = cache.get_metadata("tag1", ["unit", "max", "noworky"])
    assert isinstance(r, dict)
    assert "%" == r["unit"]
    assert 60 == r["max"]
    assert "noworky" not in r


def test_to_DST_skips_time(cache: SmartCache) -> None:
    index = pd.date_range(
        start="2018-03-25 01:50:00",
        end="2018-03-25 03:30:00",
        tz="Europe/Oslo",
        freq="600s",
        name="time",
    )
    index.freq = None  # type: ignore[misc]
    df = pd.DataFrame({"tag1": range(0, len(index))}, index=index)
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6  # type: ignore[misc]
    )
    cache.store(df=df, readtype=ReaderType.INT)
    df_read = cache.fetch(tagname="tag1", readtype=ReaderType.INT, ts=600)
    pd.testing.assert_frame_equal(df_read, df)


def test_from_DST_folds_time(cache: SmartCache) -> None:
    index = pd.date_range(
        start="2017-10-29 00:30:00",
        end="2017-10-29 04:30:00",
        tz="Europe/Oslo",
        freq="600s",
        name="time",
    )
    index.freq = None  # type: ignore[misc]
    df = pd.DataFrame({"tag1": range(0, len(index))}, index=index)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df["tag1"].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size  # type: ignore[misc]
        == 5
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1  # type: ignore[misc]
    )
    cache.store(df=df, readtype=ReaderType.INT)
    df_read = cache.fetch(tagname="tag1", readtype=ReaderType.INT, ts=600)
    pd.testing.assert_frame_equal(df_read, df)

def test_webidcache():

    from tagreader.cache import WebIDCache

    webidcache = WebIDCache(filename='test')

    webid = 'F1DPwgwnpmLxqECAJV2HpxdobgmQIAAAUElMQUIuRVFVSU5PUi5DT01cMTMyMC9BSU0sMTctVFQtNzE5Ng'
    tag = 'example_tag_name'
    webidcache.cache[tag] = webid

    webidcache.store()

    del webidcache

    webidcache = WebIDCache(filename='test')

    assert 'example_tag_name' in webidcache.cache.keys()
    assert webidcache.cache['example_tag_name'] == webid

    if os.path.isfile('webid_cache_test.pkl'):
        os.remove('webid_cache_test.pkl')