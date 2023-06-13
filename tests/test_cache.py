import os
from importlib.util import find_spec

import pandas as pd
import pytest

from tagreader.cache import SmartCache, safe_tagname
from tagreader.utils import ReaderType

os.environ["NUMEXPR_MAX_THREADS"] = "8"


@pytest.fixture()
def data():
    length = 10
    df_total = pd.DataFrame(
        {"tag1": range(0, length)},
        index=pd.date_range(
            start="2018-01-18 05:00:00", freq="60s", periods=length, name="time"
        ),
    )
    yield df_total


@pytest.fixture()
def cache(tmp_path):
    cache = SmartCache(tmp_path)
    yield cache


def test_safe_tagname():
    assert safe_tagname("ASGB.tt-___56_ _%_/_") == "ASGB_tt___56____"


def test_key_path(cache: SmartCache):
    pass


def test_cache_single_store_and_fetch(cache: SmartCache, data):
    cache.store(data, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(data, df_read)


def test_cache_multiple_store_single_fetch(cache: SmartCache, data):
    df1 = data[0:3]
    df2 = data[2:10]
    cache.store(df1, readtype=ReaderType.INT)
    cache.store(df2, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(df_read, data)


def test_interval_reads(cache: SmartCache, data):
    cache.store(data, readtype=ReaderType.INT)
    start_time_oob = pd.to_datetime("2018-01-18 04:55:00")
    start_time = pd.to_datetime("2018-01-18 05:05:00")
    stop_time = pd.to_datetime("2018-01-18 05:08:00")
    stop_time_oob = pd.to_datetime("2018-01-18 06:00:00")

    df_read = cache.fetch("tag1", ReaderType.INT, ts=60, start_time=start_time)
    pd.testing.assert_frame_equal(data[start_time:], df_read)
    df_read = cache.fetch("tag1", ReaderType.INT, ts=60, stop_time=stop_time)
    pd.testing.assert_frame_equal(data[:stop_time], df_read)
    df_read = cache.fetch("tag1", ReaderType.INT, ts=60, start_time=start_time_oob)
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch("tag1", ReaderType.INT, ts=60, stop_time=stop_time_oob)
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch(
        "tag1", ReaderType.INT, ts=60, start_time=start_time, stop_time=stop_time
    )
    pd.testing.assert_frame_equal(data[start_time:stop_time], df_read)


def test_store_empty_df(cache: SmartCache, data):
    # Empty dataframes should not be stored (note: df full of NaN is not empty!)
    cache.store(data, readtype=ReaderType.INT)
    df = pd.DataFrame({"tag1": []})
    cache.store(
        df, readtype=ReaderType.INT, ts=60
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(data, df_read)


def test_store_metadata(cache: SmartCache):
    cache.put_metadata("tag1", {"unit": "%", "desc": "Some description"})
    cache.put_metadata("tag1", {"max": 60})
    r = cache.get_metadata("tag1", "unit")
    assert "%" == r["unit"]
    r = cache.get_metadata("tag1", ["unit", "max", "noworky"])
    assert "%" == r["unit"]
    assert 60 == r["max"]
    assert "noworky" not in r


def test_to_DST_skips_time(cache: SmartCache):
    index = pd.date_range(
        start="2018-03-25 01:50:00",
        end="2018-03-25 03:30:00",
        tz="Europe/Oslo",
        freq="600s",
        name="time",
    )
    index.freq = None
    df = pd.DataFrame({"tag1": range(0, len(index))}, index=index)
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6
    )
    cache.store(df, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 600)
    pd.testing.assert_frame_equal(df_read, df)


def test_from_DST_folds_time(cache: SmartCache):
    index = pd.date_range(
        start="2017-10-29 00:30:00",
        end="2017-10-29 04:30:00",
        tz="Europe/Oslo",
        freq="600s",
        name="time",
    )
    index.freq = None
    df = pd.DataFrame({"tag1": range(0, len(index))}, index=index)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df["tag1"].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size
        == 5
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1
    )
    cache.store(df, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 600)
    pd.testing.assert_frame_equal(df_read, df)
