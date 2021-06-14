import os
import pytest
import sys
import pandas as pd
from tagreader.utils import ReaderType
from tagreader.cache import SmartCache, safe_tagname

os.environ["NUMEXPR_MAX_THREADS"] = "8"

if sys.platform == "win32" and sys.version_info >= (3, 9):
    pytest.skip("tables missing for Python 3.9 in Windows", allow_module_level=True)


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
def cache(request):
    cache = SmartCache("testcache.h5")
    yield cache
    cache.remove()


def test_safe_tagname():
    assert safe_tagname("ASGB.tt-___56_ _%_/_") == "ASGB_tt___56____"


def test_key_path(cache):
    pass


def test_cache_single_store_and_fetch(cache, data):
    cache.store(data, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(data, df_read)


def test_cache_multiple_store_single_fetch(cache, data):
    df1 = data[0:3]
    df2 = data[2:10]
    cache.store(df1, readtype=ReaderType.INT)
    cache.store(df2, readtype=ReaderType.INT)
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(df_read, data)


def test_interval_reads(cache, data):
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


def test_match_tag(cache):
    assert (
        cache._match_tag("INT/s60/tag1", readtype=ReaderType.INT, ts=60, tagname="tag1")
        is True
    )
    assert (
        cache._match_tag(
            "INT/s86401/tag1", readtype=ReaderType.INT, ts=86401, tagname="tag1"
        )
        is True
    )
    assert (
        cache._match_tag("INT/s60/tag1", readtype=ReaderType.RAW, ts=60, tagname="tag1")
        is False
    )
    assert (
        cache._match_tag("INT/s60/tag1", readtype=ReaderType.INT, ts=10, tagname="tag1")
        is False
    )
    assert (
        cache._match_tag("INT/s60/tag1", readtype=ReaderType.INT, ts=60, tagname="tag2")
        is False
    )
    assert cache._match_tag("INT/s60/tag1", ts=60, tagname="tag1") is True
    assert (
        cache._match_tag(
            "INT/s60/tag1", readtype=ReaderType.INTERPOLATE, tagname="tag1"
        )
        is True
    )
    assert cache._match_tag("INT/s60/tag1", readtype=ReaderType.INT, ts=60) is True
    assert (
        cache._match_tag(
            "INT/s60/tag1",
            readtype=[ReaderType.INT, ReaderType.RAW],
            ts=[60, 10],
            tagname=["tag1", "tag2"],
        )
        is True
    )
    assert (
        cache._match_tag(
            "INT/s60/tag1",
            readtype=[ReaderType.AVERAGE, ReaderType.RAW],
            ts=[60, 10],
            tagname=["tag1", "tag2"],
        )
        is False
    )
    assert (
        cache._match_tag(
            "INT/s60/tag1",
            readtype=[ReaderType.INT, ReaderType.RAW],
            ts=[120, 10],
            tagname=["tag1", "tag2"],
        )
        is False
    )
    assert (
        cache._match_tag(
            "INT/s60/tag1",
            readtype=[ReaderType.INT, ReaderType.RAW],
            ts=[60, 10],
            tagname=["tag3", "tag2"],
        )
        is False
    )


def test_delete_tag(cache, data):
    cache.store(data, readtype=ReaderType.INT)
    cache.store(data, readtype=ReaderType.RAW)
    with cache._get_hdfstore() as f:
        assert "INT/s60/tag1" in f
        assert "RAW/tag1" in f
    cache.delete_key("tag1", ReaderType.INT, 60)
    cache.delete_key("tag1")
    with cache._get_hdfstore() as f:
        assert "INT/s60/tag1" not in f
        assert "RAW/tag1" not in f


def test_store_empty_df(cache, data):
    # Empty dataframes should not be stored (note: df full of NaN is not empty!)
    cache.store(data, readtype=ReaderType.INT)
    df = pd.DataFrame({"tag1": []})
    cache.store(
        df, readtype=ReaderType.INT, ts=60
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch("tag1", ReaderType.INT, 60)
    pd.testing.assert_frame_equal(data, df_read)


def test_store_metadata(cache):
    cache.store_tag_metadata("tag1", {"unit": "%", "desc": "Some description"})
    cache.store_tag_metadata("tag1", {"max": 60})
    r = cache.fetch_tag_metadata("tag1", "unit")
    assert "%" == r["unit"]
    r = cache.fetch_tag_metadata("tag1", ["unit", "max", "noworky"])
    assert "%" == r["unit"]
    assert 60 == r["max"]
    assert "noworky" not in r


def test_to_DST_skips_time(cache):
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


def test_from_DST_folds_time(cache):
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
