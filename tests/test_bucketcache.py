import pytest
import pandas as pd

from tagreader.utils import ReaderType
from tagreader.cache import safe_tagname, BucketCache


TAGNAME = "tag1"
READERTYPE = ReaderType.INT

TZ = "UTC"
TS = 60
FREQ = f"{TS}s"

STARTTIME_1 = pd.to_datetime("2020-01-01 12:00:00", utc=True)
ENDTIME_1 = pd.to_datetime("2020-01-01 13:00:00", utc=True)
idx = pd.date_range(start=STARTTIME_1, end=ENDTIME_1, freq=FREQ, name="time")
DF1 = pd.DataFrame({TAGNAME: range(0, len(idx))}, index=idx)

STARTTIME_1_EPOCH = (
    STARTTIME_1 - pd.to_datetime("1970-01-01", utc=True)
) // pd.Timedelta(
    "1s"
)  # 1577880000
ENDTIME_1_EPOCH = (ENDTIME_1 - pd.to_datetime("1970-01-01", utc=True)) // pd.Timedelta(
    "1s"
)  # 1577883600

STARTTIME_2 = pd.to_datetime("2020-01-01 13:30:00", utc=True)
ENDTIME_2 = pd.to_datetime("2020-01-01 14:00:00", utc=True)
idx = pd.date_range(start=STARTTIME_2, end=ENDTIME_2, freq=FREQ, name="time")
DF2 = pd.DataFrame({TAGNAME: range(0, len(idx))}, index=idx)

ENDTIME_2_EPOCH = (ENDTIME_2 - pd.to_datetime("1970-01-01", utc=True)) // pd.Timedelta(
    "1s"
)  # 1577887200


STARTTIME_3 = pd.to_datetime("2020-01-01 12:40:00", utc=True)
ENDTIME_3 = pd.to_datetime("2020-01-01 13:40:00", utc=True)
idx = pd.date_range(start=STARTTIME_3, end=ENDTIME_3, freq=FREQ, name="time")
DF3 = pd.DataFrame({TAGNAME: range(0, len(idx))}, index=idx)


@pytest.fixture(autouse=True)
def cache():
    cache = BucketCache("testcache.h5")
    yield cache
    cache.remove()


def test_safe_tagname():
    assert safe_tagname("ASGB.tt-___56_ _%_/_") == "ASGB_tt___56____"


def test_get_intervals_from_dataset_name(cache):
    badtag = f"/tag1/INT/{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    goodtag = f"/tag1/INT/_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    starttime, endtime = cache._get_intervals_from_dataset_name(badtag)
    assert starttime is None
    assert endtime is None
    starttime, endtime = cache._get_intervals_from_dataset_name(goodtag)
    assert starttime == STARTTIME_1
    assert endtime == ENDTIME_1


def test_key_path_with_time(cache):
    assert (
        cache._key_path(
            tagname=TAGNAME,
            readtype=READERTYPE,
            ts=60,
            stepped=False,
            status=False,
            starttime=STARTTIME_1,
            endtime=ENDTIME_1,
        )
        == f"/tag1/INT/s60/_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    )


def test_key_path_stepped(cache):
    assert (
        cache._key_path(
            tagname=TAGNAME,
            readtype=READERTYPE,
            ts=60,
            stepped=True,
            status=False,
            starttime=STARTTIME_1,
            endtime=ENDTIME_1,
        )
        == f"/tag1/INT/s60/stepped/_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    )


def test_key_path_with_status(cache):
    assert (
        cache._key_path(
            tagname=TAGNAME, readtype=READERTYPE, ts=60, stepped=False, status=True,
        )
        == "/tag1/INT/s60/status"
    )


def test_key_path_RAW(cache):
    assert (
        cache._key_path(
            tagname=TAGNAME,
            readtype=ReaderType.RAW,
            ts=60,
            stepped=False,
            status=False,
        )
        == "/tag1/RAW"
    )


def test_get_missing_intervals(cache):
    cache.store(
        DF1, TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )

    cache.store(
        DF2, TAGNAME, READERTYPE, TS, False, False, STARTTIME_2, ENDTIME_2,
    )

    # Perfect coverage, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )

    assert len(missing_intervals) == 0

    # Request subsection, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 + pd.Timedelta("5m"),
        ENDTIME_1 - pd.Timedelta("5m"),
    )

    assert len(missing_intervals) == 0

    # Request data from before to after, two missing intervals
    missing_intervals = cache.get_missing_intervals(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_1 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 2
    assert missing_intervals[0] == [STARTTIME_1 - pd.Timedelta("15m"), STARTTIME_1]
    assert missing_intervals[1] == [ENDTIME_1, ENDTIME_1 + pd.Timedelta("15m")]

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket. Three missing intervals.
    missing_intervals = cache.get_missing_intervals(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_2 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 3
    assert missing_intervals[0] == [STARTTIME_1 - pd.Timedelta("15m"), STARTTIME_1]
    assert missing_intervals[1] == [ENDTIME_1, STARTTIME_2]
    assert missing_intervals[2] == [ENDTIME_2, ENDTIME_2 + pd.Timedelta("15m")]


def test_get_intersecting_datasets(cache):
    cache.store(
        DF1, TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )

    cache.store(
        DF2, TAGNAME, READERTYPE, TS, False, False, STARTTIME_2, ENDTIME_2,
    )

    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )

    # Perfect coverage
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )

    assert len(intersecting_datasets) == 1

    # Request subsection
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 + pd.Timedelta("5m"),
        ENDTIME_1 - pd.Timedelta("5m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data from before to after
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_1 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_2 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from before first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_2 - pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from inside first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 + pd.Timedelta("15m"),
        ENDTIME_2 - pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2


def test_store_single_df(cache):
    cache.store(
        DF1, TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )
    df_read = cache.fetch(TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1)
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)


def test_fetch(cache):
    cache.store(
        DF1, TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )
    cache.store(
        DF2, TAGNAME, READERTYPE, TS, False, False, STARTTIME_2, ENDTIME_2,
    )

    df_read = cache.fetch(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1,
        ENDTIME_1 - pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(DF1.loc[STARTTIME_1:ENDTIME_1-pd.Timedelta("15m")], df_read, check_freq=False)

    df_read = cache.fetch(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_1 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)

    df_read = cache.fetch(
        TAGNAME,
        READERTYPE,
        TS,
        False,
        False,
        STARTTIME_1 - pd.Timedelta("15m"),
        ENDTIME_2 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(DF1.append(DF2), df_read, check_freq=False)

def test_store_overlapping_df(cache):
    cache.store(
        DF1, TAGNAME, READERTYPE, TS, False, False, STARTTIME_1, ENDTIME_1,
    )
    cache.store(
        DF2, TAGNAME, READERTYPE, TS, False, False, STARTTIME_2, ENDTIME_2,
    )
    cache.store(DF3, TAGNAME, READERTYPE, TS, False, False, STARTTIME_3, ENDTIME_3)
    with pd.HDFStore(cache.filename, mode="r") as f:
        keys = list(f.walk(where="/"))
    for key in keys:
        if len(key[2]) > 0:
            leaves = key[2]
    assert len(leaves) == 1
    _, starttime, endtime = leaves[0].split("_")
    assert int(starttime) == STARTTIME_1_EPOCH
    assert int(endtime) == ENDTIME_2_EPOCH

