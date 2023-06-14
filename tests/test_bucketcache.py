from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from tagreader.cache import BucketCache, safe_tagname, timestamp_to_epoch
from tagreader.utils import ReaderType

TAGNAME = "tag1"
READERTYPE = ReaderType.INT

TZ = "UTC"
TS = pd.Timedelta(seconds=300)
FREQ = f"{int(TS.total_seconds())}s"

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


@pytest.fixture(autouse=True)  # type: ignore[misc]
def cache(tmp_path: Path) -> Generator[BucketCache, None, None]:
    cache = BucketCache(directory=tmp_path)
    yield cache


def test_timestamp_to_epoch() -> None:
    # Any timezone or naïve should work
    timestamp = pd.to_datetime("1970-01-01 01:00:00", utc=True)
    assert timestamp_to_epoch(timestamp) == 3600
    timestamp = pd.to_datetime("1970-01-01 01:00:00", utc=False)
    assert timestamp_to_epoch(timestamp) == 3600
    timestamp = pd.to_datetime("1970-01-01 01:00:00", utc=True)
    timestamp = timestamp.tz_convert("Europe/Oslo")
    assert timestamp_to_epoch(timestamp) == 3600


def test_safe_tagname() -> None:
    assert safe_tagname("ASGB.tt-___56_ _%_/_") == "ASGB_tt___56____"


def test_get_intervals_from_dataset_name(cache: BucketCache) -> None:
    badtag = f"/tag1/INT/{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    goodtag = f"/tag1/INT/_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    starttime, endtime = cache._get_intervals_from_dataset_name(badtag)
    assert starttime is None
    assert endtime is None  # type: ignore[unreachable]
    starttime, endtime = cache._get_intervals_from_dataset_name(goodtag)
    assert starttime == STARTTIME_1
    assert endtime == ENDTIME_1


def test_key_path_with_time(cache: BucketCache) -> None:
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
        == f"$tag1$INT$s60$_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    )


def test_key_path_stepped(cache: BucketCache) -> None:
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
        == f"$tag1$INT$s60$stepped$_{STARTTIME_1_EPOCH}_{ENDTIME_1_EPOCH}"
    )


def test_key_path_with_status(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            readtype=READERTYPE,
            ts=60,
            stepped=False,
            status=True,
        )
        == "$tag1$INT$s60$status"
    )


def test_key_path_RAW(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            readtype=ReaderType.RAW,
            ts=60,
            stepped=False,
            status=False,
        )
        == "$tag1$RAW"
    )


def test_get_missing_intervals(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )

    cache.store(
        df=DF2,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_2,
        endtime=ENDTIME_2,
    )

    # Perfect coverage, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )

    assert len(missing_intervals) == 0

    # Request subsection, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 + pd.Timedelta("5m"),
        endtime=ENDTIME_1 - pd.Timedelta("5m"),
    )

    assert len(missing_intervals) == 0

    # Request data from before to after, two missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_1 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 2
    assert missing_intervals[0] == (STARTTIME_1 - pd.Timedelta("15m"), STARTTIME_1)
    assert missing_intervals[1] == (ENDTIME_1, ENDTIME_1 + pd.Timedelta("15m"))

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket. Three missing intervals.
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_2 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 3
    assert missing_intervals[0] == (STARTTIME_1 - pd.Timedelta("15m"), STARTTIME_1)
    assert missing_intervals[1] == (ENDTIME_1, STARTTIME_2)
    assert missing_intervals[2] == (ENDTIME_2, ENDTIME_2 + pd.Timedelta("15m"))


def test_get_intersecting_datasets(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )

    cache.store(
        df=DF2,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_2,
        endtime=ENDTIME_2,
    )

    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )

    # Perfect coverage
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )

    assert len(intersecting_datasets) == 1

    # Request subsection
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 + pd.Timedelta("5m"),
        endtime=ENDTIME_1 - pd.Timedelta("5m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data from before to after
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_1 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_2 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from before first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_2 - pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from inside first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 + pd.Timedelta("15m"),
        endtime=ENDTIME_2 - pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2


def test_store_metadata(cache: BucketCache) -> None:
    cache.put_metadata(key=TAGNAME, value={"unit": "%", "desc": "Some description"})
    cache.put_metadata(key=TAGNAME, value={"max": 60})
    r = cache.get_metadata(TAGNAME, "unit")
    assert isinstance(r, dict)
    assert "%" == r["unit"]
    r = cache.get_metadata(TAGNAME, ["unit", "max", "noworky"])
    assert isinstance(r, dict)
    assert "%" == r["unit"]
    assert 60 == r["max"]
    assert "noworky" not in r


def test_store_empty_df(cache: BucketCache) -> None:
    # Empty dataframes should not be stored (note: df full of NaN is not empty!)
    df = pd.DataFrame({TAGNAME: []})
    cache.store(
        df=df,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    pd.testing.assert_frame_equal(df_read, pd.DataFrame())

    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)

    cache.store(
        df=df,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)


def test_store_single_df(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)


def test_fetch(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    cache.store(
        df=DF2,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_2,
        endtime=ENDTIME_2,
    )

    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1 - pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(
        DF1.loc[STARTTIME_1 : ENDTIME_1 - pd.Timedelta("15m")],  # type: ignore[misc]
        df_read,
        check_freq=False,
    )

    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_1 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)

    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1 - pd.Timedelta("15m"),
        endtime=ENDTIME_2 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(pd.concat([DF1, DF2]), df_read, check_freq=False)


def test_store_overlapping_df(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_1,
    )
    cache.store(
        df=DF2,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_2,
        endtime=ENDTIME_2,
    )
    cache.store(
        df=DF3,
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_3,
        endtime=ENDTIME_3,
    )
    leaves = None
    for key in cache.iterkeys():
        if len(key) > 0:
            leaves = key
    _, starttime, endtime = leaves.split("_")  # type: ignore[union-attr]
    assert int(starttime) == STARTTIME_1_EPOCH
    assert int(endtime) == ENDTIME_2_EPOCH
    df_read = cache.fetch(
        tagname=TAGNAME,
        readtype=READERTYPE,
        ts=TS,
        stepped=False,
        status=False,
        starttime=STARTTIME_1,
        endtime=ENDTIME_2,
    )
    df_expected = pd.concat(
        [
            DF1[STARTTIME_1 : STARTTIME_3 - pd.Timedelta(TS, unit="s")],  # type: ignore[misc]
            DF3[STARTTIME_3:ENDTIME_3],  # type: ignore[misc]
            DF2[ENDTIME_3 + pd.Timedelta(TS, unit="s") : ENDTIME_2],  # type: ignore[misc]
        ]
    )

    pd.testing.assert_frame_equal(
        df_read,
        df_expected,
        check_freq=False,
    )
