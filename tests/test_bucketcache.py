from datetime import timedelta
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from tagreader.cache import BucketCache, safe_tagname, timestamp_to_epoch
from tagreader.utils import ReaderType

TAGNAME = "tag1"
READE_TYPE = ReaderType.INT

TZ = "UTC"
TS = timedelta(seconds=300)
MINUTE = timedelta(seconds=60)
FREQ = f"{int(TS.total_seconds())}s"

START_TIME_1 = pd.to_datetime("2020-01-01 12:00:00", utc=True)
END_TIME_1 = pd.to_datetime("2020-01-01 13:00:00", utc=True)
index = pd.date_range(start=START_TIME_1, end=END_TIME_1, freq=FREQ, name="time")
DF1 = pd.DataFrame({TAGNAME: range(0, len(index))}, index=index)

START_TIME_1_EPOCH = (
    START_TIME_1 - pd.to_datetime("1970-01-01", utc=True)
) // pd.Timedelta(
    "1s"
)  # 1577880000
END_TIME_1_EPOCH = (
    END_TIME_1 - pd.to_datetime("1970-01-01", utc=True)
) // pd.Timedelta(
    "1s"
)  # 1577883600

START_TIME_2 = pd.to_datetime("2020-01-01 13:30:00", utc=True)
END_TIME_2 = pd.to_datetime("2020-01-01 14:00:00", utc=True)
index = pd.date_range(start=START_TIME_2, end=END_TIME_2, freq=FREQ, name="time")
DF2 = pd.DataFrame({TAGNAME: range(0, len(index))}, index=index)

END_TIME_2_EPOCH = (
    END_TIME_2 - pd.to_datetime("1970-01-01", utc=True)
) // pd.Timedelta(
    "1s"
)  # 1577887200


START_TIME_3 = pd.to_datetime("2020-01-01 12:40:00", utc=True)
END_TIME_3 = pd.to_datetime("2020-01-01 13:40:00", utc=True)
index = pd.date_range(start=START_TIME_3, end=END_TIME_3, freq=FREQ, name="time")
DF3 = pd.DataFrame({TAGNAME: range(0, len(index))}, index=index)


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
    bad_tag = f"/tag1/INT/{START_TIME_1_EPOCH}_{END_TIME_1_EPOCH}"
    good_tag = f"/tag1/INT/_{START_TIME_1_EPOCH}_{END_TIME_1_EPOCH}"
    start, end = cache._get_intervals_from_dataset_name(bad_tag)
    assert start is None
    assert end is None  # type: ignore[unreachable]
    start, end = cache._get_intervals_from_dataset_name(good_tag)
    assert start == START_TIME_1
    assert end == END_TIME_1


def test_key_path_with_time(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            read_type=READE_TYPE,
            ts=MINUTE,
            stepped=False,
            get_status=False,
            start=START_TIME_1,
            end=END_TIME_1,
        )
        == f"$tag1$INT$s60$_{START_TIME_1_EPOCH}_{END_TIME_1_EPOCH}"
    )


def test_key_path_stepped(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            read_type=READE_TYPE,
            ts=MINUTE,
            stepped=True,
            get_status=False,
            start=START_TIME_1,
            end=END_TIME_1,
        )
        == f"$tag1$INT$s60$stepped$_{START_TIME_1_EPOCH}_{END_TIME_1_EPOCH}"
    )


def test_key_path_with_status(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            read_type=READE_TYPE,
            ts=MINUTE,
            stepped=False,
            get_status=True,
        )
        == "$tag1$INT$s60$status"
    )


def test_key_path_raw(cache: BucketCache) -> None:
    assert (
        cache._key_path(
            tagname=TAGNAME,
            read_type=ReaderType.RAW,
            ts=MINUTE,
            stepped=False,
            get_status=False,
        )
        == "$tag1$RAW"
    )


def test_get_missing_intervals(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )

    cache.store(
        df=DF2,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_2,
        end=END_TIME_2,
    )

    # Perfect coverage, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )

    assert len(missing_intervals) == 0

    # Request subsection, no missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 + pd.Timedelta("5m"),
        end=END_TIME_1 - pd.Timedelta("5m"),
    )

    assert len(missing_intervals) == 0

    # Request data from before to after, two missing intervals
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_1 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 2
    assert missing_intervals[0] == (START_TIME_1 - pd.Timedelta("15m"), START_TIME_1)
    assert missing_intervals[1] == (END_TIME_1, END_TIME_1 + pd.Timedelta("15m"))

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket. Three missing intervals.
    missing_intervals = cache.get_missing_intervals(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_2 + pd.Timedelta("15m"),
    )

    assert len(missing_intervals) == 3
    assert missing_intervals[0] == (START_TIME_1 - pd.Timedelta("15m"), START_TIME_1)
    assert missing_intervals[1] == (END_TIME_1, START_TIME_2)
    assert missing_intervals[2] == (END_TIME_2, END_TIME_2 + pd.Timedelta("15m"))


def test_get_intersecting_datasets(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )

    cache.store(
        df=DF2,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_2,
        end=END_TIME_2,
    )

    # Perfect coverage
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )

    assert len(intersecting_datasets) == 1

    # Request subsection
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 + pd.Timedelta("5m"),
        end=END_TIME_1 - pd.Timedelta("5m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data from before to after
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_1 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 1

    # Request data stretching from before first bucket, including
    # space between buckets, to after second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_2 + pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from before first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_2 - pd.Timedelta("15m"),
    )

    assert len(intersecting_datasets) == 2

    # Request data stretching from inside first bucket, to
    # inside second bucket.
    intersecting_datasets = cache.get_intersecting_datasets(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 + pd.Timedelta("15m"),
        end=END_TIME_2 - pd.Timedelta("15m"),
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
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    pd.testing.assert_frame_equal(df_read, pd.DataFrame())

    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)

    cache.store(
        df=df,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )  # Specify ts to ensure correct key /if/ stored
    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)


def test_store_single_df(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)


def test_fetch(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    cache.store(
        df=DF2,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_2,
        end=END_TIME_2,
    )

    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1 - pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(
        DF1.loc[START_TIME_1 : END_TIME_1 - pd.Timedelta("15m")],  # type: ignore[misc]
        df_read,
        check_freq=False,
    )

    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_1 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(DF1, df_read, check_freq=False)

    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1 - pd.Timedelta("15m"),
        end=END_TIME_2 + pd.Timedelta("15m"),
    )
    pd.testing.assert_frame_equal(pd.concat([DF1, DF2]), df_read, check_freq=False)


def test_store_overlapping_df(cache: BucketCache) -> None:
    cache.store(
        df=DF1,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_1,
    )
    cache.store(
        df=DF2,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_2,
        end=END_TIME_2,
    )
    cache.store(
        df=DF3,
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_3,
        end=END_TIME_3,
    )
    leaves = None
    for key in cache.iterkeys():
        if len(key) > 0:
            leaves = key
    _, start, end = leaves.split("_")  # type: ignore[union-attr]
    assert int(start) == START_TIME_1_EPOCH
    assert int(end) == END_TIME_2_EPOCH
    df_read = cache.fetch(
        tagname=TAGNAME,
        read_type=READE_TYPE,
        ts=TS,
        stepped=False,
        get_status=False,
        start=START_TIME_1,
        end=END_TIME_2,
    )
    df_expected = pd.concat(
        [
            DF1[START_TIME_1 : START_TIME_3 - pd.Timedelta(TS, unit="s")],  # type: ignore[misc]
            DF3[START_TIME_3:END_TIME_3],  # type: ignore[misc]
            DF2[END_TIME_3 + pd.Timedelta(TS, unit="s") : END_TIME_2],  # type: ignore[misc]
        ]
    )

    pd.testing.assert_frame_equal(
        df_read,
        df_expected,
        check_freq=False,
    )
