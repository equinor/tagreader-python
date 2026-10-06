from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from requests.auth import AuthBase

from tagreader.cache import SmartCache
from tagreader.clients import IMSClient, get_missing_intervals, get_next_timeslice
from tagreader.utils import IMSType, ReaderType


def test_init_client_without_cache() -> None:
    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    assert not client.cache


def test_multi_read_tags_smartcache_hits(cache: SmartCache) -> None:
    tags = ["tag1", "tag2"]
    sample_time = timedelta(seconds=60)
    cached_data = pd.DataFrame(
        {"tag1": [1.0, 2.0, 3.0, 4.0], "tag2": [5.0, 6.0, 7.0, 8.0]},
        index=pd.date_range("2020-04-01 09:05:00", periods=4, freq="60s", tz="UTC"),
    )
    for tag in tags:
        cache.store(
            df=cached_data[[tag]],
            tagname=tag,
            read_type=ReaderType.INT,
            ts=sample_time,
            get_status=False,
        )
        cache[tag] = f"cached-webid-{tag}"

    client = IMSClient(
        datasource="cache-test",
        imstype=IMSType.PIWEBAPI,
        url="cache-only://pi",
        auth=AuthBase(),
        verify_ssl=True,
        cache=cache,
        tz="Europe/Oslo",
    )
    cache.enable_cache_statistics()
    cache.stats(reset=True)
    expected = cached_data.tz_convert(client.tz)

    try:
        for read_number in (1, 2):
            result = client.multi_read_tags(
                tags=tags,
                start_time=expected.index[0],
                end_time=expected.index[-1],
                ts=60,
                read_type=ReaderType.INT,
            )
            pd.testing.assert_frame_equal(result, expected, check_dtype=False)
            assert cache.stats() == (len(tags) * read_number, 0)
    finally:
        client.handler.session.close()


def test_init_client_with_tzinfo() -> None:
    """
    Currently testing valid timezone
    """
    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Eastern"
    )
    print(client.tz)
    assert client.tz == ZoneInfo("US/Eastern")

    client = IMSClient(
        datasource="mock",
        imstype=IMSType.PIWEBAPI,
        cache=None,
        tz=ZoneInfo("US/Eastern"),
    )
    print(client.tz)
    assert client.tz == ZoneInfo("US/Eastern")

    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="Europe/Oslo"
    )
    print(client.tz)
    assert client.tz == ZoneInfo("Europe/Oslo")

    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Central"
    )
    print(client.tz)
    assert client.tz == ZoneInfo("US/Central")

    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    print(client.tz)
    assert client.tz == ZoneInfo("Europe/Oslo")

    with pytest.raises(ValueError):
        _ = IMSClient(
            datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="WRONGVALUE"
        )


def test_init_client_with_datasource() -> None:
    """
    Currently we initialize SmartCache by default, and the user is not able to specify no-cache when creating the
    client. This will change to no cache by default in version 5.
    """
    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Eastern"
    )
    print(client.tz)
    assert client.tz == ZoneInfo("US/Eastern")
    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Central"
    )
    print(client.tz)
    assert client.tz == ZoneInfo("US/Central")
    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    print(client.tz)
    assert client.tz == ZoneInfo("Europe/Oslo")
    with pytest.raises(ValueError):
        _ = IMSClient(
            datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="WRONGVALUE"
        )


def test_get_next_timeslice() -> None:
    start = pd.to_datetime("2018-01-02 14:00:00")
    end = pd.to_datetime("2018-01-02 14:15:00")
    # taglist = ['tag1', 'tag2', 'tag3']
    ts = timedelta(seconds=60)
    res = get_next_timeslice(start=start, end=end, ts=ts, max_steps=20)
    assert start, start + timedelta(seconds=6) == res
    res = get_next_timeslice(start=start, end=end, ts=ts, max_steps=100000)
    assert start, end == res


def test_get_missing_intervals() -> None:
    length = 10
    ts = 60
    data = {"tag1": range(0, length)}
    idx = pd.date_range(
        start="2018-01-18 05:00:00", freq=f"{ts}s", periods=length, name="time"
    )
    df_total = pd.DataFrame(data, index=idx)
    df = pd.concat([df_total.iloc[0:2], df_total.iloc[3:4], df_total.iloc[8:]])
    missing = get_missing_intervals(
        df=df,
        start=datetime(2018, 1, 18, 5, 0, 0),
        end=datetime(2018, 1, 18, 6, 0, 0),
        ts=timedelta(seconds=ts),
        read_type=ReaderType.INT,
    )
    assert missing[0] == (idx[2], idx[2])
    assert missing[1] == (idx[4], idx[7])
    assert missing[2] == (
        datetime(2018, 1, 18, 5, 10, 0),
        datetime(2018, 1, 18, 6, 0, 0),
    )
