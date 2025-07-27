from datetime import datetime, timedelta

import pandas as pd
import pytest
import pytz

from tagreader.clients import IMSClient, get_missing_intervals, get_next_timeslice
from tagreader.utils import IMSType, ReaderType


def test_init_client_without_cache() -> None:
    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    assert not client.cache


def test_init_client_with_tzinfo() -> None:
    """
    Currently testing valid timezone
    """
    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Eastern"
    )
    print(client.tz)
    assert client.tz == pytz.timezone("US/Eastern")

    client = IMSClient(
        datasource="mock",
        imstype=IMSType.PIWEBAPI,
        cache=None,
        tz=pytz.timezone("US/Eastern"),
    )
    print(client.tz)
    assert client.tz == pytz.timezone("US/Eastern")

    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="Europe/Oslo"
    )
    print(client.tz)
    assert client.tz == pytz.timezone("Europe/Oslo")

    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Central"
    )
    print(client.tz)
    assert client.tz == pytz.timezone("US/Central")

    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    print(client.tz)
    assert client.tz == pytz.timezone("Europe/Oslo")

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
    assert client.tz == pytz.timezone("US/Eastern")
    client = IMSClient(
        datasource="mock", imstype=IMSType.PIWEBAPI, cache=None, tz="US/Central"
    )
    print(client.tz)
    assert client.tz == pytz.timezone("US/Central")
    client = IMSClient(datasource="mock", imstype=IMSType.PIWEBAPI, cache=None)
    print(client.tz)
    assert client.tz == pytz.timezone("Europe/Oslo")
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
