import os
import pytest
import pandas as pd
from tagreader.utils import ReaderType
from tagreader.clients import (
    get_missing_intervals,
    get_next_timeslice,
    IMSClient,
)
from tagreader.odbc_handlers import (
    AspenHandlerODBC,
    PIHandlerODBC,
)

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ
is_CI = is_GITHUBACTION or is_AZUREPIPELINE


def test_get_next_timeslice():
    start_time = pd.to_datetime("2018-01-02 14:00:00")
    stop_time = pd.to_datetime("2018-01-02 14:15:00")
    # taglist = ['tag1', 'tag2', 'tag3']
    ts = pd.Timedelta(60, unit="s")
    res = get_next_timeslice(start_time, stop_time, ts, max_steps=20)
    assert start_time, start_time + pd.Timedelta(6, unit="m") == res
    res = get_next_timeslice(start_time, stop_time, ts, max_steps=100000)
    assert start_time, stop_time == res


def test_get_missing_intervals():
    length = 10
    ts = 60
    data = {"tag1": range(0, length)}
    idx = pd.date_range(
        start="2018-01-18 05:00:00", freq=f"{ts}s", periods=length, name="time"
    )
    df_total = pd.DataFrame(data, index=idx)
    df = pd.concat([df_total.iloc[0:2], df_total.iloc[3:4], df_total.iloc[8:]])
    missing = get_missing_intervals(
        df,
        start_time="2018-01-18 05:00:00",
        stop_time="2018-01-18 06:00:00",
        ts=ts,
        read_type=ReaderType.INT,
    )
    assert missing[0] == (idx[2], idx[2])
    assert missing[1] == (idx[4], idx[7])
    assert missing[2] == (
        pd.Timestamp("2018-01-18 05:10:00"),
        pd.Timestamp("2018-01-18 06:00:00"),
    )


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_PI_init_odbc_client_with_host_port():
    host = "thehostname"
    port = 999
    c = IMSClient(datasource="whatever", imstype="pi", host=host)
    assert c.handler.host == host
    assert c.handler.port == 5450
    c = IMSClient(datasource="whatever", imstype="pi", host=host, port=port)
    assert c.handler.host == host
    assert c.handler.port == port


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_IP21_init_odbc_client_with_host_port():
    host = "thehostname"
    port = 999
    c = IMSClient(datasource="whatever", imstype="ip21", host=host)
    assert c.handler.host == host
    assert c.handler.port == 10014
    c = IMSClient(datasource="whatever", imstype="ip21", host=host, port=port)
    assert c.handler.host == host
    assert c.handler.port == port


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_PI_connection_string_override():
    connstr = "someuserspecifiedconnectionstring"
    c = IMSClient(
        datasource="whatever",
        host="host",
        imstype="pi",
        handler_options={"connection_string": connstr},
    )
    assert c.handler.generate_connection_string() == connstr


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_IP21_connection_string_override():
    connstr = "someuserspecifiedconnectionstring"
    c = IMSClient(
        datasource="whatever",
        host="host",
        imstype="ip21",
        handler_options={"connection_string": connstr},
    )
    assert c.handler.generate_connection_string() == connstr


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_init_odbc_clients():
    with pytest.raises(ValueError):
        c = IMSClient("xyz")
    with pytest.raises(ValueError):
        c = IMSClient("sNa", "pi")
    with pytest.raises(ValueError):
        c = IMSClient("Ono-imS", "aspen")
    with pytest.raises(ValueError):
        c = IMSClient("ono-ims", "aspen")
    with pytest.raises(ValueError):
        c = IMSClient("sna", "pi")
    c = IMSClient("onO-iMs", "pi")
    assert isinstance(c.handler, PIHandlerODBC)
    c = IMSClient("snA", "aspen")
    assert isinstance(c.handler, AspenHandlerODBC)
