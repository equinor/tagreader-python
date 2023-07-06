import os
from datetime import datetime, timedelta

import pandas as pd
import pytest

from tagreader.clients import IMSClient, get_missing_intervals, get_next_timeslice
from tagreader.utils import ReaderType, is_windows

if is_windows():
    from tagreader.odbc_handlers import AspenHandlerODBC, PIHandlerODBC

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ
is_CI = is_GITHUBACTION or is_AZUREPIPELINE


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


@pytest.mark.skipif(
    is_GITHUBACTION or not is_windows(),
    reason="ODBC drivers require Windows and are unavailable in GitHub Actions",
)
class TestODBC:
    def test_pi_init_odbc_client_with_host_port(self) -> None:
        host = "thehostname"
        port = 999
        c = IMSClient(datasource="whatever", imstype="pi", host=host)
        assert c.handler.host == host
        assert c.handler.port == 5450
        c = IMSClient(datasource="whatever", imstype="pi", host=host, port=port)
        assert c.handler.host == host
        assert c.handler.port == port

    def test_ip21_init_odbc_client_with_host_port(self) -> None:
        host = "thehostname"
        port = 999
        c = IMSClient(datasource="whatever", imstype="ip21", host=host)
        assert c.handler.host == host
        assert c.handler.port == 10014
        c = IMSClient(datasource="whatever", imstype="ip21", host=host, port=port)
        assert c.handler.host == host
        assert c.handler.port == port

    def test_pi_connection_string_override(self) -> None:
        connstr = "someuserspecifiedconnectionstring"
        c = IMSClient(
            datasource="whatever",
            host="host",
            imstype="pi",
            handler_options={"connection_string": connstr},
        )
        assert c.handler.generate_connection_string() == connstr

    def test_ip21_connection_string_override(self) -> None:
        connstr = "someuserspecifiedconnectionstring"
        c = IMSClient(
            datasource="whatever",
            host="host",
            imstype="ip21",
            handler_options={"connection_string": connstr},
        )
        assert c.handler.generate_connection_string() == connstr

    def test_init_odbc_clients(self) -> None:
        with pytest.raises(ValueError):
            _ = IMSClient(datasource="xyz")
        with pytest.raises(ValueError):
            _ = IMSClient(datasource="sNa", imstype="pi")
        with pytest.raises(ValueError):
            _ = IMSClient(datasource="Ono-imS", imstype="aspen")
        with pytest.raises(ValueError):
            _ = IMSClient(datasource="ono-ims", imstype="aspen")
        with pytest.raises(ValueError):
            _ = IMSClient(datasource="sna", imstype="pi")
        c = IMSClient(datasource="onO-iMs", imstype="pi")
        assert isinstance(c.handler, PIHandlerODBC)
        c = IMSClient(datasource="snA", imstype="aspen")
        assert isinstance(c.handler, AspenHandlerODBC)
