import pytest
import pandas as pd
from readertype import ReaderType

@pytest.fixture(scope="module")
def client():
    from imsclient.clients import IMSClient
    yield IMSClient('dummy')

def test_get_next_timeslice(client):
    start_time = pd.to_datetime('2018-01-02 14:00:00')
    stop_time = pd.to_datetime('2018-01-02 14:15:00')
    #taglist = ['tag1', 'tag2', 'tag3']
    ts = pd.Timedelta(60, unit='s')
    res = client._get_next_timeslice(start_time, stop_time, ts, max_steps=20)
    assert start_time, start_time + pd.Timedelta(6, unit='m') == res
    res = client._get_next_timeslice(start_time, stop_time, ts, max_steps=100000)
    assert start_time, stop_time == res


def test_get_missing_interval(client):
    length = 10
    ts = 60
    data = {'tag1': range(0, length)}
    idx = pd.date_range(start="2018-01-18 05:00:00", freq=f"{ts}s", periods=length, name="time")
    df_total = pd.DataFrame(data, index=idx)
    df = pd.concat([df_total.iloc[0:2], df_total.iloc[3:4], df_total.iloc[8:]])
    missing = client._get_missing_intervals(df, start_time="2018-01-18 05:00:00", stop_time="2018-01-18 06:00:00", ts=ts, read_type=ReaderType.INT)
    assert missing[0] == (idx[2], idx[2])
    assert missing[1] == (idx[4], idx[7])
    assert missing[2] == (pd.Timestamp("2018-01-18 05:10:00"), pd.Timestamp("2018-01-18 06:00:00"))