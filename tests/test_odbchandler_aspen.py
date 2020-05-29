import pytest
import pandas as pd
from tagreader import utils
from tagreader.utils import ReaderType


@pytest.fixture(scope="module")
def AspenHandler():
    from tagreader.odbc_handlers import AspenHandlerODBC

    yield AspenHandlerODBC("thehostname", 1234)
    # Insert any teardown functionality here


def test_generate_connection_string(AspenHandler):
    res = AspenHandler.generate_connection_string(
        AspenHandler.host, AspenHandler.port, 567890
    )
    expected = (
        "DRIVER={AspenTech SQLPlus};HOST=thehostname;PORT=1234;"
        "READONLY=Y;MAXROWS=567890"
    )
    assert expected == res


def test_generate_tag_read_query(AspenHandler):
    start_time = utils.datestr_to_datetime("2018-01-17 16:00:00")
    stop_time = utils.datestr_to_datetime("2018-01-17 17:00:00")
    ts = pd.Timedelta(1, unit="m")
    res = AspenHandler.generate_read_query(
        "thetag", None, start_time, stop_time, ts, ReaderType.INT
    )
    expected = (
        'SELECT CAST(ts AS CHAR FORMAT \'YYYY-MM-DD HH:MI:SS\') AS "time", '
        'value AS "value" FROM history WHERE name = '
        "'thetag' AND (ts BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') AND "
        "(request = 6) AND (period = 600) ORDER BY ts"
    )
    assert expected == res
