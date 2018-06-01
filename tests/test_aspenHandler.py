import pytest
import pandas as pd
from pyims import utils
from pyims.utils import ReaderType


@pytest.fixture(scope="module")
def handler():
    from pyims.clients import AspenHandler
    yield AspenHandler()
    # Insert any teardown functionality here

def test_generate_connection_string(handler):
    res = handler.generate_connection_string('thehostname', '1234', 567890)
    assert "DRIVER={AspenTech SQLPlus};HOST=thehostname;PORT=1234;READONLY=Y;MAXROWS=567890" == res


def test_generate_tag_read_query(handler):
    start_time = utils.datestr_to_datetime('2018-01-17 16:00:00')
    stop_time = utils.datestr_to_datetime('2018-01-17 17:00:00')
    ts = pd.Timedelta(1, unit='m')
    res = handler.generate_read_query('thetag', None, start_time, stop_time, ts, ReaderType.INT)
    expected = ("SELECT CAST(ts AS CHAR FORMAT 'YYYY-MM-DD HH:MI:SS') AS \"time\", value AS \"value\" FROM history "
                "WHERE name = 'thetag' AND (ts BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') AND "
                "(request = 6) AND (period = 600) ORDER BY ts")
    assert expected == res

