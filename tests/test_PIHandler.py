import pytest
import pandas as pd
from pyims import utils
from pyims.utils import ReaderType


@pytest.fixture(scope="module")
def handler():
    from pyims.clients import PIHandler
    yield PIHandler()
    # Insert any teardown functionality here

def test_generate_connection_string(handler):
    res = handler.generate_connection_string('thehostname.statoil.net', 1234)
    expected = ("DRIVER={PI ODBC Driver};Server=ST-W4189.statoil.net;Trusted_Connection=Yes;Command Timeout=1800;"
            "Provider Type=PIOLEDB;Provider String={Data source=thehostname;Integrated_Security=SSPI;};")
    assert expected == res

def test_generate_tag_read_query(handler):
    start_time = utils.datestr_to_datetime('2018-01-17 16:00:00')
    stop_time = utils.datestr_to_datetime('2018-01-17 17:00:00')
    ts = pd.Timedelta(1, unit='m')
    res = handler.generate_read_query('thetag', start_time, stop_time, ts, ReaderType.INT)
    expected = ("SELECT CAST(value as FLOAT32) AS value, __utctime AS timestamp "
                "FROM [piarchive]..[piinterp2] WHERE tag='thetag' AND "
                "(time BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') AND (timestep = '60s') ORDER BY timestamp")
    assert expected == res
