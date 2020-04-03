import pytest
import pandas as pd
from pyims import utils
from pyims.utils import ReaderType
from pyims import IMSClient

from pyims.odbc_handlers import list_aspen_servers,\
    list_pi_servers,\
    PIHandlerODBC,\
    AspenHandlerODBC

from pyims.clients import get_server_address_aspen,\
    get_server_address_pi

def test_init():
    with pytest.raises(ValueError):
        c = IMSClient('xyz')
    with pytest.raises(ValueError):
        c = IMSClient('sNa', 'pi')
    with pytest.raises(ValueError):
        c = IMSClient('Ono-imS', 'aspen')
    with pytest.raises(ValueError):
        c = IMSClient('ono-ims', 'aspen')
    with pytest.raises(ValueError):
        c = IMSClient('sna', 'pi')
    c = IMSClient('onO-iMs', 'pi')
    assert isinstance(c.handler, PIHandlerODBC)
    c = IMSClient('snA', 'aspen')
    assert isinstance(c.handler, AspenHandlerODBC)

@pytest.fixture(scope="module")
def AspenHandler():
    from pyims.odbc_handlers import AspenHandlerODBC
    yield AspenHandlerODBC('thehostname', 1234)
    # Insert any teardown functionality here

def test_generate_connection_string(AspenHandler):
    res = AspenHandler.generate_connection_string(AspenHandler.host, AspenHandler.port, 567890)
    assert "DRIVER={AspenTech SQLPlus};HOST=thehostname;PORT=1234;READONLY=Y;MAXROWS=567890" == res

def test_generate_tag_read_query(AspenHandler):
    start_time = utils.datestr_to_datetime('2018-01-17 16:00:00')
    stop_time = utils.datestr_to_datetime('2018-01-17 17:00:00')
    ts = pd.Timedelta(1, unit='m')
    res = AspenHandler.generate_read_query('thetag', None, start_time, stop_time, ts, ReaderType.INT)
    expected = ("SELECT CAST(ts AS CHAR FORMAT 'YYYY-MM-DD HH:MI:SS') AS \"time\", value AS \"value\" FROM history "
                "WHERE name = 'thetag' AND (ts BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') AND "
                "(request = 6) AND (period = 600) ORDER BY ts")
    assert expected == res


def test_get_aspen_server_address():
    host, port = get_server_address_aspen('SNA')
    assert 'SNA-IMS.statoil.net' == host
    assert 10014 == port

@pytest.fixture(scope="module")
def PIHandler():
    from pyims.odbc_handlers import PIHandlerODBC
    yield PIHandlerODBC('thehostname.statoil.net', 1234)

def test_generate_connection_string(PIHandler):
    res = PIHandler.generate_connection_string(PIHandler.host, PIHandler.port, 'das_server')
    expected = ("DRIVER={PI ODBC Driver};Server=das_server;Trusted_Connection=Yes;Command Timeout=1800;"
            "Provider Type=PIOLEDB;Provider String={Data source=thehostname;Integrated_Security=SSPI;};")
    assert expected == res

def test_generate_tag_read_query(PIHandler):
    start_time = utils.datestr_to_datetime('2018-01-17 16:00:00')
    stop_time = utils.datestr_to_datetime('2018-01-17 17:00:00')
    ts = pd.Timedelta(1, unit='m')
    res = PIHandler.generate_read_query('thetag', start_time, stop_time, ts, ReaderType.INT)
    expected = ("SELECT CAST(value as FLOAT32) AS value, __utctime AS timestamp "
                "FROM [piarchive]..[piinterp2] WHERE tag='thetag' AND "
                "(time BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') AND (timestep = '60s') ORDER BY timestamp")
    assert expected == res



