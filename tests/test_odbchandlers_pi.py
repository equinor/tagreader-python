import pytest
import pandas as pd
from tagreader import utils
from tagreader.utils import ReaderType


@pytest.fixture(scope="module")
def PIHandler():
    from tagreader.odbc_handlers import PIHandlerODBC

    yield PIHandlerODBC("thehostname.statoil.net", 1234)


def test_generate_connection_string(PIHandler):
    res = PIHandler.generate_connection_string(
        PIHandler.host, PIHandler.port, "das_server"
    )
    expected = (
        "DRIVER={PI ODBC Driver};Server=das_server;Trusted_Connection=Yes;"
        "Command Timeout=1800;Provider Type=PIOLEDB;"
        "Provider String={Data source=thehostname;Integrated_Security=SSPI;};"
    )
    assert expected == res


def test_generate_tag_read_query(PIHandler):
    start_time = utils.datestr_to_datetime("2018-01-17 16:00:00")
    stop_time = utils.datestr_to_datetime("2018-01-17 17:00:00")
    ts = pd.Timedelta(1, unit="m")
    res = PIHandler.generate_read_query(
        "thetag", start_time, stop_time, ts, ReaderType.INT
    )
    expected = (
        "SELECT CAST(value as FLOAT32) AS value, __utctime AS timestamp "
        "FROM [piarchive]..[piinterp2] WHERE tag='thetag' "
        "AND (time BETWEEN '17-Jan-18 16:00:00' AND '17-Jan-18 17:00:00') "
        "AND (timestep = '60s') ORDER BY timestamp"
    )
    assert expected == res
