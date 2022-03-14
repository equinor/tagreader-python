import pandas as pd
import pytest
from tagreader import utils
from tagreader.utils import ReaderType, is_windows

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)

START_TIME = "2018-01-17 16:00:00"
STOP_TIME = "2018-01-17 17:00:00"
SAMPLE_TIME = 60


@pytest.fixture(scope="module")
def PIHandler():
    from tagreader.odbc_handlers import PIHandlerODBC

    yield PIHandlerODBC(
        "thehostname.statoil.net", 1234, options={"das_server": "the_das_server"}
    )


def test_generate_connection_string(PIHandler):
    res = PIHandler.generate_connection_string()
    expected = (
        "DRIVER={PI ODBC Driver};Server=the_das_server;Trusted_Connection=Yes;"
        "Command Timeout=1800;Provider Type=PIOLEDB;"
        "Provider String={Data source=thehostname;Integrated_Security=SSPI;"
        "Time Zone=UTC};"
    )
    assert expected == res


@pytest.mark.parametrize(
    "read_type",
    [
        "RAW",
        # pytest.param(
        #     "SHAPEPRESERVING", marks=pytest.mark.skip(reason="Not implemented")
        # ),
        "INT",
        "MIN",
        "MAX",
        "RNG",
        "AVG",
        "STD",
        "VAR",
        # pytest.param("COUNT", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", marks=pytest.mark.skip(reason="Not implemented")),
        "SNAPSHOT",
    ],
)
def test_generate_tag_read_query(PIHandler, read_type):
    starttime = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(SAMPLE_TIME, unit="s")

    if read_type == "SNAPSHOT":
        res = PIHandler.generate_read_query(
            "thetag", None, None, None, getattr(ReaderType, read_type)
        )
    else:
        res = PIHandler.generate_read_query(
            "thetag", starttime, stoptime, ts, getattr(ReaderType, read_type)
        )

    expected = {
        "RAW": (
            "SELECT TOP 100000 CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[picomp2] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "ORDER BY time"
        ),
        "INT": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[piinterp2] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "MIN": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[pimin] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "MAX": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[pimax] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "RNG": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[pirange] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "AVG": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[piavg] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "STD": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[pistd] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "VAR": (
            "SELECT POWER(CAST(value as FLOAT32), 2) AS value, time "
            "FROM [piarchive]..[pistd] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "SNAPSHOT": (
            "SELECT CAST(value as FLOAT32) AS value, time "
            "FROM [piarchive]..[pisnapshot] WHERE tag='thetag'"
        ),
    }
    assert expected[read_type] == res


@pytest.mark.parametrize(
    "read_type",
    [
        "RAW",
        # pytest.param(
        #     "SHAPEPRESERVING", marks=pytest.mark.skip(reason="Not implemented")
        # ),
        "INT",
        "MIN",
        "MAX",
        "RNG",
        "AVG",
        "STD",
        "VAR",
        # pytest.param("COUNT", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", marks=pytest.mark.skip(reason="Not implemented")),
        "SNAPSHOT",
    ],
)
def test_generate_tag_read_query_with_status(PIHandler, read_type):
    starttime = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(SAMPLE_TIME, unit="s")

    if read_type == "SNAPSHOT":
        res = PIHandler.generate_read_query(
            "thetag", None, None, None, getattr(ReaderType, read_type), get_status=True
        )
    else:
        res = PIHandler.generate_read_query(
            "thetag",
            starttime,
            stoptime,
            ts,
            getattr(ReaderType, read_type),
            get_status=True,
        )

    expected = {
        "RAW": (
            "SELECT TOP 100000 CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[picomp2] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "ORDER BY time"
        ),
        "INT": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[piinterp2] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "MIN": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pimin] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "MAX": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pimax] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "RNG": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pirange] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "AVG": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[piavg] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "STD": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pistd] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "VAR": (
            "SELECT POWER(CAST(value as FLOAT32), 2) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pistd] WHERE tag='thetag' "
            "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
            "AND (timestep = '60s') ORDER BY time"
        ),
        "SNAPSHOT": (
            "SELECT CAST(value as FLOAT32) AS value, "
            "status, questionable, substituted, time "
            "FROM [piarchive]..[pisnapshot] WHERE tag='thetag'"
        ),
    }
    assert expected[read_type] == res


def test_genreadquery_long_sampletime(PIHandler):
    starttime = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(86401, unit="s")

    res = PIHandler.generate_read_query(
        "thetag", starttime, stoptime, ts, ReaderType.INT
    )

    expected = (
        "SELECT CAST(value as FLOAT32) AS value, time "
        "FROM [piarchive]..[piinterp2] WHERE tag='thetag' "
        "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
        "AND (timestep = '86401s') ORDER BY time"
    )

    assert expected == res
