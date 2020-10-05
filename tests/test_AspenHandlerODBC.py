import pytest
import pandas as pd
from tagreader import utils
from tagreader.utils import ReaderType
from tagreader.odbc_handlers import AspenHandlerODBC

START_TIME = "2018-01-17 16:00:00"
STOP_TIME = "2018-01-17 17:00:00"
SAMPLE_TIME = 60


@pytest.fixture(scope="module")
def AspenHandler():
    from tagreader.odbc_handlers import AspenHandlerODBC

    yield AspenHandlerODBC(
        "thehostname", 1234, options={"max_rows": 567890}
    )


def test_generate_connection_string(AspenHandler):
    res = AspenHandler.generate_connection_string()
    expected = (
        "DRIVER={AspenTech SQLPlus};HOST=thehostname;PORT=1234;"
        "READONLY=Y;MAXROWS=567890"
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
def test_generate_tag_read_query(read_type):
    starttime = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(SAMPLE_TIME, unit="s")

    if read_type == "SNAPSHOT":
        res = AspenHandlerODBC.generate_read_query(
            "thetag", None, None, None, None, getattr(ReaderType, read_type)
        )
    else:
        res = AspenHandlerODBC.generate_read_query(
            "thetag", None, starttime, stoptime, ts, getattr(ReaderType, read_type)
        )

    expected = {
        "RAW": (
            'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
            "name = 'thetag' AND (request = 4) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "INT": (
            'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 7) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MIN": (
            'SELECT ISO8601(ts_start) AS "time", min AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MAX": (
            'SELECT ISO8601(ts_start) AS "time", max AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "RNG": (
            'SELECT ISO8601(ts_start) AS "time", rng AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "AVG": (
            'SELECT ISO8601(ts_start) AS "time", avg AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "STD": (
            'SELECT ISO8601(ts_start) AS "time", std AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "VAR": (
            'SELECT ISO8601(ts_start) AS "time", var AS "value" FROM aggregates WHERE '
            "name = 'thetag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "SNAPSHOT": (
            'SELECT ISO8601(IP_INPUT_TIME) AS "time", IP_INPUT_VALUE AS "value" '
            'FROM "thetag"'
        ),
    }

    assert expected[read_type] == res


def test_genreadquery_long_sampletime():
    starttime = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = pd.Timedelta(86401, unit="s")

    res = AspenHandlerODBC.generate_read_query(
        "thetag", None, starttime, stoptime, ts, ReaderType.INT
    )

    expected = (
        'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
        "name = 'thetag' AND (period = 864010) AND (request = 7) "
        "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
        "ORDER BY ts"
    )

    assert expected == res
