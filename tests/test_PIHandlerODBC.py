from datetime import datetime, timedelta
from typing import Generator

import pytest
import pytz

from tagreader import utils
from tagreader.utils import ReaderType, is_windows

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)
else:
    from tagreader.odbc_handlers import PIHandlerODBC

START_TIME = pytz.timezone("Europe/Oslo").localize(
    datetime(2018, 1, 17, hour=16, minute=0, second=0)
)
STOP_TIME = pytz.timezone("Europe/Oslo").localize(
    datetime(2018, 1, 17, hour=17, minute=0, second=0)
)
NONE_END_TIME = datetime(2100, 1, 1, 0, 0, tzinfo=pytz.UTC)
SAMPLE_TIME = timedelta(seconds=60)


@pytest.fixture(scope="module")  # type: ignore[misc]
def pi_handler() -> Generator[PIHandlerODBC, None, None]:
    yield PIHandlerODBC(
        host="thehostname.statoil.net",
        port=1234,
        options={"das_server": "the_das_server"},
    )


def test_generate_connection_string(pi_handler: PIHandlerODBC) -> None:
    res = pi_handler.generate_connection_string()
    expected = (
        "DRIVER={PI ODBC Driver};Server=the_das_server;Trusted_Connection=Yes;"
        "Command Timeout=1800;Provider Type=PIOLEDB;"
        "Provider String={Data source=thehostname;Integrated_Security=SSPI;"
        "Time Zone=UTC};"
    )
    assert expected == res


@pytest.mark.parametrize(  # type: ignore[misc]
    "read_type_str",
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
def test_generate_tag_read_query(pi_handler: PIHandlerODBC, read_type_str: str) -> None:
    read_type = getattr(ReaderType, read_type_str)
    start = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = SAMPLE_TIME

    if read_type == ReaderType.SNAPSHOT:
        res = pi_handler.generate_read_query(
            tag="thetag",
            start=None,  # type: ignore[arg-type]
            end=None,  # type: ignore[arg-type]
            sample_time=None,
            read_type=read_type,
            metadata={},
        )
    else:
        res = pi_handler.generate_read_query(
            tag="thetag",
            start=start,
            end=stoptime,
            sample_time=ts,
            read_type=read_type,
            metadata={},
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
    assert expected[read_type.name] == res


@pytest.mark.parametrize(  # type: ignore[misc]
    "read_type_str",
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
def test_generate_tag_read_query_with_status(
    pi_handler: PIHandlerODBC, read_type_str: str
) -> None:
    read_type = getattr(ReaderType, read_type_str)
    start = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = SAMPLE_TIME

    if read_type == read_type.SNAPSHOT:
        res = pi_handler.generate_read_query(
            tag="thetag",
            start=None,  # type: ignore[arg-type]
            end=None,  # type: ignore[arg-type]
            sample_time=None,
            read_type=read_type,
            get_status=True,
            metadata={},
        )
    else:
        res = pi_handler.generate_read_query(
            tag="thetag",
            start=start,
            end=stoptime,
            sample_time=ts,
            read_type=read_type,
            get_status=True,
            metadata={},
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
    assert expected[read_type.name] == res


def test_genreadquery_long_sampletime(pi_handler: PIHandlerODBC) -> None:
    start = utils.ensure_datetime_with_tz(START_TIME)
    stoptime = utils.ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=86401)

    res = pi_handler.generate_read_query(
        tag="thetag",
        start=start,
        end=stoptime,
        sample_time=ts,
        read_type=ReaderType.INT,
        metadata={},
    )

    expected = (
        "SELECT CAST(value as FLOAT32) AS value, time "
        "FROM [piarchive]..[piinterp2] WHERE tag='thetag' "
        "AND (time BETWEEN '17-Jan-18 15:00:00' AND '17-Jan-18 16:00:00') "
        "AND (timestep = '86401s') ORDER BY time"
    )

    assert expected == res
