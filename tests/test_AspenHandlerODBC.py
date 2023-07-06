from datetime import datetime, timedelta
from typing import Generator

import pytest
import pytz

from tagreader.utils import ReaderType, ensure_datetime_with_tz, is_windows

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)

from tagreader.odbc_handlers import AspenHandlerODBC

START_TIME = "2018-01-17 16:00:00"
STOP_TIME = "2018-01-17 17:00:00"
NONE_END_TIME = datetime(2100, 1, 1, 0, 0, tzinfo=pytz.UTC)
SAMPLE_TIME = 60


@pytest.fixture(scope="module")  # type: ignore[misc]
def aspen_handler() -> Generator[AspenHandlerODBC, None, None]:
    from tagreader.odbc_handlers import AspenHandlerODBC

    yield AspenHandlerODBC(host="thehostname", port=1234, options={"max_rows": 567890})


def test_generate_connection_string(aspen_handler: AspenHandlerODBC) -> None:
    res = aspen_handler.generate_connection_string()
    expected = (
        "DRIVER={AspenTech SQLPlus};HOST=thehostname;PORT=1234;"
        "READONLY=Y;MAXROWS=567890"
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
def test_generate_tag_read_query(read_type_str: str) -> None:
    read_type = getattr(ReaderType, read_type_str)
    start = ensure_datetime_with_tz(START_TIME)
    stoptime = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    if read_type == ReaderType.SNAPSHOT:
        res = AspenHandlerODBC.generate_read_query(
            tag="the_tag",
            mapdef=None,
            start=None,  # type: ignore[arg-type]
            end=None,  # type: ignore[arg-type]
            sample_time=None,
            read_type=read_type,
        )
    else:
        res = AspenHandlerODBC.generate_read_query(
            tag="the_tag",
            mapdef=None,
            start=start,
            end=stoptime,
            sample_time=ts,
            read_type=read_type,
        )

    expected = {
        "RAW": (
            'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
            "name = 'the_tag' AND (request = 4) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "INT": (
            'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 7) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MIN": (
            'SELECT ISO8601(ts_start) AS "time", min AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MAX": (
            'SELECT ISO8601(ts_start) AS "time", max AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "RNG": (
            'SELECT ISO8601(ts_start) AS "time", rng AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "AVG": (
            'SELECT ISO8601(ts_start) AS "time", avg AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "STD": (
            'SELECT ISO8601(ts_start) AS "time", std AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "VAR": (
            'SELECT ISO8601(ts_start) AS "time", var AS "value" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "SNAPSHOT": (
            'SELECT ISO8601(IP_INPUT_TIME) AS "time", IP_INPUT_VALUE AS "value" '
            'FROM "the_tag"'
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
        # pytest.param("SNAPSHOT", marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_generate_tag_read_query_with_status(read_type_str: str) -> None:
    read_type = getattr(ReaderType, read_type_str)
    start = ensure_datetime_with_tz(START_TIME)
    end = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    if read_type == ReaderType.SNAPSHOT:
        res = AspenHandlerODBC.generate_read_query(
            tag="the_tag",
            mapdef=None,
            start=None,  # type: ignore[arg-type]
            end=None,  # type: ignore[arg-type]
            sample_time=None,
            read_type=read_type,
            get_status=True,
        )
    else:
        res = AspenHandlerODBC.generate_read_query(
            tag="the_tag",
            mapdef=None,
            start=start,
            end=end,
            sample_time=ts,
            read_type=read_type,
            get_status=True,
        )

    expected = {
        "RAW": (
            'SELECT ISO8601(ts) AS "time", value AS "value" '
            ', status AS "status" FROM history WHERE '
            "name = 'the_tag' AND (request = 4) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "INT": (
            'SELECT ISO8601(ts) AS "time", value AS "value" '
            ', status AS "status" FROM history WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 7) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MIN": (
            'SELECT ISO8601(ts_start) AS "time", min AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "MAX": (
            'SELECT ISO8601(ts_start) AS "time", max AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "RNG": (
            'SELECT ISO8601(ts_start) AS "time", rng AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "AVG": (
            'SELECT ISO8601(ts_start) AS "time", avg AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "STD": (
            'SELECT ISO8601(ts_start) AS "time", std AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
        "VAR": (
            'SELECT ISO8601(ts_start) AS "time", var AS "value" '
            ', status AS "status" FROM aggregates WHERE '
            "name = 'the_tag' AND (period = 600) AND (request = 1) "
            "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
            "ORDER BY ts"
        ),
    }

    assert expected[read_type.name] == res


def test_generate_read_query_long_sample_time() -> None:
    start = ensure_datetime_with_tz(START_TIME)
    end = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=86401)

    res = AspenHandlerODBC.generate_read_query(
        tag="the_tag",
        mapdef=None,
        start=start,
        end=end,
        sample_time=ts,
        read_type=ReaderType.INT,
    )

    expected = (
        'SELECT ISO8601(ts) AS "time", value AS "value" FROM history WHERE '
        "name = 'the_tag' AND (period = 864010) AND (request = 7) "
        "AND (ts BETWEEN '2018-01-17T15:00:00Z' AND '2018-01-17T16:00:00Z') "
        "ORDER BY ts"
    )

    assert expected == res
