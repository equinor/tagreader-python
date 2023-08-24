import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from tagreader.cache import SmartCache
from tagreader.clients import IMSClient
from tagreader.utils import ReaderType, ensure_datetime_with_tz, is_windows

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if not is_windows():
    pytest.skip("All tests in module require Windows", allow_module_level=True)


if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

verifySSL = not is_AZUREPIPELINE  # Certificate unavailable there

PI_DS = "PIMAM"
PI_TAG = "SINUSOID"
PI_START_TIME = datetime(2020, 6, 29, 11, 0, 0)
PI_END_TIME = datetime(2020, 6, 29, 11, 15, 0)
PI_START_TIME_2 = datetime(2020, 6, 29, 11, 30, 0)
PI_END_TIME_2 = datetime(2020, 6, 29, 11, 45, 0)
TS = timedelta(seconds=60)

ASPEN_DS = "SNA"
ASPEN_TAG = "ATCAI"
ASPEN_START_TIME = PI_START_TIME
ASPEN_END_TIME = PI_END_TIME


@pytest.fixture  # type: ignore[misc]
def pi_client_odbc() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=PI_DS, imstype="pi")
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")


@pytest.fixture  # type: ignore[misc]
def pi_client_web() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=PI_DS, imstype="piwebapi", verifySSL=verifySSL)
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")


@pytest.fixture  # type: ignore[misc]
def aspen_client_odbc() -> Generator[IMSClient, None, None]:
    c = IMSClient(datasource=ASPEN_DS, imstype="ip21")
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")


@pytest.fixture  # type: ignore[misc]
def aspen_client_web() -> Generator[IMSClient, None, None]:
    c = IMSClient(
        datasource=ASPEN_DS,
        imstype="aspenone",
        verifySSL=bool(verifySSL),
    )
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")
    c.cache = None  # type: ignore[assignment]
    c.connect()
    yield c
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")


def test_pi_odbc_web_same_values_int(
    pi_client_odbc: IMSClient, pi_client_web: IMSClient
) -> None:
    df_odbc = pi_client_odbc.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    df_web = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    assert len(df_web) == len(df_odbc) == 16
    pd.testing.assert_frame_equal(
        df_odbc, df_web, rtol=0.001  # Slightly different results for PI
    )


def test_pi_odbc_web_same_values_aggregated(
    pi_client_odbc: IMSClient, pi_client_web: IMSClient
) -> None:
    df_odbc = pi_client_odbc.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    df_web = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    assert len(df_web) == len(df_odbc) == 15
    pd.testing.assert_frame_equal(
        df_odbc, df_web, rtol=0.001  # Slightly different results for PI
    )


def test_aspen_odbc_web_same_values_raw(
    aspen_client_odbc: IMSClient, aspen_client_web: IMSClient
) -> None:
    df_odbc = aspen_client_odbc.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        read_type=ReaderType.RAW,
    )
    df_web = aspen_client_web.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        read_type=ReaderType.RAW,
    )
    assert len(df_web) == len(df_odbc) == 178
    pd.testing.assert_frame_equal(df_odbc, df_web)


def test_aspen_odbc_web_same_values_int(
    aspen_client_odbc: IMSClient, aspen_client_web: IMSClient
) -> None:
    df_odbc = aspen_client_odbc.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    df_web = aspen_client_web.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    assert len(df_web) == len(df_odbc) == 16
    pd.testing.assert_frame_equal(df_odbc, df_web)


def test_aspen_odbc_web_same_values_aggregated(
    aspen_client_odbc: IMSClient, aspen_client_web: IMSClient
) -> None:
    df_odbc = aspen_client_odbc.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    df_web = aspen_client_web.read(
        tags=ASPEN_TAG,
        start_time=ASPEN_START_TIME,
        end_time=ASPEN_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    assert len(df_web) == len(df_odbc) == 15
    pd.testing.assert_frame_equal(df_odbc, df_web)


def test_concat_proper_fill_up(pi_client_web: IMSClient) -> None:
    max_rows_backup = pi_client_web.handler._max_rows
    df_int = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    assert len(df_int) == 16
    df_avg = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    assert len(df_avg) == 15

    pi_client_web.handler._max_rows = 5
    df_int_concat = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    assert len(df_int_concat) == 16
    df_avg_concat = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.AVG,
    )
    assert len(df_avg_concat) == 15
    pd.testing.assert_frame_equal(df_int, df_int_concat)
    pd.testing.assert_frame_equal(df_avg, df_avg_concat)
    pi_client_web.handler._max_rows = max_rows_backup


def test_cache_proper_fill_up(
    pi_client_web: IMSClient, tmp_path: Path, get_status: bool = False
) -> None:
    pi_client_web.cache = SmartCache(directory=tmp_path)
    df_int_1 = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME,
        end_time=PI_END_TIME,
        ts=TS,
        read_type=ReaderType.INT,
    )
    df_int_2 = pi_client_web.read(
        tags=PI_TAG,
        start_time=PI_START_TIME_2,
        end_time=PI_END_TIME_2,
        ts=TS,
        read_type=ReaderType.INT,
    )
    assert len(df_int_1) == 16
    assert len(df_int_2) == 16
    df_cached = pi_client_web.cache.fetch(  # type: ignore[call-arg]
        tagname=PI_TAG,
        read_type=ReaderType.INT,
        ts=TS,
        start=ensure_datetime_with_tz(PI_START_TIME),
        end=ensure_datetime_with_tz(PI_END_TIME_2),
        get_status=get_status,
    )
    assert len(df_cached) == 32
