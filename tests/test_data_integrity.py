import os
import pytest
import pandas as pd
from tagreader import IMSClient
from tagreader.utils import ReaderType, datestr_to_datetime
from tagreader.cache import SmartCache

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

verifySSL = not is_AZUREPIPELINE  # Certificate unavailable there

PI_DS = "PIMAM"
PI_TAG = "SINUSOID"
PI_START_TIME = "2020-06-29 11:00:00"
PI_END_TIME = "2020-06-29 11:15:00"
PI_START_TIME_2 = "2020-06-29 11:30:00"
PI_END_TIME_2 = "2020-06-29 11:45:00"
TS = 60

ASPEN_URL = r"https://aspenone-qa.equinor.com/ProcessData/AtProcessDataREST.dll"
ASPEN_DS = "SNA"
ASPEN_TAG = "ATCAI"


@pytest.fixture()
def PIClientOdbc():
    c = IMSClient(PI_DS, "pi")
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")


@pytest.fixture()
def PIClientWeb():
    c = IMSClient(PI_DS, "piweb", verifySSL=verifySSL)
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")


@pytest.fixture()
def AspenClientOdbc():
    c = IMSClient(ASPEN_DS, "ip21")
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")


@pytest.fixture()
def AspenClientWeb():
    c = IMSClient(ASPEN_DS, "ip21web", url=ASPEN_URL, verifySSL=False)
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(ASPEN_DS + ".h5"):
        os.remove(ASPEN_DS + ".h5")


def test_pi_odbc_web_same_values_int(PIClientOdbc, PIClientWeb):
    df_odbc = PIClientOdbc.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    df_web = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    print(df_odbc.head())
    print(df_web.head())
    assert len(df_odbc) == 16
    assert len(df_web) == 16
    pd.testing.assert_frame_equal(
        df_odbc, df_web, check_less_precise=1  # Slightly different results for PI
    )


def test_pi_odbc_web_same_values_agg(PIClientOdbc, PIClientWeb):
    df_odbc = PIClientOdbc.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    df_web = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    print(df_odbc.head())
    print(df_web.head())
    assert len(df_odbc) == 15
    assert len(df_web) == 15
    pd.testing.assert_frame_equal(
        df_odbc, df_web, check_less_precise=1  # Slightly different results for PI
    )


def test_aspen_odbc_web_same_values_int(AspenClientOdbc, AspenClientWeb):
    df_odbc = AspenClientOdbc.read_tags(
        ASPEN_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    df_web = AspenClientWeb.read_tags(
        ASPEN_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    print(df_odbc.head())
    print(df_web.head())
    assert len(df_odbc) == 16
    assert len(df_web) == 16
    pd.testing.assert_frame_equal(df_odbc, df_web)


def test_aspen_odbc_web_same_values_agg(AspenClientOdbc, AspenClientWeb):
    df_odbc = AspenClientOdbc.read_tags(
        ASPEN_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    df_web = AspenClientWeb.read_tags(
        ASPEN_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    print(df_odbc.head())
    print(df_web.head())
    assert len(df_odbc) == 15
    assert len(df_web) == 15
    pd.testing.assert_frame_equal(df_odbc, df_web)


def test_concat_proper_fill_up(PIClientWeb):
    max_rows_backup = PIClientWeb.handler._max_rows
    df_int = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    assert len(df_int) == 16
    df_avg = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    assert len(df_avg) == 15

    PIClientWeb.handler._max_rows = 5
    df_int_concat = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    assert len(df_int_concat) == 16
    df_avg_concat = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.AVG
    )
    assert len(df_avg_concat) == 15
    pd.testing.assert_frame_equal(df_int, df_int_concat)
    pd.testing.assert_frame_equal(df_avg, df_avg_concat)
    PIClientWeb.handler._max_rows = max_rows_backup


def test_cache_proper_fill_up(PIClientWeb):
    PIClientWeb.cache = SmartCache(PI_DS)
    if os.path.exists(PI_DS + ".h5"):
        os.remove(PI_DS + ".h5")
    df_int_1 = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME, PI_END_TIME, TS, read_type=ReaderType.INT
    )
    df_int_2 = PIClientWeb.read_tags(
        PI_TAG, PI_START_TIME_2, PI_END_TIME_2, TS, read_type=ReaderType.INT
    )
    assert len(df_int_1) == 16
    assert len(df_int_2) == 16
    df_cached = PIClientWeb.cache.fetch(
        PI_TAG,
        ReaderType.INT,
        TS,
        datestr_to_datetime(PI_START_TIME),
        datestr_to_datetime(PI_END_TIME_2),
    )
    assert len(df_cached) == 32
