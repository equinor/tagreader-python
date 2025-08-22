import os
from datetime import timedelta
from typing import Generator

import pytest

from tagreader.cache import SmartCache
from tagreader.clients import IMSClient, list_sources
from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.web_handlers import PIHandlerWeb, get_verify_ssl, list_piwebapi_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

verify_ssl = False if is_AZUREPIPELINE else get_verify_ssl()

SOURCE = "PIMAM"
TAGS = {
    "Float32": "CDT158",  # BA:CONC.1
    "Digital": "CDM158",  # BA:ACTIVE.1
    "Int32": "CDEP158",
}

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture  # type: ignore[misc]
def client() -> Generator[IMSClient, None, None]:
    c = IMSClient(
        datasource=SOURCE,
        imstype="piwebapi",
        verify_ssl=bool(verify_ssl),
    )
    c.cache = None
    c.connect()
    c.handler._max_rows = 1000  # For the long raw test
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


@pytest.fixture  # type: ignore[misc]
def pi_handler(cache: SmartCache) -> Generator[PIHandlerWeb, None, None]:
    h = PIHandlerWeb(
        datasource=SOURCE,
        verify_ssl=bool(verify_ssl),
        auth=None,
        options={},
        url=None,
        cache=cache,
    )
    if not isinstance(h.web_id_cache, SmartCache):
        raise ValueError("Expected SmartCache in the web client.")
    h.web_id_cache["alreadyknowntag"] = "knownwebid"
    yield h


def test_list_all_piwebapi_sources() -> None:
    res = list_piwebapi_sources(verify_ssl=bool(verify_ssl), auth=None, url=None)
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_list_sources_piwebapi() -> None:
    res = list_sources(imstype="piwebapi", verify_ssl=bool(verify_ssl))
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)


def test_verify_connection(pi_handler: IMSClient) -> None:
    assert pi_handler.verify_connection("PIMAM") is True  # type: ignore[attr-defined]
    assert pi_handler.verify_connection("somerandomstuffhere") is False  # type: ignore[attr-defined]


def test_search_tag(client: IMSClient) -> None:
    res = client.search("SINUSOID")
    assert 1 == len(res)
    res = client.search("SIN*")
    assert isinstance(res, list)
    assert 3 <= len(res)
    assert isinstance(res[0], tuple)
    [taglist, desclist] = zip(*res)
    assert "SINUSOIDU" in taglist
    assert desclist[taglist.index("SINUSOID")] == "12 Hour Sine Wave"
    res = client.search("SIN*", return_desc=False)
    assert 3 <= len(res)
    assert isinstance(res, list)
    assert isinstance(res[0], str)
    res = client.search(desc="12 Hour Sine Wave")
    assert 1 <= len(res)
    res = client.search(tag="SINUSOID", desc="*Sine*")
    assert 1 <= len(res)


def test_tag_to_web_id(pi_handler: PIHandlerWeb) -> None:
    res = pi_handler.tag_to_web_id("SINUSOID")
    assert isinstance(res, str)
    assert len(res) >= 20
    with pytest.raises(AssertionError):
        _ = pi_handler.tag_to_web_id("SINUSOID*")
    res = pi_handler.tag_to_web_id("somerandomgarbage")
    assert not res


@pytest.mark.parametrize(  # type: ignore[misc]
    ("read_type", "size"),
    [
        ("RAW", 10),
        # pytest.param(
        #      "SHAPEPRESERVING", 0, marks=pytest.mark.skip(reason="Not implemented")
        # ),
        ("INT", 61),
        ("MIN", 60),
        ("MAX", 60),
        ("RNG", 60),
        ("AVG", 60),
        ("VAR", 60),
        ("STD", 60),
        # pytest.param("COUNT", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", 0, marks=pytest.mark.skip(reason="Not implemented")),
        ("SNAPSHOT", 1),
    ],
)
def test_read(client: IMSClient, read_type: str, size: int) -> None:
    if read_type == "SNAPSHOT":
        df = client.read(
            tags=TAGS["Float32"],
            read_type=getattr(ReaderType, read_type),
            start_time=None,
            end_time=None,
        )
    else:
        df = client.read(
            tags=TAGS["Float32"],
            start_time=START_TIME,
            end_time=STOP_TIME,
            ts=SAMPLE_TIME,
            read_type=getattr(ReaderType, read_type),
        )

    if read_type not in ["SNAPSHOT", "RAW"]:
        assert df.shape == (size, 1)
        assert df.index[0] == ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] == df.index[0] + (size - 1) * timedelta(seconds=SAMPLE_TIME)
    elif read_type in "RAW":
        # Weirdness for test-tag which can have two different results,
        # apparently depending on the day of the week, mood, lunar cycle...
        assert df.shape == (size, 1) or df.shape == (size - 1, 1)
        assert df.index[0] >= ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] <= ensure_datetime_with_tz(STOP_TIME)


def test_read_with_status(client: IMSClient) -> None:
    df = client.read(
        tags=TAGS["Float32"],
        start_time=START_TIME,
        end_time=STOP_TIME,
        ts=SAMPLE_TIME,
        read_type=ReaderType.RAW,
        get_status=True,
    )
    assert df.shape == (10, 2)
    assert df[TAGS["Float32"] + "::status"].iloc[0] == 0


def test_read_raw_long(client: IMSClient) -> None:
    df = client.read(
        tags=TAGS["Float32"],
        start_time=START_TIME,
        end_time="2020-04-11 20:00:00",
        read_type=ReaderType.RAW,
    )
    assert len(df) > 1000


def test_read_only_invalid_data_yields_nan_for_invalid(client: IMSClient) -> None:
    tag = TAGS["Float32"]
    df = client.read(
        tags=tag,
        start_time="2012-10-09 10:30:00",
        end_time="2012-10-09 11:00:00",
        ts=600,
    )
    assert df.shape == (4, 1)
    assert df[tag].isna().all()


def test_read_invalid_data_mixed_with_valid_yields_nan_for_invalid(
    client: IMSClient,
) -> None:
    # Hint, found first valid datapoint for tag
    tag = TAGS["Float32"]
    df = client.read(
        tags=tag,
        start_time="2018-04-23 15:20:00",
        end_time="2018-04-23 15:50:00",
        ts=600,
    )
    assert df.shape == (4, 1)
    assert df[tag].iloc[[0, 1]].isna().all()  # type: ignore[call-overload]
    assert df[tag].iloc[[2, 3]].notnull().all()  # type: ignore[call-overload]


def test_digitalread_yields_integers(client: IMSClient) -> None:
    tag = TAGS["Digital"]
    df = client.read(
        tags=tag,
        start_time=START_TIME,
        end_time=STOP_TIME,
        ts=600,
        read_type=ReaderType.INT,
    )
    assert all(x.is_integer() for x in df[tag])


def test_get_unit(client: IMSClient) -> None:
    res = client.get_units(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "DEG. C"
    assert res[TAGS["Digital"]] == "STATE"
    assert res[TAGS["Int32"]] == ""


def test_get_description(client: IMSClient) -> None:
    res = client.get_descriptions(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "Atmospheric Tower OH Vapor"
    assert res[TAGS["Digital"]] == "Light Naphtha End Point Control"
    assert res[TAGS["Int32"]] == "Light Naphtha End Point"


def test_from_dst_folds_time(client: IMSClient) -> None:
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = client.read(tags=[tag], start_time=interval[0], end_time=interval[1], ts=600)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df[tag].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size == 5  # type: ignore[misc]
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1  # type: ignore[misc]
    )


def test_to_dst_skips_time(client: IMSClient) -> None:
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2018-03-25 00:30:00", "2018-03-25 03:30:00"]
    df = client.read(tags=[tag], start_time=interval[0], end_time=interval[1], ts=600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6  # type: ignore[misc]
    )


def test_tags_with_no_data_included_in_results(client: IMSClient) -> None:
    df = client.read(
        tags=[TAGS["Float32"]],
        start_time="2099-01-01 00:00:00",
        end_time="2099-01-02 00:00:00",
        ts=timedelta(seconds=60),
    )
    assert len(df.columns) == 1


def test_tags_raw_with_no_data_included_in_results(client: IMSClient) -> None:
    df = client.read(
        tags=[TAGS["Float32"]],
        start_time="2099-01-01 00:00:00",
        end_time="2099-01-02 00:00:00",
        read_type=ReaderType.RAW,
        ts=timedelta(seconds=60),
    )
    assert df.empty
