from datetime import timedelta
from typing import Any, Dict, Generator, List, Optional, Union

import pytest

import tagreader.web_handlers as web_handlers
from tagreader.cache import SmartCache
from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.web_handlers import (
    PIHandlerWeb,
    _get_piwebapi_source_map,
    _piwebapi_sources_cache,
    list_piwebapi_sources,
)

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture  # type: ignore[misc]
def pi_handler(cache: SmartCache) -> Generator[PIHandlerWeb, None, None]:
    h = PIHandlerWeb(
        datasource="sourcename",
        url=None,
        auth=None,
        verify_ssl=True,
        options={},
        cache=cache,
    )
    if not isinstance(h.web_id_cache, SmartCache):
        raise ValueError("Expected SmartCache in the web client.")
    h.web_id_cache.add(key="alreadyknowntag", value="knownwebid")
    yield h


def test_escape_chars() -> None:
    assert (
        PIHandlerWeb.escape('+-&|(){}[]^"~*:\\') == r"\+\-\&\|\(\)\{\}\[\]\^\"\~*\:\\"
    )


def test_generate_search_query() -> None:
    handler = PIHandlerWeb(
        datasource=None,
        url=None,
        auth=object(),
        verify_ssl=True,
        options={},
        cache=None,
    )
    assert handler.generate_search_params(
        tag="SINUSOID", desc=None, datasource=None
    ) == {"query": "name:SINUSOID"}
    assert handler.generate_search_params(
        tag=r"BA:*.1", desc=None, datasource=None
    ) == {
        "query": r"name:BA\:*.1",
    }
    assert handler.generate_search_params(tag="BA:*.1", datasource=None, desc=None) == {
        "query": r"name:BA\:*.1",
    }
    assert handler.generate_search_params(
        desc="Concentration Reactor 1", datasource=None, tag=None
    ) == {
        "query": r"description:Concentration\ Reactor\ 1",
    }
    assert handler.generate_search_params(
        tag="BA:*.1", desc="Concentration Reactor 1", datasource=None
    ) == {"query": r"name:BA\:*.1 AND description:Concentration\ Reactor\ 1"}


def test_verify_connection_uses_configured_piwebapi_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth = object()
    list_calls: List[Dict[str, Any]] = []
    handler = PIHandlerWeb(
        datasource="sourcename",
        url="https://example.test/piwebapi",
        auth=auth,
        verify_ssl=True,
        options={},
        cache=None,
    )

    def fake_list_sources(
        url: Optional[str] = None,
        auth: Optional[Any] = None,
        verify_ssl: Optional[Union[bool, str]] = True,
    ) -> List[str]:
        list_calls.append({"url": url, "auth": auth, "verify_ssl": verify_ssl})
        return ["source-a", "source-b"]

    monkeypatch.setattr(web_handlers, "list_piwebapi_sources", fake_list_sources)

    assert handler.verify_connection("source-b")
    assert not handler.verify_connection("unknown-source")
    assert (
        list_calls
        == [{"url": "https://example.test/piwebapi", "auth": auth, "verify_ssl": True}]
        * 2
    )


def test_verify_connection_uses_default_auth_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    list_calls: List[Dict[str, Any]] = []

    def fake_list_sources(
        url: Optional[str] = None,
        auth: Optional[Any] = None,
        verify_ssl: Optional[Union[bool, str]] = True,
    ) -> List[str]:
        list_calls.append({"url": url, "auth": auth, "verify_ssl": verify_ssl})
        return ["source-a"]

    monkeypatch.setattr(web_handlers, "list_piwebapi_sources", fake_list_sources)
    handler = PIHandlerWeb(
        datasource="sourcename",
        url="https://example.test/piwebapi",
        auth=None,
        verify_ssl=True,
        options={},
        cache=None,
    )

    assert handler.verify_connection("source-a")
    assert list_calls == [
        {"url": "https://example.test/piwebapi", "auth": None, "verify_ssl": True}
    ]


def test_piwebapi_sources_and_webids_share_cached_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = "https://example.test/piwebapi"
    requested_urls: List[str] = []

    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> Dict[str, List[Dict[str, str]]]:
            return {"Items": [{"Name": "source-a", "WebId": "webid-a"}]}

    def fake_get(
        request_url: str,
        auth: Optional[Any] = None,
        verify: Optional[Union[bool, str]] = None,
        timeout: Optional[Any] = None,
    ) -> Response:
        requested_urls.append(request_url)
        return Response()

    monkeypatch.setattr(web_handlers, "get_auth_pi", lambda: None)
    monkeypatch.setattr(web_handlers.requests, "get", fake_get)
    _piwebapi_sources_cache.pop(url, None)
    handler = PIHandlerWeb(
        datasource="source-a",
        url=url,
        auth=None,
        verify_ssl=True,
        options={},
        cache=None,
    )
    try:
        assert list_piwebapi_sources(url=url) == ["source-a"]
        assert _get_piwebapi_source_map(
            url=handler.base_url,
            auth=None if handler._uses_default_auth else handler.auth,
            verify_ssl=handler.session.verify,
        ) == {"source-a": "webid-a"}
    finally:
        _piwebapi_sources_cache.pop(url, None)

    assert requested_urls == ["https://example.test/piwebapi/dataservers"]


def test_is_summary(pi_handler: PIHandlerWeb) -> None:
    assert pi_handler._is_summary(ReaderType.AVG)
    assert pi_handler._is_summary(ReaderType.MIN)
    assert pi_handler._is_summary(ReaderType.MAX)
    assert pi_handler._is_summary(ReaderType.RNG)
    assert pi_handler._is_summary(ReaderType.STD)
    assert pi_handler._is_summary(ReaderType.VAR)
    assert not pi_handler._is_summary(ReaderType.RAW)
    assert not pi_handler._is_summary(ReaderType.SHAPEPRESERVING)
    assert not pi_handler._is_summary(ReaderType.INT)
    assert not pi_handler._is_summary(ReaderType.GOOD)
    assert not pi_handler._is_summary(ReaderType.BAD)
    assert not pi_handler._is_summary(ReaderType.SNAPSHOT)


@pytest.mark.parametrize(  # type: ignore[misc]
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
def test_generate_read_query(pi_handler: PIHandlerWeb, read_type: str) -> None:
    if not isinstance(pi_handler.web_id_cache, SmartCache):
        raise ValueError("Expected SmartCache in the fixture.")
    start = ensure_datetime_with_tz(START_TIME)
    stop = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    tag_no = pi_handler.tag_to_web_id("alreadyknowntag")
    if tag_no is None:
        raise ValueError("Expected a valid web ID for the tag.")

    url, params = pi_handler.generate_read_query(
        tag=tag_no,
        start=start,
        end=stop,
        sample_time=ts,
        read_type=getattr(ReaderType, read_type),
        metadata=None,
    )
    if read_type != "SNAPSHOT":
        assert params["startTime"] == "01-Apr-20 09:05:00"
        assert params["endTime"] == "01-Apr-20 10:05:00"
        assert params["timeZone"] == "UTC"

    if read_type == "INT":
        assert (
            url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/interpolated"
        )
        assert params["selectedFields"] == "Links;Items.Timestamp;Items.Value"
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/summary"
        assert (
            params["selectedFields"] == "Links;Items.Value.Timestamp;Items.Value.Value"
        )
        assert {
            "AVG": "Average",
            "MIN": "Minimum",
            "MAX": "Maximum",
            "RNG": "Range",
            "STD": "StdDev",
            "VAR": "StdDev",
        }.get(read_type) == params["summaryType"]
        assert params["summaryDuration"] == f"{SAMPLE_TIME}s"
    elif read_type == "SNAPSHOT":
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/value"
        assert params["selectedFields"] == "Timestamp;Value"
        assert len(params) == 3
    elif read_type == "RAW":
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/recorded"
        assert params["selectedFields"] == "Links;Items.Timestamp;Items.Value"
        assert params["maxCount"] == 10000


@pytest.mark.parametrize(  # type: ignore[misc]
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
def test_generate_read_query_with_status(
    pi_handler: PIHandlerWeb, read_type: str
) -> None:
    if not isinstance(pi_handler.web_id_cache, SmartCache):
        raise ValueError("Expected SmartCache in the fixture.")
    start = ensure_datetime_with_tz(START_TIME)
    stop = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=SAMPLE_TIME)

    tag_no = pi_handler.tag_to_web_id("alreadyknowntag")
    if tag_no is None:
        raise ValueError("Expected a valid web ID for the tag.")

    url, params = pi_handler.generate_read_query(
        tag=tag_no,
        start=start,
        end=stop,
        sample_time=ts,
        read_type=getattr(ReaderType, read_type),
        get_status=True,
        metadata=None,
    )
    if read_type != "SNAPSHOT":
        assert params["startTime"] == "01-Apr-20 09:05:00"
        assert params["endTime"] == "01-Apr-20 10:05:00"
        assert params["timeZone"] == "UTC"

    if read_type == "INT":
        assert (
            url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/interpolated"
        )
        assert params["selectedFields"] == (
            "Links;Items.Timestamp;Items.Value;"
            "Items.Good;Items.Questionable;Items.Substituted"
        )
        assert params["interval"] == f"{SAMPLE_TIME}s"
    elif read_type in ["AVG", "MIN", "MAX", "RNG", "STD", "VAR"]:
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/summary"
        assert params["selectedFields"] == (
            "Links;Items.Value.Timestamp;Items.Value.Value;"
            "Items.Value.Good;Items.Value.Questionable;Items.Value.Substituted"
        )
        assert {
            "AVG": "Average",
            "MIN": "Minimum",
            "MAX": "Maximum",
            "RNG": "Range",
            "STD": "StdDev",
            "VAR": "StdDev",
        }.get(read_type) == params["summaryType"]
        assert params["summaryDuration"] == f"{SAMPLE_TIME}s"
    elif read_type == "SNAPSHOT":
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/value"
        assert (
            params["selectedFields"] == "Timestamp;Value;Good;Questionable;Substituted"
        )
        assert len(params) == 3
    elif read_type == "RAW":
        assert url == f"streams/{pi_handler.web_id_cache['alreadyknowntag']}/recorded"
        assert params["selectedFields"] == (
            "Links;Items.Timestamp;Items.Value;"
            "Items.Good;Items.Questionable;Items.Substituted"
        )
        assert params["maxCount"] == 10000


def test_generate_read_query_long_sample_time(pi_handler: PIHandlerWeb) -> None:
    start = ensure_datetime_with_tz(START_TIME)
    stop = ensure_datetime_with_tz(STOP_TIME)
    ts = timedelta(seconds=86410)

    tag_no = pi_handler.tag_to_web_id("alreadyknowntag")
    if tag_no is None:
        raise ValueError("Expected a valid web ID for the tag.")

    url, params = pi_handler.generate_read_query(
        tag=tag_no,
        start=start,
        end=stop,
        sample_time=ts,
        read_type=ReaderType.INT,
        metadata=None,
    )
    assert params["interval"] == f"{86410}s"
