import runpy
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from tagreader.utils import ReaderType

DEMO = Path(__file__).resolve().parents[1] / "examples" / "pi_summary_timestamps.py"


def test_demo_compares_raw_modes_and_handler_output(
    monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    main = runpy.run_path(str(DEMO))["main"]
    requests: List[Dict[str, Any]] = []

    class FakeHandler:
        base_url = "https://example.test"

        def __init__(self, **kwargs: Any) -> None:
            self.session = demo_session

        def tag_to_web_id(self, tag: str) -> str:
            return "web-id"

        def generate_read_query(self, **kwargs: Any) -> Any:
            return kwargs["read_type"].name, {}

        def read_tag(self, **kwargs: Any) -> pd.DataFrame:
            return pd.DataFrame(index=pd.DatetimeIndex(["2020-01-01T00:00:00Z"]))

    class FakeSession:
        auth: Any = None
        verify = True

        def close(self) -> None:
            pass

    demo_session = FakeSession()

    def fake_fetch(session: Any, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        requests.append({"url": url, **params})
        times = ["2020-01-01T00:00:00Z", "2020-01-01T00:01:00Z"]
        if params.get("timeType") == "MostRecentTime":
            times = ["2020-01-01T00:01:00Z", "2020-01-01T00:02:00Z"]
        elif url.endswith("/MAX") and "timeType" not in params:
            times = ["2020-01-01T00:00:30Z", "2020-01-01T00:01:30Z"]
        if url.endswith("/SNAPSHOT"):
            return {"Timestamp": times[-1]}
        if url.endswith(("/INT", "/RAW")):
            return {"Items": [{"Timestamp": t} for t in times]}
        return {"Items": [{"Value": {"Timestamp": t}} for t in times]}

    monkeypatch.setitem(main.__globals__, "PIHandlerWeb", FakeHandler)
    monkeypatch.setitem(main.__globals__, "BoundedSession", FakeSession)
    monkeypatch.setitem(main.__globals__, "fetch", fake_fetch)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pi_summary_timestamps.py",
            "--datasource",
            "source",
            "--tag",
            "tag",
            "--start",
            "2020-01-01T00:00:00Z",
            "--end",
            "2020-01-01T00:02:00Z",
            "--intervals",
            "60",
        ],
    )
    main()
    output = capsys.readouterr().out

    assert len(requests) == 21
    assert sum(request.get("timeType") == "EarliestTime" for request in requests) == 6
    assert sum(request.get("timeType") == "MostRecentTime" for request in requests) == 6
    assert "complete start grid=True; complete end grid=False" in output
    assert "complete start grid=False; complete end grid=True" in output
    assert "complete start grid=False; complete end grid=False" in output
    assert "tagreader matches default server timestamps=False" in output


def test_demo_detects_incomplete_or_paginated_data(capsys: Any) -> None:
    demo = runpy.run_path(str(DEMO))
    start = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    values = demo["timestamps"]({"Items": []}, ReaderType.MAX)
    demo["show_timestamps"](
        "empty", values, start, start, pd.Timedelta(seconds=60), ReaderType.MAX
    )
    assert "no data (inconclusive)" in capsys.readouterr().out
    with pytest.raises(ValueError, match="paginated"):
        demo["timestamps"]({"Links": {"Next": "next"}, "Items": []}, ReaderType.MAX)
