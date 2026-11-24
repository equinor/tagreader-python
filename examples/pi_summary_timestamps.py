"""Compare PI Web API timestamps with tagreader's results.

Example:
    uv run python examples/pi_summary_timestamps.py --datasource PIMAM --tag CDT158 \
        --start 2020-04-01T09:05:00Z --end 2020-04-01T10:05:00Z

The script uses the configured Kerberos credentials and certificate verification.
It prints timestamps, not process values. Use a numeric tag with historical data.
"""

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Union

import pandas as pd
import requests

from tagreader.utils import ReaderType, urljoin
from tagreader.web_handlers import PIHandlerWeb

SUMMARY_TYPES = (
    ReaderType.MIN,
    ReaderType.MAX,
    ReaderType.AVG,
    ReaderType.RNG,
    ReaderType.STD,
    ReaderType.VAR,
)


class BoundedSession(requests.Session):
    def send(
        self, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = (10, 30)
        elif isinstance(timeout, tuple):
            kwargs["timeout"] = (timeout[0] or 10, timeout[1] or 30)
        return super().send(request, **kwargs)


def fetch(
    session: requests.Session, url: str, params: Mapping[str, Union[str, int]]
) -> Dict[str, Any]:
    response = session.get(url, params=params, timeout=(10, 30))
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object from PI Web API.")
    return data


def parse_time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "Specify a timezone, e.g. 2020-01-01T00:00:00Z"
        )
    return result.astimezone(timezone.utc)


def timestamps(data: Dict[str, Any], read_type: ReaderType) -> pd.DatetimeIndex:
    if data.get("Links", {}).get("Next"):
        raise ValueError("Response is paginated; shorten the requested window.")
    if read_type == ReaderType.SNAPSHOT:
        items = [data]
    else:
        items = data["Items"]
    field_values = (
        [item["Value"]["Timestamp"] for item in items]
        if read_type in SUMMARY_TYPES
        else [item["Timestamp"] for item in items]
    )
    return pd.DatetimeIndex(pd.to_datetime(field_values, utc=True))


def show_timestamps(
    label: str,
    values: pd.DatetimeIndex,
    start: datetime,
    end: datetime,
    interval: timedelta,
    read_type: ReaderType,
) -> None:
    if values.empty:
        print(f"  {label}: no data (inconclusive)")
        return
    print(f"  {label}: {len(values)} timestamp(s)")
    for value in values:
        offset = (value.to_pydatetime() - start).total_seconds()
        print(f"    {value.isoformat()}  offset from start={offset:g}s")
    if read_type in SUMMARY_TYPES:
        starts = pd.date_range(start=start, end=end, freq=interval, inclusive="left")
        ends = starts + interval
        print(
            f"    complete start grid={values.equals(starts)}; "
            f"complete end grid={values.equals(ends)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasource", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--url", help="PI Web API base URL (default: configured URL)")
    parser.add_argument("--start", required=True, type=parse_time)
    parser.add_argument("--end", required=True, type=parse_time)
    parser.add_argument(
        "--intervals", type=int, nargs="+", default=[60, 300], metavar="SECONDS"
    )
    args = parser.parse_args()
    if args.end <= args.start:
        parser.error("--end must be later than --start")
    for seconds in args.intervals:
        if seconds <= 0 or (args.end - args.start).total_seconds() % seconds:
            parser.error("Intervals must be positive and divide the window exactly")
        if (args.end - args.start).total_seconds() / seconds > 100:
            parser.error("Use a window of at most 100 intervals per request")

    handler = PIHandlerWeb(
        datasource=args.datasource,
        url=args.url,
        auth=None,
        verify_ssl=None,
        options={},
        cache=None,
    )
    session = BoundedSession()
    session.auth = handler.session.auth
    session.verify = handler.session.verify
    handler.session.close()
    handler.session = session
    web_id = handler.tag_to_web_id(args.tag)
    if not web_id:
        raise ValueError(f"Tag not found: {args.tag}")

    failures: List[str] = []
    for seconds in args.intervals:
        interval = timedelta(seconds=seconds)
        print(
            f"\nWindow: {args.start.isoformat()} to {args.end.isoformat()}, {seconds}s"
        )
        for read_type in SUMMARY_TYPES + (
            ReaderType.INT,
            ReaderType.RAW,
            ReaderType.SNAPSHOT,
        ):
            path, params = handler.generate_read_query(
                tag=web_id,
                start=args.start,
                end=args.end,
                sample_time=interval,
                read_type=read_type,
                metadata=None,
            )
            url = urljoin(handler.base_url, path)
            print(f"\n{read_type.name}")
            modes = (
                (None, "EarliestTime", "MostRecentTime")
                if read_type in SUMMARY_TYPES
                else (None,)
            )
            default_times: Optional[pd.DatetimeIndex] = None
            for mode in modes:
                request_params = dict(params)
                if mode:
                    request_params["timeType"] = mode
                label = f"server timeType={mode or 'omitted (Auto default)'}"
                try:
                    data = fetch(session, url, request_params)
                except requests.HTTPError as error:
                    status = (
                        error.response.status_code
                        if error.response is not None
                        else "unknown"
                    )
                    failures.append(
                        f"{read_type.name} {mode or 'default'}: HTTP {status}"
                    )
                    print(f"  {label}: HTTP {status}")
                    continue
                server_times = timestamps(data, read_type)
                if mode is None:
                    default_times = server_times
                show_timestamps(
                    label, server_times, args.start, args.end, interval, read_type
                )

            frame = handler.read_tag(
                tag=args.tag,
                start=args.start,
                end=args.end,
                sample_time=interval,
                read_type=read_type,
                metadata=None,
            )
            output_times = pd.DatetimeIndex(frame.index)
            show_timestamps(
                "tagreader output",
                output_times,
                args.start,
                args.end,
                interval,
                read_type,
            )
            if (
                default_times is not None
                and not default_times.empty
                and not output_times.empty
            ):
                print(
                    f"  tagreader matches default server timestamps={output_times.equals(default_times)}"
                )
            else:
                print("  comparison with default server timestamps: inconclusive")
    if failures:
        raise SystemExit("Failed requests: " + "; ".join(failures))


if __name__ == "__main__":
    main()
