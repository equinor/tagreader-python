import os
import warnings
from importlib.util import find_spec
from typing import Dict, List, Literal, Optional, Tuple, Union, cast

import pandas as pd

from tagreader.utils import ReaderType


def safe_tagname(tagname: str) -> str:
    tagname = tagname.replace(".", "_")
    tagname = "".join(c for c in tagname if c.isalnum() or c == "_").strip()
    if tagname[0].isnumeric():
        tagname = "_" + tagname  # Conform to NaturalName
    return tagname


def timestamp_to_epoch(timestamp: pd.Timestamp) -> int:
    origin = pd.Timestamp("1970-01-01")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return (timestamp - origin) // pd.Timedelta("1s")


class BucketCache:
    def __init__(self, filename: str, path: str = ".") -> None:
        if not find_spec("tables"):
            warnings.warn("Package 'tables' not installed. Disabling cache.")
            return None
        self.filename = os.path.splitext(filename)[0] + ".h5"
        self.filename = os.path.join(path, self.filename)

    def _key_path(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Union[int, pd.Timedelta],
        stepped: bool,
        status: bool,
        starttime: Optional[pd.Timestamp] = None,
        endtime: Optional[pd.Timestamp] = None,
    ) -> str:
        """Return a string on the form
        /tagname/readtype[/sample_time][/stepped][/status]/_starttime_endtime
        tagname: safe tagname
        sample_time: integer value. Empty for RAW.
        stepped: "stepped" if value was read as stepped. Empty if not.
        status: "status" if value contains status. Empty if not.
        starttime: The starttime of the query that created the bucket.
        endtime: The endtime of the query that created the bucket.
        """
        tagname = safe_tagname(tagname)

        ts = (
            int(ts.total_seconds())
            if readtype != ReaderType.RAW and isinstance(ts, pd.Timedelta)
            else ts
        )
        timespan = ""
        if starttime is not None:
            starttime_epoch = timestamp_to_epoch(starttime)
            endtime_epoch = timestamp_to_epoch(endtime)  # type: ignore[arg-type]
            timespan = f"/_{starttime_epoch}_{endtime_epoch}"

        keyval = (
            f"/{tagname}"
            f"/{readtype.name}"
            f"{(ts is not None and readtype != ReaderType.RAW) * f'/s{ts}'}"
            f"{stepped * '/stepped'}"
            f"{status * '/status'}"
            f"{timespan}"
        )
        return keyval

    def store_tag_metadata(
        self, tagname: str, metadata: Dict[str, Union[str, int]]
    ) -> None:
        tagname = safe_tagname(tagname)
        key = f"/{tagname}"
        with pd.HDFStore(self.filename, mode="a") as f:
            if key not in f:
                f.put(key, pd.DataFrame())
            origmetadata = {}
            if "metadata" in f.get_storer(key).attrs:  # type: ignore[operator]
                origmetadata = f.get_storer(key).attrs.metadata  # type: ignore[operator]
            f.get_storer(key).attrs.metadata = {**origmetadata, **metadata}  # type: ignore[operator]

    def fetch_tag_metadata(
        self, tagname: str, properties: Union[str, List[str]]
    ) -> Dict[str, Union[str, int]]:
        res: Dict[str, Union[str, int]] = {}
        if not os.path.isfile(self.filename):
            return res
        tagname = safe_tagname(tagname)
        key = f"/{tagname}"
        if isinstance(properties, str):
            properties = [properties]
        with pd.HDFStore(self.filename, mode="r") as f:
            if key not in f or "metadata" not in f.get_storer(key).attrs:  # type: ignore[operator]
                return {}
            metadata = f.get_storer(key).attrs.metadata  # type: ignore[operator]
        for p in properties:
            if p in metadata.keys():
                res[p] = metadata.get(p)
        return res

    def remove(self, filename: Optional[str] = None) -> None:
        if not filename:
            filename = self.filename
        if os.path.isfile(filename):
            os.unlink(filename)

    def store(
        self,
        df: pd.DataFrame,
        tagname: str,
        readtype: ReaderType,
        ts: pd.Timedelta,
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ) -> None:
        if df.empty:
            return

        intersecting = self.get_intersecting_datasets(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )
        if len(intersecting) > 0:
            with pd.HDFStore(self.filename, mode="a") as f:
                for dataset in intersecting:
                    this_start, this_end = self._get_intervals_from_dataset_name(
                        dataset
                    )
                    starttime = min(starttime, this_start)
                    endtime = max(endtime, this_end)
                    df = pd.concat([df, f.get(dataset)])  # type: ignore[list-item]
                    del f[dataset]
            df = df[~df.index.duplicated(keep="first")].sort_index()
        key = self._key_path(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )
        with pd.HDFStore(self.filename, mode="a") as f:
            f.put(key, df, format="table")

    @staticmethod
    def _get_intervals_from_dataset_name(
        name: str,
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        name_with_times = name.split("/")[-1]
        if not name_with_times.count("_") == 2:
            return (None, None)  # type: ignore[return-value]
        _, starttime_epoch, endtime_epoch = name_with_times.split("_")
        starttime = pd.to_datetime(int(starttime_epoch), unit="s").tz_localize("UTC")
        endtime = pd.to_datetime(int(endtime_epoch), unit="s").tz_localize("UTC")
        return starttime, endtime

    def get_intersecting_datasets(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Union[int, pd.Timedelta],
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ) -> List[str]:
        if not os.path.isfile(self.filename):
            return []
        intersecting_datasets = []
        with pd.HDFStore(self.filename, mode="r") as f:
            for bucket in f.walk(
                where=self._key_path(
                    tagname=tagname,
                    readtype=readtype,
                    starttime=None,
                    endtime=None,
                    ts=ts,
                    stepped=stepped,
                    status=status,
                )
            ):
                for leaf in bucket[2]:
                    dataset = bucket[0] + "/" + leaf
                    starttime_ds, endtime_ds = self._get_intervals_from_dataset_name(
                        dataset
                    )
                    if endtime_ds >= starttime and endtime >= starttime_ds:
                        intersecting_datasets.append(dataset)
        return intersecting_datasets

    def get_missing_intervals(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Union[int, pd.Timedelta],
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        datasets = self.get_intersecting_datasets(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )
        missing_intervals = [(starttime, endtime)]
        for dataset in datasets:
            b = self._get_intervals_from_dataset_name(dataset)
            for _ in range(0, len(missing_intervals)):
                r = missing_intervals.pop(0)
                if b[1] < r[0] or b[0] > r[1]:
                    # No overlap
                    missing_intervals.append(r)
                elif b[0] <= r[0] and b[1] >= r[1]:
                    # The bucket covers the entire interval
                    continue
                elif b[0] > r[0] and b[1] < r[1]:
                    # The bucket splits the interval in two
                    missing_intervals.append((r[0], b[0]))
                    missing_intervals.append((b[1], r[1]))
                elif b[0] <= r[0] and r[0] <= b[1] < r[1]:
                    # The bucket chomps the start of the interval
                    missing_intervals.append((b[1], r[1]))
                elif r[0] < b[0] <= r[1] and b[1] >= r[1]:
                    # The bucket chomps the end of the interval
                    missing_intervals.append((r[0], b[0]))
        return missing_intervals

    def fetch(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: int,
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        if not os.path.isfile(self.filename):
            return df

        datasets = self.get_intersecting_datasets(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )

        with pd.HDFStore(self.filename, mode="r") as f:
            for dataset in datasets:
                # df = df.append(
                #     f.select(dataset, where="index >= starttime and index <= endtime")
                # )
                df = pd.concat(
                    [
                        df,
                        f.select(  # type: ignore[list-item]
                            dataset, where="index >= starttime and index <= endtime"
                        ),
                    ]
                )

        return df.sort_index()


class SmartCache:
    def __init__(self, filename: str, path: str = ".") -> None:
        if not find_spec("tables"):
            warnings.warn("Package 'tables' not installed. Disabling cache.")
            return None
        self.filename = os.path.splitext(filename)[0] + ".h5"
        self.filename = os.path.join(path, self.filename)
        # self.open(self.filename)

    # def open(self, filename=None):
    #     if filename is None:
    #         filename = self.filename
    #     self.hdfstore = pd.HDFStore(filename)
    #
    # def close(self):
    #     self.hdfstore.close()

    @staticmethod
    def key_path(
        df: Union[str, pd.DataFrame],
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timedelta]] = None,
    ) -> str:
        """Return a string on the form
        XXX/sYY/ZZZ where XXX is the ReadType, YY is the interval between samples
        (in seconds) and ZZZ is a safe tagname.
        """
        name = list(df)[0] if isinstance(df, pd.DataFrame) else df
        name = safe_tagname(name)  # type: ignore[arg-type]
        ts = int(ts.total_seconds()) if isinstance(ts, pd.Timedelta) else ts
        if readtype != ReaderType.RAW:
            if ts is None:
                # Determine sample time by reading interval between first two
                # samples of dataframe.
                if isinstance(df, pd.DataFrame):
                    interval = int(df[0:2].index.to_series().diff().mean().value / 1e9)  # type: ignore[attr-defined]
                else:
                    raise TypeError
            else:
                interval = int(ts)
            return f"{readtype.name}/s{interval}/{name}"
        else:
            return f"{readtype.name}/{name}"

    def store(
        self,
        df: pd.DataFrame,
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timedelta]] = None,
    ) -> None:
        key = self.key_path(df=df, readtype=readtype, ts=ts)
        if df.empty:
            return  # Weirdness ensues when using empty df in select statement below
        with pd.HDFStore(self.filename, mode="a") as f:
            if key in f:
                idx = f.select(  # noqa: F841
                    key, where="index in df.index", columns=["index"]
                ).index
                f.append(key, df.query("index not in @idx"))
            else:
                f.append(key, df)

    def fetch(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timestamp]] = None,
        start_time: Optional[pd.Timestamp] = None,
        stop_time: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        if not os.path.isfile(self.filename):
            return df
        key = self.key_path(df=tagname, readtype=readtype, ts=ts)  # type: ignore[arg-type]
        where = []
        if start_time is not None:
            where.append("index >= start_time")
        if stop_time is not None:
            where.append("index <= stop_time")
        wheretxt = " and ".join(where)
        with pd.HDFStore(self.filename, mode="r") as f:
            if key in f:
                if wheretxt:
                    df = f.select(key, where=wheretxt)  # type: ignore[assignment]
                else:
                    df = f.select(key)  # type: ignore[assignment]
        return df.sort_index()

    def store_tag_metadata(
        self, tagname: str, metadata: Dict[str, Union[str, int]]
    ) -> None:
        tagname = safe_tagname(tagname)
        key = f"/metadata/{tagname}"
        with pd.HDFStore(self.filename, mode="a") as f:
            if key not in f:
                f.put(key, pd.DataFrame())
            origmetadata = {}
            if "metadata" in f.get_storer(key).attrs:  # type: ignore[operator]
                origmetadata = f.get_storer(key).attrs.metadata  # type: ignore[operator]
            f.get_storer(key).attrs.metadata = {**origmetadata, **metadata}  # type: ignore[operator]

    def fetch_tag_metadata(
        self, tagname: str, properties: Union[str, List[str]]
    ) -> Dict[str, Union[str, int]]:
        res: Dict[str, Union[str, int]] = {}
        if not os.path.isfile(self.filename):
            return res
        tagname = safe_tagname(tagname)
        key = f"/metadata/{tagname}"
        if isinstance(properties, str):
            properties = [properties]
        with pd.HDFStore(self.filename, mode="r") as f:
            if key not in f or "metadata" not in f.get_storer(key).attrs:  # type: ignore[operator]
                return {}
            metadata = f.get_storer(key).attrs.metadata  # type: ignore[operator]
        for p in properties:
            if p in metadata.keys():
                res[p] = metadata.get(p)
        return res

    def remove(self, filename: Optional[str] = None) -> None:
        if not filename:
            filename = self.filename
            # self.close()
        if os.path.isfile(filename):
            os.unlink(filename)

    def _match_tag(
        self,
        key: str,
        readtype: Optional[Union[List[ReaderType], ReaderType]] = None,
        ts: Optional[List[Optional[Union[int, pd.Timestamp]]]] = None,
        tagname: Optional[Union[List[str], str]] = None,
    ) -> bool:
        def readtype_to_str(rt):  # type: ignore
            return getattr(
                rt, "name", rt
            )  # if isinstance(rt, ReaderType) always returns False...?

        def timedelta_to_str(t):  # type: ignore[no-untyped-def]
            if isinstance(t, pd.Timedelta):
                return str(int(t.total_seconds()))
            return t

        key = "/" + key.lstrip("/")  # Ensure absolute path:
        readtype = readtype if isinstance(readtype, list) else [readtype]  # type: ignore[list-item]
        ts = ts if isinstance(ts, list) else [ts]
        tagname = tagname if isinstance(tagname, list) else [tagname]  # type: ignore[list-item]
        readtype = list(map(readtype_to_str, readtype))
        ts = list(map(timedelta_to_str, ts))
        if tagname[0] is not None:
            tagname = list(map(safe_tagname, tagname))
        # print(f"Readtype: {readtype}, ts: {ts}, tagname: {tagname}")
        elements = key.split("/")[1:]
        if len(elements) == 2:
            elements.insert(1, None)  # type: ignore[arg-type]
        else:
            elements[1] = int(  # type: ignore[call-overload]
                elements[1][1:]
            )  # Discard the initial s
        if elements[0] not in readtype and readtype[0] is not None:  # type: ignore[comparison-overlap]
            # print(f"{elements[0]} not in {readtype}")
            return False
        if elements[1] not in ts and ts[0] is not None:
            # print(f"{elements[1]} not in {ts}")
            return False
        if elements[2] not in tagname and tagname[0] is not None:
            # print(f"{elements[2]} not in {tagname}")
            return False
        return True

    def delete_key(
        self,
        tagname: Optional[str] = None,
        readtype: Optional[ReaderType] = None,
        ts: Optional[Union[int, pd.Timestamp]] = None,
    ) -> None:
        with pd.HDFStore(self.filename) as f:
            for key in f:
                if self._match_tag(key=key, tagname=tagname, readtype=readtype, ts=ts):  # type: ignore[arg-type]
                    f.remove(key)  # type: ignore[operator]

    def _get_hdfstore(self, mode: Literal["a", "w", "r", "r+"] = "r") -> pd.HDFStore:
        f = pd.HDFStore(self.filename, mode=mode)
        return f
