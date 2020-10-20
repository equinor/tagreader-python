import os
import pandas as pd
from .utils import ReaderType

from typing import Tuple, Union, List


def safe_tagname(tagname):
    tagname = tagname.replace(".", "_")
    tagname = "".join(c for c in tagname if c.isalnum() or c == "_").strip()
    if tagname[0].isnumeric():
        tagname = "_" + tagname  # Conform to NaturalName
    return tagname


def timestamp_to_epoch(timestamp: pd.Timestamp) -> int:
    origin = pd.Timestamp("1970-01-01")
    if timestamp.tzinfo is not None:
        origin = origin.tz_localize("UTC")
    return (timestamp - origin) // pd.Timedelta("1s")


class BucketCache:
    def __init__(self, filename, path="."):
        self.filename = os.path.splitext(filename)[0] + ".h5"
        self.filename = os.path.join(path, self.filename)

    def _key_path(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Union[int, pd.Timedelta],
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp = None,
        endtime: pd.Timestamp = None,
    ) -> str:
        """Return a string on the form
        /tagname/readtype[/sample_time][/stepped][/status]/_starttime_endtime
        tagname: safe tagname
        sample_time: integer value. 0 for RAW.
        stepped: "stepped" or "cont" whether tag was read as stepped or not
        status: "status" or "nostatus" indicates whether status value is included
        starttime: start of bucket
        endtime: end of bucket
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
            endtime_epoch = timestamp_to_epoch(endtime)
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

    def store_tag_metadata(self, tagname, metadata):
        tagname = safe_tagname(tagname)
        key = f"/{tagname}"
        with pd.HDFStore(self.filename, mode="a") as f:
            if key not in f:
                f.put(key, pd.DataFrame())
            origmetadata = {}
            if "metadata" in f.get_storer(key).attrs:
                origmetadata = f.get_storer(key).attrs.metadata
            f.get_storer(key).attrs.metadata = {**origmetadata, **metadata}

    def fetch_tag_metadata(self, tagname, properties):
        res = {}
        if not os.path.isfile(self.filename):
            return res
        tagname = safe_tagname(tagname)
        key = f"/{tagname}"
        if isinstance(properties, str):
            properties = [properties]
        with pd.HDFStore(self.filename, mode="r") as f:
            if key not in f or "metadata" not in f.get_storer(key).attrs:
                return {}
            metadata = f.get_storer(key).attrs.metadata
        for p in properties:
            if p in metadata.keys():
                res[p] = metadata.get(p)
        return res

    def remove(self, filename=None):
        if not filename:
            filename = self.filename
            # self.close()
        if os.path.isfile(filename):
            os.unlink(filename)

    def store(
        self,
        df: pd.DataFrame,
        tagname: str,
        readtype: ReaderType,
        ts: pd.Timestamp,
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ):
        if df.empty:
            return

        intersecting = self.get_intersecting_datasets(
            tagname, readtype, ts, stepped, status, starttime, endtime
        )
        if len(intersecting) > 0:
            df_cached = pd.DataFrame()
            with pd.HDFStore(self.filename, mode="a") as f:
                for dataset in intersecting:
                    this_start, this_end = self._get_intervals_from_dataset_name(
                        dataset
                    )
                    starttime = min(starttime, this_start)
                    endtime = max(endtime, this_end)
                    df_cached.append(f.get(dataset))
                    del f[dataset]
            df = df.append(df_cached)
            df = df[~df.index.duplicated(keep="first")].sort_index()
        key = self._key_path(
            tagname, readtype, ts, stepped, status, starttime, endtime
        )
        with pd.HDFStore(self.filename, mode="a") as f:
            f.put(key, df, format="table")

    @staticmethod
    def _get_intervals_from_dataset_name(
        name: str,
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        name_with_times = name.split("/")[-1]
        if not name_with_times.count("_") == 2:
            return (None, None)
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
                where=self._key_path(tagname, readtype, ts, stepped, status)
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
            tagname, readtype, ts, stepped, status, starttime, endtime
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

        where = []
        if starttime is not None:
            where.append("index >= starttime")
        if endtime is not None:
            where.append("index <= endtime")
        where = " and ".join(where)

        with pd.HDFStore(self.filename, mode="r") as f:
            datasets = self.get_intersecting_datasets(
                tagname, readtype, ts, stepped, status, starttime, endtime
            )
            df_total = pd.DataFrame()
            for dataset in datasets:
                if where:
                    df = f.select(
                        dataset, where="index >= starttime and index <= endtime"
                    )
                else:
                    df = f.select(dataset)
                df_total = df_total.append(df)

        return df_total.sort_index()


class SmartCache:
    def __init__(self, filename, path="."):
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

    def key_path(self, df, readtype, ts=None):
        """Return a string on the form
        XXX/sYY/ZZZ where XXX is the ReadType, YY is the interval between samples
        (in seconds) and ZZZ is a safe tagname.
        """
        name = list(df)[0] if isinstance(df, pd.DataFrame) else df
        name = safe_tagname(name)
        ts = int(ts.total_seconds()) if isinstance(ts, pd.Timedelta) else ts
        if readtype != ReaderType.RAW:
            if ts is None:
                # Determine sample time by reading interval between first two
                # samples of dataframe.
                if isinstance(df, pd.DataFrame):
                    interval = int(df[0:2].index.to_series().diff().mean().value / 1e9)
                else:
                    raise TypeError
            else:
                interval = int(ts)
            return f"{readtype.name}/s{interval}/{name}"
        else:
            return f"{readtype.name}/{name}"

    def store(self, df, readtype, ts=None):
        key = self.key_path(df, readtype, ts)
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

    def fetch(self, tagname, readtype, ts=None, start_time=None, stop_time=None):
        df = pd.DataFrame()
        if not os.path.isfile(self.filename):
            return df
        key = self.key_path(tagname, readtype, ts)
        where = []
        if start_time is not None:
            where.append("index >= start_time")
        if stop_time is not None:
            where.append("index <= stop_time")
        where = " and ".join(where)
        with pd.HDFStore(self.filename, mode="r") as f:
            if key in f:
                if where:
                    df = f.select(key, where=where)
                else:
                    df = f.select(key)
        return df.sort_index()

    def store_tag_metadata(self, tagname, metadata):
        tagname = safe_tagname(tagname)
        key = f"/metadata/{tagname}"
        with pd.HDFStore(self.filename, mode="a") as f:
            if key not in f:
                f.put(key, pd.DataFrame())
            origmetadata = {}
            if "metadata" in f.get_storer(key).attrs:
                origmetadata = f.get_storer(key).attrs.metadata
            f.get_storer(key).attrs.metadata = {**origmetadata, **metadata}

    def fetch_tag_metadata(self, tagname, properties):
        res = {}
        if not os.path.isfile(self.filename):
            return res
        tagname = safe_tagname(tagname)
        key = f"/metadata/{tagname}"
        if isinstance(properties, str):
            properties = [properties]
        with pd.HDFStore(self.filename, mode="r") as f:
            if key not in f or "metadata" not in f.get_storer(key).attrs:
                return {}
            metadata = f.get_storer(key).attrs.metadata
        for p in properties:
            if p in metadata.keys():
                res[p] = metadata.get(p)
        return res

    def remove(self, filename=None):
        if not filename:
            filename = self.filename
            # self.close()
        if os.path.isfile(filename):
            os.unlink(filename)

    def _match_tag(self, key, readtype=None, ts=None, tagname=None):
        def readtype_to_str(rt):
            return getattr(
                rt, "name", rt
            )  # if isinstance(rt, ReaderType) always returns False...?

        def timedelta_to_str(t):
            if isinstance(t, pd.Timedelta):
                return str(int(t.total_seconds()))
            return t

        key = "/" + key.lstrip("/")  # Ensure absolute path
        readtype = readtype if isinstance(readtype, list) else [readtype]
        ts = ts if isinstance(ts, list) else [ts]
        tagname = tagname if isinstance(tagname, list) else [tagname]
        readtype = list(map(readtype_to_str, readtype))
        ts = list(map(timedelta_to_str, ts))
        if tagname[0] is not None:
            tagname = list(map(safe_tagname, tagname))
        # print(f"Readtype: {readtype}, ts: {ts}, tagname: {tagname}")
        elements = key.split("/")[1:]
        if len(elements) == 2:
            elements.insert(1, None)
        else:
            elements[1] = int(elements[1][1:])  # Discard the initial s
        if elements[0] not in readtype and readtype[0] is not None:
            # print(f"{elements[0]} not in {readtype}")
            return False
        if elements[1] not in ts and ts[0] is not None:
            # print(f"{elements[1]} not in {ts}")
            return False
        if elements[2] not in tagname and tagname[0] is not None:
            # print(f"{elements[2]} not in {tagname}")
            return False
        return True

    def delete_key(self, tagname=None, readtype=None, ts=None):
        with pd.HDFStore(self.filename) as f:
            for key in f:
                if self._match_tag(key, tagname=tagname, readtype=readtype, ts=ts):
                    f.remove(key)

    def _get_hdfstore(self, mode="r"):
        f = pd.HDFStore(self.filename, mode)
        return f
