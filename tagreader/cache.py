from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, cast

import pandas as pd
from diskcache import Cache

from tagreader.logger import logger
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


def _infer_pandas_index_freq(df: pd.DataFrame) -> pd.DataFrame:
    try:
        if pd.infer_freq(df.index):  # type: ignore[arg-type]
            df = df.asfreq(pd.infer_freq(df.index))  # type: ignore[arg-type]
    except (TypeError, ValueError) as e:
        logger.warning(f"Could not infer frequency of timeseries in Cache. {e}")
    return df


class BaseCache(ABC):
    def __init__(
        self,
        directory: Path = Path("."),
        expire_time_in_seconds: Optional[int] = None,
        enable_stats: bool = False,
    ) -> None:
        self.cache = Cache(directory=directory.as_posix())
        self._expire_time = expire_time_in_seconds

        if enable_stats:
            self.enable_cache_statistics()

    def enable_cache_statistics(self) -> None:
        self.cache.stats(enable=True)

    def clear_cache(self) -> None:
        self.cache.clear()

    def put(self, key: str, value: pd.DataFrame) -> None:
        self.cache.add(key, value, expire=self._expire_time)

    def get(self, key: str) -> pd.DataFrame:
        return cast(pd.DataFrame, self.cache.get(key))

    def delete(self, key: str) -> None:
        self.cache.delete(key)

    def get_metadata(
        self, key: str, properties: Union[str, List[str]]
    ) -> Dict[str, Union[str, int, float]]:
        if isinstance(properties, str):
            properties = [properties]
        _key = f"$metadata${key}"
        metadata: Dict[str, Union[str, int, float]] = self.cache.get(_key)
        return {k: v for (k, v) in metadata.items() if k in properties}

    def put_metadata(
        self, key: str, value: Dict[str, Union[str, int, float]]
    ) -> Dict[str, Union[str, int, float]]:
        _key = f"$metadata${key}"
        combined_value = value
        if _key in self.cache:
            existing = self.cache.get(_key)
            existing.update(value)
            combined_value = existing
            self.cache.delete(_key)

        self.cache.add(_key, combined_value, expire=self._expire_time)
        return combined_value

    def delete_metadata(self, key: str) -> None:
        _key = f"$metadata${key}"
        self.cache.delete(_key)


class BucketCache(BaseCache):
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
        $tagname$readtype[$sample_time][$stepped][$status]$_starttime_endtime
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
            endtime_epoch = timestamp_to_epoch(endtime) if endtime else endtime
            timespan = f"$_{starttime_epoch}_{endtime_epoch}"

        keyval = (
            f"${tagname}"
            f"${readtype.name}"
            f"{(ts is not None and readtype != ReaderType.RAW) * f'$s{ts}'}"
            f"{stepped * '$stepped'}"
            f"{status * '$status'}"
            f"{timespan}"
        )
        return keyval

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
            for dataset in intersecting:
                this_start, this_end = self._get_intervals_from_dataset_name(dataset)
                starttime = min(starttime, this_start if this_start else starttime)
                endtime = max(endtime, this_end if this_end else endtime)
                df = _infer_pandas_index_freq(
                    pd.concat([df, self.get(dataset)], axis=0)
                )
                self.delete(dataset)
            df = df.drop_duplicates(subset="index", keep="last").sort_index()
        key = self._key_path(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )
        self.put(key=key, value=df)

    @staticmethod
    def _get_intervals_from_dataset_name(
        name: str,
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        name_with_times = name.split("$")[-1]
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
        if not len(self.cache) > 0:
            return []
        intersecting_datasets = []
        for dataset in self.cache.iterkeys():
            target_key = self._key_path(
                tagname=tagname,
                readtype=readtype,
                starttime=None,
                endtime=None,
                ts=ts,
                stepped=stepped,
                status=status,
            )
            if target_key in dataset:
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
        ts: Union[int, pd.Timedelta],
        stepped: bool,
        status: bool,
        starttime: pd.Timestamp,
        endtime: pd.Timestamp,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        if not len(self.cache) > 0:
            return df

        if isinstance(ts, pd.Timedelta):
            ts = int(ts.total_seconds())

        datasets = self.get_intersecting_datasets(
            tagname=tagname,
            readtype=readtype,
            ts=ts,
            stepped=stepped,
            status=status,
            starttime=starttime,
            endtime=endtime,
        )

        for dataset in datasets:
            df = _infer_pandas_index_freq(
                pd.concat([df, self.get(dataset).loc[starttime:endtime]], axis=0)  # type: ignore[call-overload, misc]
            )

        return df.drop_duplicates(subset="index", keep="last").sort_index()


class SmartCache(BaseCache):
    @staticmethod
    def key_path(
        df: Union[str, pd.DataFrame],
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timedelta]] = None,
    ) -> str:
        """Return a string on the form
        XXX$sYY$ZZZ where XXX is the ReadType, YY is the interval between samples
        (in seconds) and ZZZ is a safe tagname.
        """
        name = str(list(df)[0]) if isinstance(df, pd.DataFrame) else df
        name = safe_tagname(name)
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
            return f"{readtype.name}$s{interval}${name}"
        else:
            return f"{readtype.name}${name}"

    def store(
        self,
        df: pd.DataFrame,
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timedelta]] = None,
    ) -> None:
        key = self.key_path(df=df, readtype=readtype, ts=ts)
        if df.empty:
            return  # Weirdness ensues when using empty df in select statement below
        if key in self.cache:
            data = _infer_pandas_index_freq(pd.concat([df, self.get(key)], axis=0))
            self.delete(key=key)
            self.put(
                key=key,
                value=data.drop_duplicates(subset="index", keep="last").sort_index(),
            )
        else:
            self.put(key, df)

    def fetch(
        self,
        tagname: str,
        readtype: ReaderType,
        ts: Optional[Union[int, pd.Timedelta]] = None,
        start_time: Optional[pd.Timestamp] = None,
        stop_time: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        key = self.key_path(df=tagname, readtype=readtype, ts=ts)
        df: pd.DataFrame = self.cache.get(key=key)
        if start_time:
            df = df.loc[df.index >= start_time]
        if stop_time:
            df = df.loc[df.index <= stop_time]
        return df
