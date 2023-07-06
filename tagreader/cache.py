from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, cast

import pandas as pd
import pytz
from diskcache import Cache

from tagreader.logger import logger
from tagreader.utils import ReaderType


def safe_tagname(tagname: str) -> str:
    tagname = tagname.replace(".", "_")
    tagname = "".join(c for c in tagname if c.isalnum() or c == "_").strip()
    if tagname[0].isnumeric():
        tagname = "_" + tagname  # Conform to NaturalName
    return tagname


def timestamp_to_epoch(timestamp: datetime) -> int:
    origin = datetime(1970, 1, 1)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(pytz.utc).replace(tzinfo=None)
    return (timestamp - origin) // timedelta(seconds=1)


def _infer_pandas_index_freq(df: pd.DataFrame) -> pd.DataFrame:
    try:
        if pd.infer_freq(df.index):  # type: ignore[arg-type]
            df = df.asfreq(pd.infer_freq(df.index))  # type: ignore[arg-type]
    except (TypeError, ValueError) as e:
        logger.warning(f"Could not infer frequency of timeseries in Cache. {e}")
    return df


def _drop_duplicates_and_sort_index(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.index.duplicated(keep="first")].sort_index()


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return _infer_pandas_index_freq(_drop_duplicates_and_sort_index(df))


class BaseCache(Cache):  # type: ignore[misc]
    """
    Cache works as a Python dictionary with persistence. It is simple to use, and only requires a directory for
    the cache. The default directory is <current path>/.cache/
    """

    def __init__(
        self,
        directory: Path = Path(".") / ".cache",
        enable_stats: bool = False,
    ) -> None:
        super().__init__(directory=directory.as_posix())

        if enable_stats:
            self.enable_cache_statistics()

    def enable_cache_statistics(self) -> None:
        self.stats(enable=True)

    def put(self, key: str, value: pd.DataFrame, expire: Optional[int] = None) -> None:
        self.add(key=key, value=value, expire=expire)

    def get_metadata(
        self, key: str, properties: Optional[Union[str, List[str]]]
    ) -> Optional[Dict[str, Union[str, int, float]]]:
        if isinstance(properties, str):
            properties = [properties]
        _key = f"$metadata${key}"
        metadata = cast(Optional[Dict[str, Union[str, int, float]]], self.get(_key))
        if metadata:
            if properties:
                return {k: v for (k, v) in metadata.items() if k in properties}
            return metadata
        else:
            return None

    def put_metadata(
        self,
        key: str,
        value: Dict[str, Union[str, int, float]],
        expire: Optional[int] = None,
    ) -> Dict[str, Union[str, int, float]]:
        _key = f"$metadata${key}"
        combined_value = value
        if _key in self:
            existing = self.get(_key)
            if existing:
                existing.update(value)
                combined_value = existing
            else:
                combined_value = value
            self.delete(_key)

        self.add(_key, combined_value, expire=expire)
        return combined_value

    def delete_metadata(self, key: str) -> None:
        _key = f"$metadata${key}"
        self.delete(_key)


class BucketCache(BaseCache):
    @staticmethod
    def _key_path(
        tagname: str,
        read_type: ReaderType,
        ts: Union[int, timedelta],
        stepped: bool,
        get_status: bool,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> str:
        """Return a string on the form
        $tagname$read_type[$sample_time][$stepped][$get_status]$_start_end
        tagname: safe tagname
        sample_time: integer value. Empty for RAW.
        stepped: "stepped" if value was read as stepped. Empty if not.
        get_status: "status" if value contains status. Empty if not.
        start: The start of the query that created the bucket.
        end: The end of the query that created the bucket.
        """
        tagname = safe_tagname(tagname)

        ts = (
            int(ts.total_seconds())
            if read_type != ReaderType.RAW and isinstance(ts, timedelta)
            else ts
        )
        timespan = ""
        if start is not None:
            start_epoch = timestamp_to_epoch(start)
            end_epoch = timestamp_to_epoch(end) if end else end
            timespan = f"$_{start_epoch}_{end_epoch}"

        keyval = (
            f"${tagname}"
            f"${read_type.name}"
            f"{(ts is not None and read_type != ReaderType.RAW) * f'$s{ts}'}"
            f"{stepped * '$stepped'}"
            f"{get_status * '$get_status'}"
            f"{timespan}"
        )
        return keyval

    def store(
        self,
        df: pd.DataFrame,
        tagname: str,
        read_type: ReaderType,
        ts: timedelta,
        stepped: bool,
        get_status: bool,
        start: datetime,
        end: datetime,
    ) -> None:
        if df.empty:
            return

        intersecting = self.get_intersecting_datasets(
            tagname=tagname,
            read_type=read_type,
            ts=ts,
            stepped=stepped,
            get_status=get_status,
            start=start,
            end=end,
        )
        if len(intersecting) > 0:
            for dataset in intersecting:
                this_start, this_end = self._get_intervals_from_dataset_name(dataset)
                start = min(start, this_start if this_start else start)
                end = max(end, this_end if this_end else end)
                df2 = self.get(dataset)
                if df2 is not None:
                    df = pd.concat([df, df2], axis=0)
                self.delete(dataset)
        key = self._key_path(
            tagname=tagname,
            read_type=read_type,
            ts=ts,
            stepped=stepped,
            get_status=get_status,
            start=start,
            end=end,
        )
        self.put(key=key, value=clean_dataframe(df))

    @staticmethod
    def _get_intervals_from_dataset_name(
        name: str,
    ) -> Tuple[datetime, datetime]:
        name_with_times = name.split("$")[-1]
        if not name_with_times.count("_") == 2:
            return None, None  # type: ignore[return-value]
        _, start_epoch, end_epoch = name_with_times.split("_")
        start = pd.to_datetime(int(start_epoch), unit="s").tz_localize("UTC")
        end = pd.to_datetime(int(end_epoch), unit="s").tz_localize("UTC")
        return start, end

    def get_intersecting_datasets(
        self,
        tagname: str,
        read_type: ReaderType,
        ts: Union[int, timedelta],
        stepped: bool,
        get_status: bool,
        start: datetime,
        end: datetime,
    ) -> List[str]:
        if not len(self) > 0:
            return []
        intersecting_datasets = []
        for dataset in self.iterkeys():
            target_key = self._key_path(
                tagname=tagname,
                read_type=read_type,
                start=None,
                end=None,
                ts=ts,
                stepped=stepped,
                get_status=get_status,
            )
            if target_key in dataset:
                start_ds, end_ds = self._get_intervals_from_dataset_name(dataset)
                if end_ds >= start and end >= start_ds:
                    intersecting_datasets.append(dataset)
        return intersecting_datasets

    def get_missing_intervals(
        self,
        tagname: str,
        read_type: ReaderType,
        ts: Union[int, timedelta],
        stepped: bool,
        get_status: bool,
        start: datetime,
        end: datetime,
    ) -> List[Tuple[datetime, datetime]]:
        datasets = self.get_intersecting_datasets(
            tagname=tagname,
            read_type=read_type,
            ts=ts,
            stepped=stepped,
            get_status=get_status,
            start=start,
            end=end,
        )
        missing_intervals = [(start, end)]
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
        read_type: ReaderType,
        ts: Union[int, timedelta],
        stepped: bool,
        get_status: bool,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        if not len(self) > 0:
            return df

        if isinstance(ts, timedelta):
            ts = int(ts.total_seconds())

        datasets = self.get_intersecting_datasets(
            tagname=tagname,
            read_type=read_type,
            ts=ts,
            stepped=stepped,
            get_status=get_status,
            start=start,
            end=end,
        )

        for dataset in datasets:
            df2 = self.get(dataset)
            if df2 is not None:
                df = pd.concat([df, df2.loc[start:end]], axis=0)  # type: ignore[call-overload, misc]

        return clean_dataframe(df)


class SmartCache(BaseCache):
    @staticmethod
    def key_path(
        df: Union[str, pd.DataFrame],
        read_type: ReaderType,
        ts: Optional[Union[int, timedelta]] = None,
    ) -> str:
        """Return a string on the form
        XXX$sYY$ZZZ where XXX is the ReadType, YY is the interval between samples
        (in seconds) and ZZZ is a safe tagname.
        """
        name = str(list(df)[0]) if isinstance(df, pd.DataFrame) else df
        name = safe_tagname(name)
        ts = int(ts.total_seconds()) if isinstance(ts, timedelta) else ts
        if read_type != ReaderType.RAW:
            if ts is None:
                # Determine sample time by reading interval between first two
                # samples of dataframe.
                if isinstance(df, pd.DataFrame):
                    interval = int(df[0:2].index.to_series().diff().mean().value / 1e9)  # type: ignore[attr-defined]
                else:
                    raise TypeError
            else:
                interval = int(ts)
            return f"{read_type.name}$s{interval}${name}"
        else:
            return f"{read_type.name}${name}"

    def store(
        self,
        df: pd.DataFrame,
        read_type: ReaderType,
        ts: Optional[Union[int, timedelta]] = None,
    ) -> None:
        key = self.key_path(df=df, read_type=read_type, ts=ts)
        if df.empty:
            return  # Weirdness ensues when using empty df in select statement below
        if key in self:
            df2 = self.get(key)
            if df2 is not None:
                df = pd.concat([df, df2], axis=0)
            self.delete(key=key)
            self.put(
                key=key,
                value=clean_dataframe(df),
            )
        else:
            self.put(key, df)

    def fetch(
        self,
        tagname: str,
        read_type: ReaderType,
        ts: Optional[Union[int, timedelta]] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pd.DataFrame:
        key = self.key_path(df=tagname, read_type=read_type, ts=ts)
        df = cast(Optional[pd.DataFrame], self.get(key=key))
        if df is None:
            return pd.DataFrame()
        if start is not None:
            df = df.loc[df.index >= start]
        if end is not None:
            df = df.loc[df.index <= end]
        return df
