import concurrent
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pytz

from tagreader.cache import BucketCache, SmartCache
from tagreader.logger import logger
from tagreader.utils import (
    IMSType,
    ReaderType,
    convert_to_pydatetime,
    ensure_datetime_with_tz,
    find_registry_key,
    find_registry_key_from_name,
    is_windows,
)
from tagreader.web_handlers import (
    AspenHandlerWeb,
    PIHandlerWeb,
    get_auth_aspen,
    get_auth_pi,
    list_aspenone_sources,
    list_piwebapi_sources,
)

if is_windows():
    import pyodbc

    from tagreader.odbc_handlers import (
        AspenHandlerODBC,
        PIHandlerODBC,
        list_aspen_sources,
        list_pi_sources,
    )

NONE_START_TIME = datetime(1970, 1, 1, tzinfo=pytz.UTC)


def list_sources(
    imstype: Union[IMSType, str],
    url: Optional[str] = None,
    auth: Optional[Any] = None,
    verifySSL: bool = True,
) -> List[str]:
    if isinstance(imstype, str):
        try:
            imstype = getattr(IMSType, imstype.upper())
        except AttributeError:
            raise ValueError(
                f"imstype needs to be one of {', '.join([v for v in IMSType.__members__.values()])}."
                f" We suggest to use the tagreader.IMSType enumerator when initiating a client."
            )
    accepted_values = [IMSType.PIWEBAPI, IMSType.ASPENONE]
    win_accepted_values = [IMSType.PI, IMSType.ASPEN, IMSType.IP21]
    if is_windows():
        accepted_values.extend(win_accepted_values)

    if imstype == IMSType.PI:
        return list_pi_sources()
    elif imstype in [IMSType.ASPEN, IMSType.IP21]:
        return list_aspen_sources()
    elif imstype == IMSType.PIWEBAPI:
        if auth is None:
            auth = get_auth_pi()
        return list_piwebapi_sources(url=url, auth=auth, verifySSL=verifySSL)
    elif imstype == IMSType.ASPENONE:
        if auth is None:
            auth = get_auth_aspen()
        return list_aspenone_sources(url=url, auth=auth, verifySSL=verifySSL)


def get_missing_intervals(
    df: pd.DataFrame,
    start_time: datetime,
    stop_time: datetime,
    ts: Optional[timedelta],
    read_type: ReaderType,
):
    if (
        read_type == ReaderType.RAW
    ):  # Fixme: How to check for completeness for RAW data?
        return [[start_time, stop_time]]
    seconds = int(ts.total_seconds())
    tvec = pd.date_range(start=start_time, end=stop_time, freq=f"{seconds}s")
    if len(df) == len(tvec):  # Short-circuit if dataset is complete
        return []
    values_in_df = tvec.isin(df.index)
    missing_intervals = []
    for k, g in groupby(enumerate(values_in_df), lambda ix: ix[1]):
        if not k:
            seq = list(map(itemgetter(0), g))
            missing_intervals.append(
                (
                    pd.Timestamp(tvec[seq[0]]).to_pydatetime(),
                    pd.Timestamp(tvec[seq[-1]]).to_pydatetime(),
                )
            )
            # Should be unnecessary to fetch overlapping points since get_next_timeslice
            # ensures start <= t <= stop
            # missingintervals.append((pd.Timestamp(tvec[seq[0]]),
            #                          pd.Timestamp(tvec[min(seq[-1]+1, len(tvec)-1)])))
    return missing_intervals


def get_next_timeslice(
    start_time: datetime,
    stop_time: datetime,
    ts: Optional[timedelta],
    max_steps: Optional[int],
) -> Tuple[datetime, datetime]:
    if max_steps is None:
        calc_stop_time = stop_time
    else:
        calc_stop_time = start_time + ts * max_steps
    calc_stop_time = min(stop_time, calc_stop_time)
    # Ensure we include the last data point.
    # Discrepancies between Aspen and Pi for +ts
    # Discrepancies between IMS and cache for e.g. ts.
    # if calc_stop_time == stop_time:
    #     calc_stop_time += ts / 2
    return start_time, calc_stop_time


def get_server_address_aspen(datasource: str) -> Optional[Tuple[str, int]]:
    """Data sources are listed under
    HKEY_CURRENT_USER\\Software\\AspenTech\\ADSA\\Caches\\AspenADSA\\username.
    For each data source there are multiple keys with Host entries. We start by
    identifying the correct key to use by locating the UUID for Aspen SQLplus
    services located under Aspen SQLplus service component. Then we find the
    host and port based on the path above and the UUID.
    """

    if not is_windows():
        return None
    import winreg

    regkey_clsid = winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Classes\Wow6432Node\CLSID"
    )
    regkey, _ = find_registry_key_from_name(
        regkey_clsid, "Aspen SQLplus service component"
    )
    regkey_implemented_categories = winreg.OpenKeyEx(regkey, "Implemented Categories")

    _, aspen_UUID = find_registry_key_from_name(
        regkey_implemented_categories, "Aspen SQLplus services"
    )

    reg_adsa = winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        r"Software\AspenTech\ADSA\Caches\AspenADSA\\" + os.getlogin(),
    )

    try:
        reg_site_key = winreg.OpenKey(reg_adsa, datasource + "\\" + aspen_UUID)
        host = winreg.QueryValueEx(reg_site_key, "Host")[0]
        port = int(winreg.QueryValueEx(reg_site_key, "Port")[0])
        return host, port
    except FileNotFoundError:
        return None


def get_server_address_pi(datasource: str) -> Optional[Tuple[str, int]]:
    """
    PI data sources are listed under
    HKEY_LOCAL_MACHINE\\Software\\Wow6432Node\\PISystem\\PI-SDK\\x.x\\ServerHandles or
    \\Software\\PISystem\\PI-SDK\\x.x\\ServerHandles.

    :param datasource: Name of data source
    :return: host, port
    :type: tuple(string, int)
    """

    if not is_windows():
        return None
    import winreg

    try:
        reg_key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\PISystem\PI-SDK"
        )
        reg_key_handles = find_registry_key(reg_key, "ServerHandles")
        reg_site_key = find_registry_key(reg_key_handles, datasource)
        if reg_site_key is None:
            reg_key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\PISystem\PI-SDK"
            )
            reg_key_handles = find_registry_key(reg_key, "ServerHandles")
            reg_site_key = find_registry_key(reg_key_handles, datasource)
        if reg_site_key is not None:
            host = winreg.QueryValueEx(reg_site_key, "path")[0]
            port = int(winreg.QueryValueEx(reg_site_key, "port")[0])
            return host, port
    except FileNotFoundError:
        return None


def get_handler(
    imstype: Optional[IMSType],
    datasource: str,
    url: Optional[str],
    host: Optional[str],
    port: Optional[int],
    options: Dict[str, Union[int, float, str]],
    verifySSL: Optional[bool],
    auth: Optional[Any],
):
    if imstype is None:
        if datasource in list_aspenone_sources(
            url=None, auth=None, verifySSL=verifySSL
        ):
            imstype = IMSType.ASPENONE
        elif datasource in list_piwebapi_sources(
            url=None, auth=None, verifySSL=verifySSL
        ):
            imstype = IMSType.PIWEBAPI

    if imstype == IMSType.PI:
        if not is_windows():
            raise RuntimeError(
                "ODBC drivers not available for non-Windows environments. "
                "Try Web API ('piwebapi') instead."
            )
        if "PI ODBC Driver" not in pyodbc.drivers():
            raise RuntimeError(
                "No PI ODBC driver detected. "
                "Either switch to Web API ('piwebapi') or install appropriate driver."
            )
        if host is None:
            hostport = get_server_address_pi(datasource)
            if not hostport:
                raise ValueError(
                    f"Unable to locate data source '{datasource}'."
                    "Do you have correct permissions?"
                )
            host, port = hostport
        if port is None:
            port = 5450
        return PIHandlerODBC(host=host, port=port, options=options)

    if imstype in [IMSType.ASPEN, IMSType.IP21]:
        if not is_windows():
            raise RuntimeError(
                "ODBC drivers not available for non-Windows environments. "
                "Try Web API ('aspenone') instead."
            )
        if "AspenTech SQLplus" not in pyodbc.drivers():
            raise RuntimeError(
                "No Aspen SQLplus ODBC driver detected. "
                "Either switch to Web API ('aspenone') or install appropriate driver."
            )
        if host is None:
            hostport = get_server_address_aspen(datasource)
            if not hostport:
                raise ValueError(
                    f"Unable to locate data source '{datasource}'."
                    "Do you have correct permissions?"
                )
            host, port = hostport
        if port is None:
            port = 10014
        return AspenHandlerODBC(host=host, port=port, options=options)

    if imstype == IMSType.PIWEBAPI:
        return PIHandlerWeb(
            url=url,
            datasource=datasource,
            options=options,
            verifySSL=verifySSL,
            auth=auth,
        )

    if imstype == IMSType.ASPENONE:
        return AspenHandlerWeb(
            datasource=datasource,
            url=url,
            options=options,
            verifySSL=verifySSL,
            auth=auth,
        )

    raise ValueError(
        f"Could not infer IMSType for datasource: {datasource}."
        f"Please specify correct datasource, imstype or host, or refer to the user docs."
    )


class IMSClient:
    def __init__(
        self,
        datasource: str,
        imstype: Optional[Union[str, IMSType]] = None,
        tz: pytz.timezone = pytz.timezone("Europe/Oslo"),
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        handler_options: Dict[str, Union[int, float, str]] = {},  # noqa:
        verifySSL: bool = True,
        auth: Optional[Any] = None,
    ):
        if isinstance(imstype, str):
            try:
                imstype = getattr(IMSType, imstype.upper())
            except AttributeError:
                raise ValueError(
                    f"imstype needs to be one of {', '.join([v for v in IMSType.__members__.values()])}."
                    f" We suggest to use the tagreader.IMSType enumerator when initiating a client."
                )

        self.tz = tz
        self.handler = get_handler(
            imstype=imstype,
            datasource=datasource,
            url=url,
            host=host,
            port=port,
            options=handler_options,
            verifySSL=verifySSL,
            auth=auth,
        )
        self.cache = SmartCache(directory=Path(".") / ".cache" / datasource)

    def connect(self) -> None:
        self.handler.connect()

    def search_tag(
        self, tag: Optional[str] = None, desc: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        logger.warning("This function is deprecated. Please call 'search()' instead")
        return self.search(tag=tag, desc=desc)

    def search(
        self, tag: Optional[str] = None, desc: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        return self.handler.search(tag=tag, desc=desc)

    def _get_metadata(self, tag: str):
        return self.handler._get_tag_metadata(
            tag
        )  # noqa: Should probably expose this as a public method if needed.

    def _read_single_tag(
        self,
        tag: str,
        start_time: Optional[datetime],
        stop_time: Optional[datetime],
        ts: timedelta,
        read_type: ReaderType,
        get_status: bool,
        cache: Optional[Union[BucketCache, SmartCache]],
    ):
        if read_type == ReaderType.SNAPSHOT:
            metadata = self._get_metadata(tag)
            df = self.handler.read_tag(
                tag=tag,
                start_time=start_time,
                stop_time=stop_time,
                sample_time=ts,
                read_type=read_type,
                metadata=metadata,
                get_status=get_status,
            )
        else:
            stepped = False
            missing_intervals = [(start_time, stop_time)]
            df = pd.DataFrame()

            if (
                isinstance(cache, SmartCache)
                and read_type != ReaderType.RAW
                and not get_status
            ):
                time_slice = get_next_timeslice(
                    start_time=start_time, stop_time=stop_time, ts=ts, max_steps=None
                )
                df = cache.fetch(
                    tagname=tag,
                    readtype=read_type,
                    ts=ts,
                    start_time=time_slice[0],
                    stop_time=time_slice[1],
                )
                missing_intervals = get_missing_intervals(
                    df=df,
                    start_time=start_time,
                    stop_time=stop_time,
                    ts=ts,
                    read_type=read_type,
                )
                if not missing_intervals:
                    return df.tz_convert(self.tz).sort_index()
            elif isinstance(cache, BucketCache):
                df = cache.fetch(
                    tagname=tag,
                    readtype=read_type,
                    ts=ts,
                    stepped=stepped,
                    status=get_status,
                    starttime=start_time,
                    endtime=stop_time,
                )
                missing_intervals = cache.get_missing_intervals(
                    tagname=tag,
                    readtype=read_type,
                    ts=ts,
                    stepped=stepped,
                    status=get_status,
                    starttime=start_time,
                    endtime=stop_time,
                )
                if not missing_intervals:
                    return df.tz_convert(self.tz).sort_index()

            metadata = self._get_metadata(tag)
            frames = [df]
            for start, stop in missing_intervals:
                while True:
                    df = self.handler.read_tag(
                        tag=tag,
                        start_time=start,
                        stop_time=stop,
                        sample_time=ts,
                        read_type=read_type,
                        metadata=metadata,
                        get_status=get_status,
                    )
                    if len(df.index) > 0:
                        if (
                            cache is not None
                            and read_type
                            not in [
                                ReaderType.SNAPSHOT,
                                ReaderType.RAW,
                            ]
                            and not get_status
                        ):
                            cache.store(df=df, readtype=read_type, ts=ts)
                    frames.append(df)
                    if len(df) < self.handler._max_rows:
                        break
                    start = df.index[-1]
                # if read_type != ReaderType.RAW:
                #     time_slice = [start, start]
                #     while time_slice[1] < stop:
                #         time_slice = get_next_timeslice(
                #             time_slice[1], stop, ts, self.handler._max_rows
                #         )
                #         df = self.handler.read_tag(
                #             tag, time_slice[0], time_slice[1], ts, read_type, metadata
                #         )
                #         if len(df.index) > 0:
                #             if cache is not None and read_type != ReaderType.RAW:
                #                 cache.store(df, read_type, ts)
                #             frames.append(df)

            # df = pd.concat(frames, verify_integrity=True)
            df = pd.concat(frames)
            # read_type INT leads to overlapping values after concatenating
            # due to both start time and end time included.
            # Aggregate read_types (should) align perfectly and don't
            # (shouldn't) need deduplication.
            df = df[~df.index.duplicated(keep="first")]  # Deduplicate on index
        df = df.tz_convert(self.tz).sort_index()
        df = df.rename(columns={"value": tag})
        return df

    def get_units(self, tags: Union[str, List[str]]):
        if isinstance(tags, str):
            tags = [tags]
        units = {}
        for tag in tags:
            if self.cache is not None:
                r = self.cache.get_metadata(key=tag, properties="unit")
                if r is not None and "unit" in r:
                    units[tag] = r["unit"]
            if tag not in units:
                unit = self.handler._get_tag_unit(tag)
                if self.cache is not None and unit is not None:
                    self.cache.put_metadata(key=tag, value={"unit": unit})
                units[tag] = unit
        return units

    def get_descriptions(self, tags: Union[str, List[str]]) -> Dict[str, str]:
        if isinstance(tags, str):
            tags = [tags]
        descriptions = {}
        for tag in tags:
            if self.cache is not None:
                r = self.cache.get_metadata(key=tag, properties="description")
                if r is not None and "description" in r:
                    descriptions[tag] = r["description"]
            if tag not in descriptions:
                desc = self.handler._get_tag_description(tag)
                if self.cache is not None and desc is not None:
                    self.cache.put_metadata(key=tag, value={"description": desc})
                descriptions[tag] = desc
        return descriptions

    def read_tags(
        self,
        tags: Union[str, List[str]],
        start_time: Optional[Union[datetime, pd.Timestamp, str]] = None,
        stop_time: Optional[Union[datetime, pd.Timestamp, str]] = None,
        ts: Optional[Union[timedelta, pd.Timedelta]] = timedelta(seconds=60),
        read_type: ReaderType = ReaderType.INT,
        get_status: bool = False,
    ):
        logger.warn(
            (
                "This function has been renamed to read() and is deprecated. "
                "Please call 'read()' instead"
            )
        )
        return self.read(
            tags=tags,
            start_time=start_time,
            end_time=stop_time,
            ts=ts,
            read_type=read_type,
            get_status=get_status,
        )

    def read(
        self,
        tags: Union[str, List[str]],
        start_time: Optional[Union[datetime, pd.Timestamp, str]] = None,
        end_time: Optional[Union[datetime, pd.Timestamp, str]] = None,
        ts: Union[timedelta, pd.Timedelta, int] = timedelta(seconds=60),
        read_type: ReaderType = ReaderType.INT,
        get_status: bool = False,
    ) -> pd.DataFrame:
        """Reads values for the specified [tags] from the IMS server for the
        time interval from [start_time] to [stop_time] in intervals [ts].

        The interval [ts] can be specified as pd.Timedelta or as an integer,
        in which case it will be interpreted as seconds.

        Default value for [read_type] is ReaderType.INT, which interpolates
        the raw data.
        All possible values for read_type are defined in the ReaderType class,
        which can be imported as follows:
            from utils import ReaderType

        Values for ReaderType.* that should work for all handlers are:
            INT, RAW, MIN, MAX, RNG, AVG, VAR, STD and SNAPSHOT
        """
        if isinstance(tags, str):
            tags = [tags]
        if isinstance(read_type, str):
            try:
                read_type = getattr(ReaderType, read_type)
            except AttributeError:
                ValueError(
                    "readtype needs to be of type ReaderType.* or a legal value. Please refer to the docstring."
                )
        if read_type in [ReaderType.RAW, ReaderType.SNAPSHOT] and len(tags) > 1:
            raise RuntimeError(
                "Unable to read raw/sampled data for multiple tags since they don't "
                "share time vector. Read one at a time."
            )

        if isinstance(tags, str):
            tags = [tags]

        if start_time is None:
            start_time = NONE_START_TIME
        elif isinstance(start_time, (str, pd.Timestamp)):
            try:
                start_time = convert_to_pydatetime(start_time)
            except ValueError:
                start_time = convert_to_pydatetime(start_time)
        if end_time is None:
            end_time = datetime.utcnow()
        elif isinstance(end_time, (str, pd.Timestamp)):
            end_time = convert_to_pydatetime(end_time)

        if isinstance(ts, pd.Timedelta):
            ts = ts.to_pytimedelta()
        elif isinstance(ts, int):
            ts = timedelta(seconds=ts)
        elif not isinstance(ts, timedelta):
            raise ValueError(
                "ts needs to be either a None, timedelta or and integer (number of seconds)."
                f" Given type: {type(ts)}"
            )

        if read_type != ReaderType.SNAPSHOT:
            start_time = ensure_datetime_with_tz(start_time, tz=self.tz)
        if end_time:
            end_time = ensure_datetime_with_tz(end_time, tz=self.tz)

        oldtags = tags
        tags = list(dict.fromkeys(tags))
        if len(oldtags) > len(tags):
            duplicates = set([x for n, x in enumerate(oldtags) if x in oldtags[:n]])
            logger.warning(
                f"Duplicate tags found, removed duplicates: {', '.join(duplicates)}"
            )

        # Fixme: Temporary reading from IMS using Threads to improve performance. Need to use batch queries or at least
        #     keep the same connection between queries.
        results = []
        with ThreadPoolExecutor(
            max_workers=min(10, (os.cpu_count() or 1) + 4)
        ) as executor:
            for i, tag in enumerate(tags):
                results.append(
                    executor.submit(
                        self._read_single_tag,
                        tag=tag,
                        start_time=start_time,
                        stop_time=end_time,
                        ts=ts,
                        read_type=read_type,
                        get_status=get_status,
                        cache=self.cache,
                    )
                )

        return pd.concat(
            [result.result() for result in concurrent.futures.as_completed(results)],
            axis=1,
        )

    def query_sql(self, query: str, parse: bool = True):
        """[summary]
        Args:
            query (str): [description]
            parse (bool, optional): Whether to attempt to parse query return
                                    value as table. Defaults to True.
        Returns:
            Union[pd.DataFrame, pyodbc.Cursor, str]: Return value
        """
        df_or_cursor = self.handler.query_sql(query=query, parse=parse)
        return df_or_cursor
