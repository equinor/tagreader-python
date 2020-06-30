import os
import pyodbc
import pandas as pd
from itertools import groupby
from operator import itemgetter
from .utils import (
    datestr_to_datetime,
    find_registry_key,
    find_registry_key_from_name,
    logging,
    ReaderType,
    winreg,
)
from .cache import SmartCache
from .odbc_handlers import PIHandlerODBC, AspenHandlerODBC
from .web_handlers import PIHandlerWeb, AspenHandlerWeb

logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def get_missing_intervals(df, start_time, stop_time, ts, read_type):
    if (
        read_type == ReaderType.RAW
    ):  # Fixme: How to check for completeness for RAW data?
        return [[start_time, stop_time]]
    tvec = pd.date_range(start=start_time, end=stop_time, freq=f"{ts}s")
    if len(df) == len(tvec):  # Short-circuit if dataset is complete
        return []
    values_in_df = tvec.isin(df.index)
    missing_intervals = []
    for k, g in groupby(enumerate(values_in_df), lambda ix: ix[1]):
        if not k:
            seq = list(map(itemgetter(0), g))
            missing_intervals.append(
                (pd.Timestamp(tvec[seq[0]]), pd.Timestamp(tvec[seq[-1]]))
            )
            # Should be unnecessary to fetch overlapping points since get_next_timeslice
            # ensures start <= t <= stop
            # missingintervals.append((pd.Timestamp(tvec[seq[0]]),
            #                          pd.Timestamp(tvec[min(seq[-1]+1, len(tvec)-1)])))
    return missing_intervals


def get_next_timeslice(start_time, stop_time, ts, max_steps=None):
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


def get_server_address_aspen(datasource):
    """Data sources are listed under
    HKEY_CURRENT_USER\\Software\\AspenTech\\ADSA\\Caches\\AspenADSA\\username.
    For each data source there are multiple keys with Host entries. We start by
    identifying the correct key to use by locating the UUID for Aspen SQLplus
    services located under Aspen SQLplus service component. Then we find the
    host and port based on the path above and the UUID.
    """

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


def get_server_address_pi(datasource):
    """
    PI data sources are listed under
    HKEY_LOCAL_MACHINE\\Software\\Wow6432Node\\PISystem\\PI-SDK\\x.x\\ServerHandles or
    \\Software\\PISystem\\PI-SDK\\x.x\\ServerHandles.

    :param datasource: Name of data source
    :return: host, port
    :type: tuple(string, int)
    """
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


def get_handler(imstype, datasource, url=None, options={}, verifySSL=None, auth=None):
    accepted_values = ["pi", "aspen", "ip21", "piweb", "aspenweb", "ip21web"]

    if not imstype or imstype.lower() not in accepted_values:
        raise ValueError(f"`imstype` must be one of {accepted_values}")

    if imstype.lower() == "pi":
        if "PI ODBC Driver" not in pyodbc.drivers():
            raise RuntimeError(
                "No PI ODBC driver detected. "
                "Either switch to Web API ('piweb') or install appropriate driver."
            )
        hostport = get_server_address_pi(datasource)
        if not hostport:
            raise ValueError(
                f"Unable to locate data source '{datasource}'."
                "Do you have correct permissions?"
            )
        host, port = hostport
        return PIHandlerODBC(host=host, port=port, options=options)

    if imstype.lower() in ["aspen", "ip21"]:
        if "AspenTech SQLplus" not in pyodbc.drivers():
            raise RuntimeError(
                "No Aspen SQLplus ODBC driver detected. Either switch to Web API "
                "('aspenweb') or install appropriate driver."
            )
        hostport = get_server_address_aspen(datasource)
        if not hostport:
            raise ValueError(
                f"Unable to locate data source '{datasource}'."
                "Do you have correct permissions?"
            )
        host, port = hostport
        return AspenHandlerODBC(host=host, port=port, options=options)

    if imstype.lower() == "piweb":
        return PIHandlerWeb(
            url=url,
            datasource=datasource,
            options=options,
            verifySSL=verifySSL,
            auth=auth,
        )

    if imstype.lower() in ["aspenweb", "ip21web"]:
        return AspenHandlerWeb(
            datasource=datasource,
            url=url,
            options=options,
            verifySSL=verifySSL,
            auth=auth,
        )


class IMSClient:
    def __init__(
        self,
        datasource,
        imstype=None,
        tz="Europe/Oslo",
        url=None,
        handler_options={},
        verifySSL=None,
        auth=None,
    ):
        self.handler = None
        self.datasource = datasource.lower()  # FIXME
        self.tz = tz
        self.handler = get_handler(
            imstype,
            datasource,
            url=url,
            options=handler_options,
            verifySSL=verifySSL,
            auth=auth,
        )
        self.cache = SmartCache(datasource)

    def connect(self):
        self.handler.connect()

    def search_tag(self, tag=None, desc=None):
        return self.handler.search_tag(tag, desc)

    def _get_metadata(self, tag):
        return self.handler._get_tag_metadata(tag)

    def _read_single_tag(self, tag, start_time, stop_time, ts, read_type, cache=None):
        missing_intervals = [(start_time, stop_time)]
        df = pd.DataFrame()
        if cache is not None:
            time_slice = get_next_timeslice(start_time, stop_time, ts)
            df = cache.fetch(
                tag,
                readtype=read_type,
                ts=ts,
                start_time=time_slice[0],
                stop_time=time_slice[1],
            )
            missing_intervals = get_missing_intervals(
                df, start_time, stop_time, ts.seconds, read_type
            )
            if not missing_intervals:
                return df
        metadata = self._get_metadata(tag)
        frames = [df]
        for (start, stop) in missing_intervals:
            time_slice = [start, start]
            while time_slice[1] < stop:
                time_slice = get_next_timeslice(
                    time_slice[1], stop, ts, self.handler._max_rows
                )
                df = self.handler.read_tag(
                    tag, time_slice[0], time_slice[1], ts, read_type, metadata
                )
                if cache is not None:
                    cache.store(df, read_type, ts)
                frames.append(df)
        # df = pd.concat(frames, verify_integrity=True)
        df = pd.concat(frames)
        df.sort_index(inplace=True)
        # read_type INT leads to overlapping values after concatenating
        # due to both start time and end time included.
        # Aggregate read_types (should) align perfectly and don't
        # (shouldn't) need deduplication.
        df = df[~df.index.duplicated(keep="first")]  # Deduplicate on index
        df = df.rename(columns={"value": tag})
        return df

    def get_units(self, tags):
        if isinstance(tags, str):
            tags = [tags]
        units = {}
        for tag in tags:
            if self.cache is not None:
                r = self.cache.fetch_tag_metadata(tag, "unit")
                if "unit" in r:
                    units[tag] = r["unit"]
            if tag not in units:
                unit = self.handler._get_tag_unit(tag)
                if self.cache is not None:
                    self.cache.store_tag_metadata(tag, {"unit": unit})
                units[tag] = unit
        return units

    def get_descriptions(self, tags):
        if isinstance(tags, str):
            tags = [tags]
        descriptions = {}
        for tag in tags:
            if self.cache is not None:
                r = self.cache.fetch_tag_metadata(tag, "description")
                if "description" in r:
                    descriptions[tag] = r["description"]
            if tag not in descriptions:
                desc = self.handler._get_tag_description(tag)
                if self.cache is not None:
                    self.cache.store_tag_metadata(tag, {"description": desc})
                descriptions[tag] = desc
        return descriptions

    def read_tags(self, tags, start_time, stop_time, ts, read_type=ReaderType.INT):
        """Reads values for the specified [tags] from the IMS server for the
        time interval from [start_time] to [stop_time] in intervals [ts].

        The interval [ts] can be specified as pd.Timedelta or as an integer,
        in which case it will be interpreted as seconds.

        Default value for [read_type] is ReaderType.INT, which interpolates
        the raw data.
        All possible values for read_type are defined in the ReaderType class,
        which can be imported as follows:
            from utils import ReaderType

        Values for Readertype.* that should work are:
            INT, MIN, MAX, RNG, AVG, VAR and STD
        """
        if isinstance(tags, str):
            tags = [tags]
        if read_type == ReaderType.SAMPLED and len(tags) > 1:
            raise RuntimeError(
                "Unable to read raw/sampled data for multiple tags since they don't "
                "share time vector"
            )
        start_time = datestr_to_datetime(start_time, tz=self.tz)
        stop_time = datestr_to_datetime(stop_time, tz=self.tz)
        if not isinstance(ts, pd.Timedelta):
            ts = pd.Timedelta(ts, unit="s")

        cols = []
        for tag in tags:
            cols.append(
                self._read_single_tag(
                    tag, start_time, stop_time, ts, read_type, cache=self.cache
                )
            )
        return pd.concat(cols, axis=1)
