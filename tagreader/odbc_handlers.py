import os
import pyodbc
import pandas as pd

from .utils import logging, winreg, find_registry_key, ReaderType

logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def list_aspen_servers():
    server_list = []
    reg_adsa = winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        r"Software\AspenTech\ADSA\Caches\AspenADSA\\" + os.getlogin(),
    )
    num_servers, _, _ = winreg.QueryInfoKey(reg_adsa)
    for i in range(0, num_servers):
        server_list.append(winreg.EnumKey(reg_adsa, i))
    return server_list


def list_pi_servers():
    server_list = []
    reg_key = winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\PISystem\PI-SDK"
    )
    reg_key = find_registry_key(reg_key, "ServerHandles")
    if reg_key is None:
        reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\PISystem\PI-SDK")
        reg_key = find_registry_key(reg_key, "ServerHandles")
    if reg_key is not None:
        num_servers, _, _ = winreg.QueryInfoKey(reg_key)
        for i in range(0, num_servers):
            server_list.append(winreg.EnumKey(reg_key, i))
    return server_list


class AspenHandlerODBC:
    def __init__(self, host=None, port=None, options={}):
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None
        self._max_rows = options.get("max_rows", 100000)
        self._field_engineering_unit = ["Engineering Unit", "IP_ENG_UNITS"]

    @staticmethod
    def generate_connection_string(host, port, max_rows=100000):
        return f"DRIVER={{AspenTech SQLPlus}};HOST={host};PORT={port};READONLY=Y;MAXROWS={max_rows}"  # noqa: E501

    @staticmethod
    def generate_search_query(tag=None, desc=None):
        if desc is not None:
            raise NotImplementedError("Description search not implemented")
        query = "SELECT name FROM all_records WHERE name LIKE '{tag}'".format(
            tag=tag.replace("*", "%")
        )
        return query

    @staticmethod
    def generate_read_query(
        tag, mapping, start_time, stop_time, sample_time, read_type
    ):
        timecast_format_query = "%d-%b-%y %H:%M:%S"  # 05-jan-18 14:00:00
        timecast_format_output = "YYYY-MM-DD HH:MI:SS"

        if read_type in [
            ReaderType.COUNT,
            ReaderType.GOOD,
            ReaderType.BAD,
            ReaderType.TOTAL,
            ReaderType.SUM,
            ReaderType.RAW,
            ReaderType.SNAPSHOT,
            ReaderType.SHAPEPRESERVING,
        ]:
            raise (NotImplementedError)

        sample_time = sample_time.seconds
        if ReaderType.SAMPLED == read_type:
            sample_time = 0
        else:
            if sample_time <= 0:
                raise NotImplementedError
                # sample_time = (stop_time-start_time).totalseconds

        request_num = {
            ReaderType.SAMPLED: 4,
            ReaderType.SHAPEPRESERVING: 3,
            ReaderType.INT: 6,
            ReaderType.NOTGOOD: 0,
            ReaderType.SNAPSHOT: None,
        }.get(read_type, 1)

        from_column = {
            ReaderType.SAMPLED: "history",
            ReaderType.SHAPEPRESERVING: "history",
            ReaderType.INT: "history",
            ReaderType.SNAPSHOT: '"' + str(tag) + '"',
        }.get(read_type, "aggregates")
        # For RAW: historyevent?
        # Ref https://help.sap.com/saphelp_pco151/helpdata/en/4c/72e34ee631469ee10000000a15822d/content.htm?no_cache=true  # noqa: E501

        value = {
            ReaderType.MIN: "min",
            ReaderType.MAX: "max",
            ReaderType.RNG: "rng",
            ReaderType.AVG: "avg",
            ReaderType.VAR: "var",
            ReaderType.STD: "std",
            ReaderType.GOOD: "good",
            ReaderType.NOTGOOD: "ng",
            ReaderType.TOTAL: "sum",
            ReaderType.SNAPSHOT: "IP_INPUT_VALUE",
        }.get(read_type, "value")

        ts_actual = {
            ReaderType.MIN: "ts_middle",
            ReaderType.MAX: "ts_middle",
            ReaderType.RNG: "ts_middle",
            ReaderType.AVG: "ts_middle",
            ReaderType.VAR: "ts_middle",
            ReaderType.STD: "ts_middle",
            ReaderType.GOOD: "ts_middle",
            ReaderType.NOTGOOD: "ts_middle",
            ReaderType.TOTAL: "ts_middle",
            ReaderType.SNAPSHOT: "IP_INPUT_TIME",
        }.get(read_type, "ts")

        query = [
            f'SELECT CAST({ts_actual} AS CHAR FORMAT {timecast_format_output!r}) AS "time",',  # noqa: E501
            f'{value} AS "value" FROM {from_column}',
        ]
        # Query is actually slower without cast(time) regardless of whether post-fetch
        # date parsing is enabled or not.
        # SQL_QUERY = 'SELECT ISO8601({ts_actual}) AS "timestamp"' is even slower

        if ReaderType.SNAPSHOT != read_type:
            start = start_time.strftime(timecast_format_query)
            stop = stop_time.strftime(timecast_format_query)
            query.extend([f"WHERE name = {tag!r}"])
            if mapping is not None:
                query.extend([f"AND FIELD_ID = FT({mapping!r})"])
            query.extend(
                [
                    f"AND (ts BETWEEN {start!r} AND {stop!r})",
                    f"AND (request = {request_num}) AND (period = {sample_time*10})",
                    "ORDER BY ts",
                ]
            )

        return " ".join(query)

    def set_options(self, options):
        pass

    def connect(self):
        connection_string = self.generate_connection_string(self.host, self.port)
        # The default autocommit=False is not supported by PI odbc driver.
        self.conn = pyodbc.connect(connection_string, autocommit=True)
        self.cursor = self.conn.cursor()

    def search_tag(self, tag=None, desc=None):
        query = self.generate_search_query(tag, desc)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _get_tag_metadata(self, tag):
        return None

    def _get_tag_unit(self, tag):
        if isinstance(self._field_engineering_unit, str):
            query = f'SELECT "{self._field_engineering_unit}" FROM "{tag}"'
            # TODO: Add mapping
            self.cursor.execute(query)
            unit = self.cursor.fetchone()
        else:
            for field in self._field_engineering_unit:
                query = f'SELECT "{field}" FROM "{tag}"'  # TODO: Add mapping
                unit = None
                try:
                    self.cursor.execute(query)
                    unit = self.cursor.fetchone()
                except Exception as e:
                    logging.debug(e)
                if (
                    unit
                ):  # We discovered which field to use, so no need to check next time
                    self._field_engineering_unit = field
                    break
        if not unit:
            raise Exception(
                "Failed to fetch units. "
                f'I tried these fields: "{self._field_engineering_unit}"'  # noqa: E501
            )
        return unit[0]

    def _get_tag_description(self, tag):
        query = f'SELECT "Description" FROM "{tag}"'  # TODO: Add mapping
        self.cursor.execute(query)
        desc = self.cursor.fetchone()
        return desc[0]

    def read_tag(self, tag, start_time, stop_time, sample_time, read_type, metadata):
        (cleantag, mapping) = tag.split(";") if ";" in tag else (tag, None)
        map_historyvalue = None
        if mapping is not None:
            c = self.conn.cursor()
            c.execute(f'SELECT MAP_HistoryValue FROM "{mapping}"')
            map_historyvalue = c.fetchone()[0]
        query = self.generate_read_query(
            cleantag, map_historyvalue, start_time, stop_time, sample_time, read_type
        )
        # logging.debug(f'Executing SQL query {query!r}')
        df = pd.read_sql(
            query,
            self.conn,
            index_col="time",
            parse_dates={"time": {"format": "%Y-%m-%d %H:%M:%S"}},
        )
        if len(df) > len(
            df.index.unique()
        ):  # One hour repeated during transition from DST to standard time.
            df = df.tz_localize(start_time.tzinfo, ambiguous="infer")
        else:
            df = df.tz_localize(start_time.tzinfo)
        return df.rename(columns={"value": tag})


class PIHandlerODBC:
    def __init__(self, host=None, port=None, options={}):
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None
        self._max_rows = options.get("max_rows", 100000)
        # TODO: Find default das_server under
        # HKLM\SOFTWARE\Wow6432Node\PISystem\Analytics\InstallData/AFServer
        # It seems that is actually not possible anymore.
        # ws3099.statoil.net
        self._das_server = options.get("das_server", "piwebapi.equinor.com")

        # print(self._das_server)

    @staticmethod
    def generate_connection_string(host, port, das_server):
        return (
            f"DRIVER={{PI ODBC Driver}};Server={das_server};"
            "Trusted_Connection=Yes;Command Timeout=1800;Provider Type=PIOLEDB;"
            f'Provider String={{Data source={host.replace(".statoil.net", "")};'
            "Integrated_Security=SSPI;};"
        )

    @staticmethod
    def generate_search_query(tag=None, desc=None):
        query = ["SELECT tag, descriptor as description FROM pipoint.pipoint2 WHERE"]
        if tag is not None:
            query.extend(["tag LIKE '{tag}'".format(tag=tag.replace("*", "%"))])
        if tag is not None and desc is not None:
            query.extend(["AND"])
        if desc is not None:
            query.extend(
                ["descriptor LIKE '{desc}'".format(desc=desc.replace("*", "%"))]
            )
        return " ".join(query)

    @staticmethod
    def generate_read_query(
        tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
        timecast_format_query = "%d-%b-%y %H:%M:%S"  # 05-jan-18 14:00:00
        # timecast_format_output = "yyyy-MM-dd HH:mm:ss"

        if read_type in [
            ReaderType.COUNT,
            ReaderType.GOOD,
            ReaderType.BAD,
            ReaderType.TOTAL,
            ReaderType.SUM,
            ReaderType.RAW,
            ReaderType.SNAPSHOT,
            ReaderType.SHAPEPRESERVING,
        ]:
            raise (NotImplementedError)

        sample_time = sample_time.seconds
        if ReaderType.SAMPLED == read_type:
            sample_time = 0
        else:
            if sample_time <= 0:
                pass  # Fixme: Not implemented
                # sample_time = (stop_time-start_time).totalseconds

        source = {
            ReaderType.INT: "[piarchive]..[piinterp2]",
            ReaderType.MIN: "[piarchive]..[pimin]",
            ReaderType.MAX: "[piarchive]..[pimax]",
            ReaderType.RNG: "[piarchive]..[pirange]",
            ReaderType.AVG: "[piarchive]..[piavg]",
            ReaderType.VAR: "[piarchive]..[pistd]",
            ReaderType.STD: "[piarchive]..[pistd]",
            ReaderType.GOOD: "[piarchive]..[picount]",
            ReaderType.NOTGOOD: "[piarchive]..[picount]",
            ReaderType.TOTAL: "[piarchive]..[pitotal]",
            ReaderType.SUM: "[piarchive]..[pitotal]",
            ReaderType.COUNT: "[piarchive]..[picount]",
            ReaderType.SNAPSHOT: "[piarchive]..[pisnapshot]",
            ReaderType.SHAPEPRESERVING: "[piarchive]..[piplot]",
        }.get(read_type, "[piarchive]..[picomp2]")

        if ReaderType.VAR == read_type:
            query = ["SELECT POWER(CAST(value as FLOAT32), 2)"]
        elif ReaderType.GOOD == read_type:
            query = ["SELECT CAST(pctgood as FLOAT32)"]
        elif ReaderType.BAD == read_type:
            query = ["SELECT 100-CAST(pctgood as FLOAT32)"]
        else:
            query = ["SELECT CAST(value as FLOAT32)"]

        # query.extend([f"AS value, FORMAT(time, '{timecast_format_output}') AS timestamp FROM {source} WHERE tag='{tag}'"])  # noqa E501
        query.extend(
            [f"AS value, __utctime AS timestamp FROM {source} WHERE tag='{tag}'"]
        )

        if ReaderType.SNAPSHOT != read_type:
            start = start_time.strftime(timecast_format_query)
            stop = stop_time.strftime(timecast_format_query)
            query.extend([f"AND (time BETWEEN '{start}' AND '{stop}')"])

        if ReaderType.GOOD == read_type:
            query.extend(["AND questionable = FALSE"])
        elif ReaderType.NOTGOOD == read_type:
            query.extend(["AND questionable = TRUE"])
        elif ReaderType.SHAPEPRESERVING == read_type:
            query.extend(
                [
                    f"AND (intervalcount = {int((stop_time-start_time).seconds/sample_time)})" # noqa E501
                ]
            )
        elif ReaderType.RAW == read_type:
            pass
        else:
            query.extend([f"AND (timestep = '{sample_time}s')"])

        if ReaderType.SNAPSHOT != read_type:
            query.extend(["ORDER BY timestamp"])

        return " ".join(query)

    def set_options(self, options):
        pass

    def connect(self):
        connection_string = self.generate_connection_string(
            self.host, self.port, self._das_server
        )
        # The default autocommit=False is not supported by PI odbc driver.
        self.conn = pyodbc.connect(connection_string, autocommit=True)
        self.cursor = self.conn.cursor()

    def search_tag(self, tag=None, desc=None):
        query = self.generate_search_query(tag, desc)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _get_tag_metadata(self, tag):
        query = f"SELECT digitalset, engunits, descriptor FROM pipoint.pipoint2 WHERE tag='{tag}'"  # noqa E501
        self.cursor.execute(query)
        desc = self.cursor.description
        col_names = [col[0] for col in desc]
        metadata = dict(zip(col_names, self.cursor.fetchone()))
        return metadata

    def _get_tag_description(self, tag):
        query = f"SELECT descriptor FROM pipoint.pipoint2 WHERE tag='{tag}'"
        self.cursor.execute(query)
        desc = self.cursor.fetchone()
        return desc[0]

    def _get_tag_unit(self, tag):
        query = f"SELECT engunits FROM pipoint.pipoint2 WHERE tag='{tag}'"
        self.cursor.execute(query)
        unit = self.cursor.fetchone()
        return unit[0]

    def read_tag(
        self, tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
        query = self.generate_read_query(
            tag, start_time, stop_time, sample_time, read_type
        )
        # logging.debug(f'Executing SQL query {query!r}')
        df = pd.read_sql(
            query,
            self.conn,
            index_col="timestamp",
            parse_dates={"timestamp": {"format": "%Y-%m-%d %H:%M:%S"}},
        )
        df.index.name = "time"
        # One hour repeated during transition from DST to standard time:
        # if len(df) > len(df.index.unique()):
        #     df = df.tz_localize(start_time.tzinfo, ambiguous='infer')
        # else:
        #     df = df.tz_localize(start_time.tzinfo)
        # df = df.tz_localize(start_time.tzinfo)
        df = df.tz_localize("UTC").tz_convert(start_time.tzinfo)
        if len(metadata["digitalset"]) > 0:
            self.cursor.execute(
                f"SELECT code, offset FROM pids WHERE digitalset='{metadata['digitalset']}'"  # noqa E501
            )
            digitalset = self.cursor.fetchall()
            code = [x[0] for x in digitalset]
            offset = [x[1] for x in digitalset]
            df = df.replace(code, offset)
        # cols = tuple(['value', metadata['engunits'], metadata['descriptor']])
        # df.columns = cols
        # df.columns.names = ['Tag', 'Unit', 'Description']
        return df.rename(columns={"value": tag})
