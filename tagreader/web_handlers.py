import datetime
import logging
import re
import urllib
import warnings
from typing import Union

import numpy as np
import pandas as pd
import pytz
import requests
from requests_kerberos import OPTIONAL, HTTPKerberosAuth

from .utils import ReaderType, is_mac, is_windows, urljoin

# Requests will use simplejson if it has been installed, so handle both errors here
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json.decoder import JSONDecodeError

logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)

internal = False


def get_verifySSL():
    if is_windows() or is_mac():
        return True
    return "/etc/ssl/certs/ca-bundle.trust.crt"


def get_auth_pi():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def get_url_pi():
    return r"https://piwebapi.equinor.com/piwebapi"


def get_auth_aspen():
    if internal:
        return HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    else:
        from .BearerAuth import BearerAuth

        tenantID = '3aa4a235-b6e2-48d5-9195-7fcf05b459b0'
        clientID = '7adaaa99-897f-428c-8a5f-4053db565b32'
        scopes = [
            "https://ewepwapa1pep04-statoilsrm.msappproxy.net/ProcessExplorer/ProcessData//user_impersonation"]
        auth = BearerAuth.get_bearer_token(
            tenantID=tenantID, clientID=clientID, scopes=scopes, verbose=True)
        return auth


def get_url_aspen():
    if internal:
        # internal url (redirects to url including AtProcessDataREST.dll)
        return r"https://aspenone.api.equinor.com"
    else:
        return r"https://ewepwapa1pep04-statoilsrm.msappproxy.net/ProcessExplorer/ProcessData/AtProcessDataREST.dll"


def list_aspenone_sources(url=None, auth=None, verifySSL=None):
    if url is None:
        url = get_url_aspen()

    if auth is None:
        auth = get_auth_aspen()

    if verifySSL is False:
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning
        )
    elif verifySSL is None:
        verifySSL = get_verifySSL()

    url_ = urljoin(url, "DataSources")
    params = {"service": "ProcessData", "allQuotes": 1}

    res = requests.get(url_, params=params, auth=auth, verify=verifySSL)
    if res.status_code == 200:
        try:
            source_list = [r["n"] for r in res.json()["data"] if r["t"] == "IP21"]
            return source_list
        except JSONDecodeError:
            print("Did not return json")
    elif res.status_code == 404:
        print("Not found")
    elif res.status_code == 401:
        print("Not authorized")
    res.raise_for_status()


def list_piwebapi_sources(url=None, auth=None, verifySSL=None):
    if url is None:
        url = get_url_pi()

    if auth is None:
        auth = get_auth_pi()

    if verifySSL is False:
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning
        )
    elif verifySSL is None:
        verifySSL = get_verifySSL()

    url_ = urljoin(url, "dataservers")
    res = requests.get(url_, auth=auth, verify=verifySSL)

    if res.status_code == 200:
        try:
            source_list = [r["Name"] for r in res.json()["Items"]]
            return source_list
        except JSONDecodeError:
            print("Did not return json")
    elif res.status_code == 404:
        print("Not found")
    elif res.status_code == 401:
        print("Not authorized")
    res.raise_for_status()


class AspenHandlerWeb:
    def __init__(
        self,
        datasource=None,
        url=None,
        auth=None,
        verifySSL=None,
        options={},
    ):
        self._max_rows = options.get("max_rows", 100000)
        if url is None:
            url = get_url_aspen()
        self.datasource = datasource
        self.base_url = url
        self.session = requests.Session()
        self.session.auth = auth if auth is not None else get_auth_aspen()
        if verifySSL is False:
            requests.packages.urllib3.disable_warnings(
                requests.packages.urllib3.exceptions.InsecureRequestWarning
            )
        self.session.verify = verifySSL if verifySSL is not None else get_verifySSL()
        self._connection_string = ""  # Used for raw SQL queries

    @staticmethod
    def generate_connection_string(host, *_, **__):
        raise NotImplementedError

    @staticmethod
    def generate_search_query(tag=None, desc=None, datasource=None):
        if not datasource:
            raise ValueError("Data source is required argument")
        # Aspen Web API expects single space instead of consecutive spaces.
        tag = " ".join(tag.split())
        params = {"datasource": datasource, "tag": tag, "max": 100, "getTrendable": 0}
        return params

    def generate_read_query(
        self,
        tagname,
        mapname,
        start_time,
        stop_time,
        sample_time,
        read_type,
        metadata=None,
    ):
        # Maxpoints is used for Actual (raw) and Bestfit (shapepreserving).
        # Probably need to handle this in another way at some point
        maxpoints = self._max_rows
        stepped = 0
        outsiders = 0

        rt = {
            ReaderType.RAW: 0,
            ReaderType.SHAPEPRESERVING: 2,
            ReaderType.INT: 1,
            ReaderType.MIN: 14,
            ReaderType.MAX: 13,
            ReaderType.AVG: 12,
            ReaderType.VAR: 18,
            ReaderType.STD: 17,
            ReaderType.RNG: 15,
            ReaderType.COUNT: -1,
            ReaderType.GOOD: 11,
            ReaderType.BAD: 10,
            ReaderType.TOTAL: -1,
            ReaderType.SUM: 16,
            ReaderType.SNAPSHOT: -1,
        }.get(read_type, -1)

        if read_type == ReaderType.SNAPSHOT:
            if stop_time is not None:
                use_current = 0
                end_time = int(stop_time.timestamp()) * 1000
            else:
                use_current = 1
                end_time = 0

            query = f'<Q f="d" allQuotes="1" rt="{end_time}" uc="{use_current}">'
        else:
            query = '<Q f="d" allQuotes="1">'

        query += "<Tag>" f"<N><![CDATA[{tagname}]]></N>"

        if mapname:
            query += f"<M><![CDATA[{mapname}]]></M>"

        query += f"<D><![CDATA[{self.datasource}]]></D>" "<F><![CDATA[VAL]]></F>"

        if read_type == ReaderType.SNAPSHOT:
            query += "<VS>1</VS>"
        else:
            query += (
                "<HF>0</HF>"  # History format: 0=Raw, 1=RecordAsString
                f"<St>{int(start_time.timestamp())*1000}</St>"
                f"<Et>{int(stop_time.timestamp())*1000}</Et>"
                f"<RT>{rt}</RT>"
            )
        if read_type in [ReaderType.RAW, ReaderType.SHAPEPRESERVING]:
            query += f"<X>{maxpoints}</X>"
        if read_type not in [ReaderType.INT, ReaderType.SNAPSHOT]:
            query += f"<O>{outsiders}</O>"
        if read_type not in [ReaderType.RAW]:
            query += f"<S>{stepped}</S>"
        if read_type not in [
            ReaderType.RAW,
            ReaderType.SHAPEPRESERVING,
            ReaderType.SNAPSHOT,
        ]:
            query += (
                f"<P>{int(sample_time.total_seconds())}</P>"
                "<PU>3</PU>"  # Period Unit: 0=day, 1=hour, 2=min, 3=sec
            )
        if read_type not in [
            ReaderType.RAW,
            ReaderType.SHAPEPRESERVING,
            ReaderType.INT,
            ReaderType.SNAPSHOT,
        ]:
            query += (
                # Method: 0=integral, 2=value, 3=integral complete, 4=value complete
                # Value averages all actual values inside interval
                # Integral time-weighs values
                "<AM>0</AM>"
                "<AS>0</AS>"  # Start: 0=Start of day, 1=Start of time
                "<AA>0</AA>"  # Anchor: 0=Begin, 1=Middle, 2=End
                # TODO: Unify anchor selection among all handlers
                "<DSA>0</DSA>"  # DS Adjust: 0=False, 1=True
            )
        query += "</Tag></Q>"

        return query

    def verify_connection(self, datasource):
        """Connects to the URL and verifies that the provided data source exists.

        :param datasource: Data source to look for
        :type datasource: String
        :raises ConnectionError: If connection fails
        :return: True if datasource exists, False if not.
        :rtype: Bool
        """
        url = urljoin(self.base_url, "Datasources")
        params = {"service": "ProcessData", "allQuotes": 1}
        if not self.session.verify:
            requests.packages.urllib3.disable_warnings(
                requests.packages.urllib3.exceptions.InsecureRequestWarning
            )

        res = self.session.get(url, params=params)
        res.raise_for_status()
        j = res.json()
        for item in j["data"]:
            if item["n"] == datasource:
                return True
        return False

    def connect(self):
        try:
            self.verify_connection(self.datasource)
        except requests.ConnectionError:
            raise ConnectionError(
                f"Not able to connect to {self.base_url}. Check network connection."
            ) from None

    @staticmethod
    def split_tagmap(tagmap):
        return tuple(tagmap.split(";") if ";" in tagmap else (tagmap, None))

    def generate_get_unit_query(self, tag):
        tagname, _ = self.split_tagmap(tag)
        parts = [
            '<Q allQuotes="1" attributeData="1">',
            "<Tag>",
            f"<N><![CDATA[{tagname}]]></N>",
            "<T>0</T>",  # What is this?
            f"<G><![CDATA[{tagname}]]></G>",
            f"<D><![CDATA[{self.datasource}]]></D>",
            "<AL>",
            # Units only or MAP_Units only: ATCAI=>3. Both: Units=psig, MAP_Units=3
            "<A>Units</A>",
            "<A>MAP_Units</A>",
            "<VS>0</VS>",  # What is this?
            "</AL>",
            "</Tag>",
            "</Q>",
        ]
        return "".join(parts)

    def generate_get_map_query(self, tagname):
        parts = [
            '<Q allQuotes="1" categoryInfo="1">',
            "<Tag>",
            f"<N><![CDATA[{tagname}]]></N>",
            "<T>0</T>",  # What is this?
            f"<G><![CDATA[{tagname}]]></G>",
            f"<D><![CDATA[{self.datasource}]]></D>",
            "</Tag>",
            "</Q>",
        ]
        return "".join(parts)

    def _get_maps(self, tagname):
        params = self.generate_get_map_query(tagname)
        url = urljoin(self.base_url, "TagInfo")
        res = self.session.get(url, params=params)
        res.raise_for_status()
        j = res.json()

        if "tags" not in j["data"]:
            return {}

        ret = {}
        for item in j["data"]["tags"][0]["categories"][0]["ta"]:
            ret[item["m"]] = True if item["d"] == "True" else False
        return ret

    def _get_default_mapname(self, tagname):
        (tagname, _) = self.split_tagmap(tagname)
        allmaps = self._get_maps(tagname)
        for k, v in allmaps.items():
            if v:
                return k

    def search(self, tag=None, desc=None):
        if tag is None:
            raise ValueError("Tag is a required argument")

        tag = tag.replace("%", "*") if isinstance(tag, str) else None
        # Prepare for regex
        desc = desc.replace("%", "*") if isinstance(desc, str) else None
        desc = desc.replace("*", ".*") if isinstance(desc, str) else None

        params = self.generate_search_query(tag, desc, self.datasource)
        # Ensure space is encoded as "%20" instead of default "+" and leave asterisks
        # unencoded. Otherwise searches for tags containing spaces break, as do wildcard
        # searches.
        encoded_params = urllib.parse.urlencode(
            params, safe="*", quote_via=urllib.parse.quote
        )
        url = urljoin(self.base_url, "Browse?")
        url += encoded_params
        res = self.session.get(url)

        res.raise_for_status()
        j = res.json()

        if "tags" not in j["data"]:
            return []

        ret = []
        for item in j["data"]["tags"]:
            tagname = item["t"]
            description = self._get_tag_description(tagname)
            ret.append((tagname, description))

        if not desc:
            return ret

        r = re.compile(desc)
        ret = [x for x in ret if r.search(x[1])]
        return ret

    def _get_tag_metadata(self, tag):
        return {}  # FIXME

    def _get_tag_unit(self, tag):
        query = self.generate_get_unit_query(tag)
        url = urljoin(self.base_url, "TagInfo")
        res = self.session.get(url, params=query)
        res.raise_for_status()
        j = res.json()
        try:
            attrdata = j["data"]["tags"][0]["attrData"]
        except Exception:
            print(f"Error. I got this: {j}")
            raise KeyError
        unit = ""
        for a in attrdata:
            if a["g"] == "Units":
                unit = a["samples"][0]["v"]
                break
        return unit

    def generate_get_description_query(self, tag):
        tagname, _ = self.split_tagmap(tag)
        parts = [
            '<Q allQuotes="1" attributeData="1">',
            "<Tag>",
            f"<N><![CDATA[{tagname}]]></N>",
            "<T>0</T>",  # What is this?
            f"<G><![CDATA[{tagname}]]></G>",
            f"<D><![CDATA[{self.datasource}]]></D>",
            "<AL>",
            "<A>DSCR</A>",
            "<VS>0</VS>",  # What is this?
            "</AL>",
            "</Tag>",
            "</Q>",
        ]
        return "".join(parts)

    def _get_tag_description(self, tag):
        query = self.generate_get_description_query(tag)
        url = urljoin(self.base_url, "TagInfo")
        res = self.session.get(url, params=query)
        res.raise_for_status()
        j = res.json()
        try:
            desc = j["data"]["tags"][0]["attrData"][0]["samples"][0]["v"]
        except Exception:
            desc = ""
        return desc

    def read_tag(
        self,
        tag,
        start_time,
        stop_time,
        sample_time,
        read_type,
        metadata=None,
        get_status=False,
    ):
        if read_type not in [
            ReaderType.INT,
            ReaderType.MIN,
            ReaderType.MAX,
            ReaderType.RNG,
            ReaderType.AVG,
            ReaderType.VAR,
            ReaderType.STD,
            ReaderType.SNAPSHOT,
            ReaderType.RAW,
        ]:
            raise NotImplementedError

        if read_type == ReaderType.SNAPSHOT:
            url = urljoin(self.base_url, "Attribute")
        else:
            url = urljoin(self.base_url, "History")

        # Actual and bestfit read types allow specifying maxpoints.
        # Aggregate reads limit to 10 000 points and issue a moredata-token.
        # TODO: May need to look into using this later - most likely more
        # efficient than creating new query starting at previous stoptime.
        # Interpolated reads return error message if more than 100 000 points,
        # so we need to limit the range. Note -1 because INT normally includes
        # both start and end time.
        if read_type == ReaderType.INT:
            stop_time = min(stop_time, start_time + sample_time * (self._max_rows - 1))

        tagname, mapname = self.split_tagmap(tag)

        params = self.generate_read_query(
            tagname, mapname, start_time, stop_time, sample_time, read_type
        )

        res = self.session.get(url, params=params)
        res.raise_for_status()

        if len(res.text) == 0:  # res.text='' for timestamps in future
            return pd.DataFrame(columns=[tag])

        try:
            j = res.json()
        except JSONDecodeError:
            # AspenOne sometimes returns completely and utterly invalid -nan.
            # Since json/simplejson has no mechanism to handle this, we need to
            # pre-process
            import json

            txt = res.text.replace('"v":nan', '"v":NaN').replace('"v":-nan', '"v":NaN')
            j = json.loads(txt)

        if "er" in j["data"][0]["samples"][0]:
            warnings.warn(j["data"][0]["samples"][0]["es"])
            return pd.DataFrame(columns=[tag])
        if get_status:
            # The "l" field maps 1:1 to ODBC status field values 0, 1, 2, 4, 5, 6
            df = (
                pd.DataFrame.from_dict(j["data"][0]["samples"])
                .drop(labels=["s", "V"], axis="columns")
                .rename(columns={"t": "Timestamp", "v": "Value", "l": "Status"})
            )
        else:
            df = (
                pd.DataFrame.from_dict(j["data"][0]["samples"])
                .drop(labels=["l", "s", "V"], axis="columns")
                .rename(columns={"t": "Timestamp", "v": "Value"})
            )

        # Ensure non-numericals like "1.#QNAN" are returned as NaN
        df["Value"] = pd.to_numeric(df.Value, errors="coerce")

        df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms", origin="unix")
        df = df.set_index("Timestamp", drop=True).tz_localize("UTC")
        df.index.name = "time"
        return df.rename(columns={"Value": tag, "Status": tag + "::status"})

    @staticmethod
    def generate_sql_query(
        connection_string=None, datasource=None, query=None, max_rows=100000
    ):
        if connection_string is not None:
            connstr = f'<SQL c="{connection_string}" m="{max_rows}" to="30" s="1">'
        else:
            connstr = (
                f'<SQL t="SQLplus" ds="{datasource}" '
                'dso="CHARINT=N;CHARFLOAT=N;CHARTIME=N;CONVERTERRORS=N" '
                f'm="{max_rows}" to="30" s="1">'
            )

        connstr += f"<![CDATA[{query}]]></SQL>"
        return connstr

    def initialize_connectionstring(
        self, host=None, port=10014, connection_string=None
    ):
        if connection_string:
            self._connection_string = connection_string
        else:
            self._connection_string = (
                f"DRIVER=AspenTech SQLPlus;HOST={host};"
                f"PORT={port};CHARINT=N;CHARFLOAT=N;"
                "CHARTIME=N;CONVERTERRORS=N"
            )

    def query_sql(self, query: str, parse: bool = True) -> Union[str, pd.DataFrame]:
        url = urljoin(self.base_url, "SQL")
        if self._connection_string is None:
            params = self.generate_sql_query(
                datasource=self.datasource, query=query, max_rows=self._max_rows
            )
        else:
            params = self.generate_sql_query(
                connection_string=self._connection_string,
                query=query,
                max_rows=self._max_rows,
            )
        res = self.session.get(url, params=params)
        res.raise_for_status()
        # For now just return result as text regardless of value of parse
        if parse:
            raise NotImplementedError(
                "Use parse=False to receive and handle text result instead"
            )
        return res.text


class PIHandlerWeb:
    def __init__(
        self,
        url=None,
        datasource=None,
        auth=None,
        verifySSL=None,
        options={},
    ):
        self._max_rows = options.get("max_rows", 10000)
        if url is None:
            url = get_url_pi()
        self.base_url = url
        self.datasource = datasource
        self.session = requests.Session()
        self.session.auth = auth if auth is not None else get_auth_pi()
        if verifySSL is False:
            requests.packages.urllib3.disable_warnings(
                requests.packages.urllib3.exceptions.InsecureRequestWarning
            )
        self.session.verify = verifySSL if verifySSL is not None else get_verifySSL()
        self.webidcache = {}

    @staticmethod
    def _time_to_UTC_string(time):
        timecast_format_query = "%d-%b-%y %H:%M:%S"
        if isinstance(time, datetime.datetime):
            return time.astimezone(pytz.UTC).strftime(timecast_format_query)
        else:
            return time.tz_convert("UTC").strftime(timecast_format_query)

    @staticmethod
    def generate_connection_string(host, *_, **__):
        raise NotImplementedError

    @staticmethod
    def escape(s):
        # https://techsupport.osisoft.com/Documentation/PI-Web-API/help/topics/search-queries.html  # noqa: E501
        return s.translate(
            str.maketrans(
                {
                    "+": r"\+",
                    "-": r"\-",
                    "&": r"\&",
                    "|": r"\|",
                    "(": r"\(",
                    ")": r"\)",
                    "{": r"\{",
                    "}": r"\}",
                    "[": r"\[",
                    "]": r"\]",
                    "^": r"\^",
                    '"': r"\"",
                    "~": r"\~",
                    # Do not escape wildcard
                    # "*": r"\*",
                    ":": r"\:",
                    "\\": r"\\",
                    " ": r"\ ",
                }
            )
        )

    @staticmethod
    def generate_search_query(tag=None, desc=None, datasource=None):
        q = []
        if tag is not None:
            q.extend([f"name:{PIHandlerWeb.escape(tag)}"])
        if desc is not None:
            q.extend([f"description:{PIHandlerWeb.escape(desc)}"])
        query = " AND ".join(q)
        params = {"q": f"{query}"}

        if datasource is not None:
            params["scope"] = f"pi:{datasource}"

        return params

    def generate_read_query(
        self,
        tag,
        start_time,
        stop_time,
        sample_time,
        read_type,
        metadata=None,
        get_status=False,
    ):

        if read_type in [
            ReaderType.COUNT,
            ReaderType.GOOD,
            ReaderType.BAD,
            ReaderType.TOTAL,
            ReaderType.SUM,
            ReaderType.SHAPEPRESERVING,
        ]:
            raise NotImplementedError

        webid = tag

        seconds = 0
        if read_type != ReaderType.SNAPSHOT:
            seconds = int(sample_time.total_seconds())

        get_action = {
            ReaderType.INT: "interpolated",
            ReaderType.RAW: "recorded",
            ReaderType.SNAPSHOT: "value",
            ReaderType.SHAPEPRESERVING: "plot",
        }.get(read_type, "summary")

        url = f"streams/{webid}/{get_action}"
        params = {}

        if read_type != ReaderType.SNAPSHOT:
            params["startTime"] = self._time_to_UTC_string(start_time)
            params["endTime"] = self._time_to_UTC_string(stop_time)
            params["timeZone"] = "UTC"
        elif read_type == ReaderType.SNAPSHOT and stop_time is not None:
            params["time"] = self._time_to_UTC_string(stop_time)
            params["timeZone"] = "UTC"

        summary_type = {
            ReaderType.MIN: "Minimum",
            ReaderType.MAX: "Maximum",
            ReaderType.AVG: "Average",
            ReaderType.VAR: "StdDev",
            ReaderType.STD: "StdDev",
            ReaderType.RNG: "Range",
        }.get(read_type, None)

        if ReaderType.INT == read_type:
            params["interval"] = f"{seconds}s"
        elif summary_type:
            params["summaryType"] = summary_type
            params["summaryDuration"] = f"{seconds}s"

        if self._is_summary(read_type):
            params["selectedFields"] = "Links;Items.Value.Timestamp;Items.Value.Value"
            if get_status:
                params["selectedFields"] += (
                    ";Items.Value.Good"
                    ";Items.Value.Questionable"
                    ";Items.Value.Substituted"
                )
        elif read_type in [ReaderType.INT, ReaderType.RAW, ReaderType.SHAPEPRESERVING]:
            params["selectedFields"] = "Links;Items.Timestamp;Items.Value"
            if get_status:
                params[
                    "selectedFields"
                ] += ";Items.Good;Items.Questionable;Items.Substituted"
        elif read_type == ReaderType.SNAPSHOT:
            params["selectedFields"] = "Timestamp;Value"
            if get_status:
                params["selectedFields"] += ";Good;Questionable;Substituted"

        if read_type == ReaderType.RAW:
            params["maxCount"] = self._max_rows

        return (url, params)

    def verify_connection(self, datasource):
        """Connects to the URL and verifies that the provided data source exists.

        :param source: Data source to look for
        :type source: String
        :raises ConnectionError: If connection fails
        :return: True if data source exists, False if not.
        :rtype: Bool
        """
        url = urljoin(self.base_url, "dataservers")
        res = self.session.get(url)
        res.raise_for_status()
        j = res.json()
        for item in j["Items"]:
            if item["Name"] == datasource:
                return True
        return False

    def connect(self):
        try:
            self.verify_connection(self.datasource)
        except requests.ConnectionError:
            raise ConnectionError(
                f"Not able to connect to {self.base_url}. Check network connection."
            ) from None

    def search(self, tag=None, desc=None):
        params = self.generate_search_query(tag, desc, self.datasource)
        url = urljoin(self.base_url, "search", "query")
        done = False
        ret = []
        while not done:
            res = self.session.get(url, params=params)

            res.raise_for_status()
            j = res.json()
            for item in j["Items"]:
                description = item["Description"] if "Description" in item else ""
                ret.append((item["Name"], description))
            next_start = int(j["Links"]["Next"].split("=")[-1])
            if int(j["Links"]["Last"].split("=")[-1]) >= next_start:
                params["start"] = next_start
            else:
                done = True
        return ret

    def _get_tag_metadata(self, tag):
        return {}  # FIXME

    def _get_tag_unit(self, tag):
        webid = self.tag_to_webid(tag)
        if webid is None:
            return None
        url = urljoin(self.base_url, "points", webid)
        res = self.session.get(url)
        res.raise_for_status()
        j = res.json()
        unit = j["EngineeringUnits"]
        return unit

    def _get_tag_description(self, tag):
        webid = self.tag_to_webid(tag)
        if webid is None:
            return None
        url = urljoin(self.base_url, "points", webid)
        res = self.session.get(url)
        res.raise_for_status()
        j = res.json()
        description = j["Descriptor"]
        return description

    def tag_to_webid(self, tag):
        """Given a tag, returns the WebId.

        :param tag: The tag
        :type tag: str
        :raises ConnectionError: If connection or query fails
        :return: WebId
        :rtype: str
        """
        if tag not in self.webidcache:
            params = self.generate_search_query(tag=tag, datasource=self.datasource)
            params["fields"] = "name;webid"
            url = urljoin(self.base_url, "search", "query")
            res = self.session.get(url, params=params)
            res.raise_for_status()
            j = res.json()

            if len(j["Errors"]) > 0:
                msg = f"Received error from server when searching for WebId for {tag}: {j['Errors']}"  # noqa: E501
                raise ValueError(msg)

            if len(j["Items"]) > 1:
                # Compare elements and if same, return the first
                first = j["Items"][0]
                for item in j["Items"][1:]:
                    if item != first:
                        raise AssertionError(
                            f"Received {len(j['Items'])} results when trying to find unique WebId for {tag}."  # noqa: E501
                        )
            elif len(j["Items"]) == 0:
                warnings.warn(f"Tag {tag} not found")
                return None

            webid = j["Items"][0]["WebId"]
            self.webidcache[tag] = webid
        return self.webidcache[tag]

    @staticmethod
    def _is_summary(read_type):
        if read_type in [
            ReaderType.AVG,
            ReaderType.MIN,
            ReaderType.MAX,
            ReaderType.RNG,
            ReaderType.STD,
            ReaderType.VAR,
        ]:
            return True
        return False

    def read_tag(
        self,
        tag,
        start_time=None,
        stop_time=None,
        sample_time=None,
        read_type=ReaderType.INTERPOLATED,
        metadata=None,
        get_status=False,
    ):
        webid = self.tag_to_webid(tag)
        if not webid:
            return pd.DataFrame()

        (url, params) = self.generate_read_query(
            webid, start_time, stop_time, sample_time, read_type, get_status=get_status
        )
        url = urljoin(self.base_url, url)
        res = self.session.get(url, params=params)
        res.raise_for_status()

        j = res.json()
        if read_type == ReaderType.SNAPSHOT:
            df = pd.DataFrame.from_dict([j])
        else:
            # Summary (aggregated) data and DigitalSets return Value as dict
            df = pd.json_normalize(data=j, record_path="Items")

        # Can happen for RAW reads w/o data in interval
        if df.empty:
            return df

        # Summary data, digitalset or invalid data
        if "Value" not in df.columns:
            # Either digitalset or invalid data. Set invalid to NaN
            if "Value.Name" in df.columns:
                df.loc[df["Value.Name"] == "No Data", "Value.Value"] = np.nan
            # Replaced below replacement test with above when adding get_status
            # since we should avoid getting Good when get_status == False
            # Value.Name can also be the name of the digitalset, e.g. "Active"
            # Alternative: df["Value.IsSystem"] == True since it seems to be False
            # for digitalsets?
            #    df.loc[df.Good == False, "Value.Value"] = np.nan  # noqa E712
            df = df.rename(
                columns={
                    "Value.Value": "Value",
                    "Value.Timestamp": "Timestamp",
                    "Value.Good": "Good",
                    "Value.Questionable": "Questionable",
                    "Value.Substituted": "Substituted",
                }
            )

        df = df.filter(["Timestamp", "Value", "Good", "Questionable", "Substituted"])

        try:
            if read_type == ReaderType.RAW or read_type == ReaderType.SNAPSHOT:
                # Sub-second timestamps are common
                df["Timestamp"] = pd.to_datetime(
                    df["Timestamp"], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True
                )
            else:
                # Sub-second timestamps are uncommon
                df["Timestamp"] = pd.to_datetime(
                    df["Timestamp"], format="%Y-%m-%dT%H:%M:%SZ", utc=True
                )
        except ValueError:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)

        if read_type == ReaderType.VAR:
            df["Value"] = df["Value"] ** 2

        df = df.set_index("Timestamp", drop=True)
        df.index.name = "time"
        # df = df.tz_localize("UTC")

        # Correct weird bug in PI Web API where MAX timestamps end of interval while
        # all the other summaries stamp start of interval by shifting all timestamps
        # one interval down.
        if read_type == ReaderType.MAX and df.index[0] > start_time:
            df.index = df.index - sample_time

        if get_status:
            df["Status"] = (
                # Values are boolean, but no need to do .astype(int)
                df["Questionable"] +
                2 * (1 - df["Good"]) +
                4 * df["Substituted"]
            )
            df = df.drop(columns=["Good", "Questionable", "Substituted"])

        return df.rename(columns={"Value": tag, "Status": tag + "::status"})

    def query_sql(self, query: str, parse: bool = True) -> pd.DataFrame:
        raise NotImplementedError
