import requests
import urllib
import re
import pandas as pd

from requests_kerberos import HTTPKerberosAuth, OPTIONAL

from .utils import (
    logging,
    ReaderType,
    urljoin,
)

logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def get_auth_pi():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def get_auth_aspen():
    return HTTPKerberosAuth(service="HTTPS")


def list_aspen_servers(
    url=r"ws2679.statoil.net/ProcessData/AtProcessDataREST.dll",
    auth=get_auth_aspen(),
    verifySSL=True,
):
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url_ = urljoin(url, "DataSources")
    params = {"service": "ProcessData", "allQuotes": 1}

    res = requests.get(url_, params=params, auth=auth, verify=verifySSL)
    if res.status_code == 200:
        server_list = [r["n"] for r in res.json()["data"] if r["t"] == "IP21"]
        return server_list
    elif res.status_code == 404:
        print("Not found")
    elif res.status_code == 401:
        print("Not authorized")


def list_pi_servers(url=r"https://piwebapi.equinor.com/piwebapi", auth=get_auth_pi()):
    url_ = urljoin(url, "dataservers")
    res = requests.get(url_, auth=auth)
    if res.status_code == 200:
        server_list = [r["Name"] for r in res.json()["Items"]]
        return server_list
    elif res.status_code == 404:
        print("Not found")
    elif res.status_code == 401:
        print("Not authorized")


class NoEncodeSession(requests.Session):
    """Override requests.Session to avoid percent-encoding asterisk,
    which causes Aspen Web API to fail.
    """

    def send(self, *args, **kwargs):
        args[0].url = args[0].url.replace(urllib.parse.quote("*"), "*")
        return requests.Session.send(self, *args, **kwargs)


class AspenHandlerWeb:
    def __init__(
        self, server=None, url=None, auth=None, verifySSL=None, options={},
    ):
        self._max_rows = options.get("max_rows", 100000)
        if url is None:
            url = r"https://ws2679.statoil.net/ProcessData/AtProcessDataREST.dll"
        self.base_url = url
        self.dataserver = server
        self.session = NoEncodeSession()
        self.session.verify = verifySSL if verifySSL is not None else True
        self.session.auth = auth if auth else get_auth_aspen()

    @staticmethod
    def stringify(params):
        """Aspen web api doesn't like percent-encoded arguments.
        This method converts a set of parameters on dict-form to
        a continuous string, compatible with Aspen WEB API.

        Chose to use NoEncodeSession() instead, but keeping this
        method in case it is needed at some point.

        :param params: Parameters to request's get()
        :type params: dict
        :return: String-based parameter to send to get()
        :rtype: str
        """
        return "&".join("%s=%s" % (k, v) for k, v in params.items())

    @staticmethod
    def generate_connection_string(host, *_, **__):
        raise NotImplementedError

    @staticmethod
    def generate_search_query(tag=None, desc=None, server=None):
        if not server:
            raise ValueError("Server is required argument")
        params = {"datasource": server, "tag": tag, "max": 100, "getTrendable": 0}
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
        # Probably need to handle this in some way at some point
        maxpoints = 100000
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

        # fmt: off
        query = (
            '<Q f="d" allQuotes="1">'
            "<Tag>"
            f"<N><![CDATA[{tagname}]]></N>"
        )
        # fmt: on

        if mapname:
            query += f"<M><![CDATA[{mapname}]]></M>"

        query += (
            f"<D><![CDATA[{self.dataserver}]]></D>"
            "<F><![CDATA[VAL]]></F>"
            "<HF>0</HF>"  # History format: 0=Raw, 1=RecordAsString
            f"<St>{int(start_time.timestamp())*1000}</St>"
            f"<Et>{int(stop_time.timestamp())*1000}</Et>"
            f"<RT>{rt}</RT>"
        )
        if read_type in [ReaderType.RAW, ReaderType.SHAPEPRESERVING]:
            query += f"<X>{maxpoints}</X>"
        if read_type not in [ReaderType.INT]:
            query += f"<O>{outsiders}</O>"
        if read_type not in [ReaderType.RAW]:
            query += f"<S>{stepped}</S>"
        if read_type not in [ReaderType.RAW, ReaderType.SHAPEPRESERVING]:
            query += (
                f"<P>{int(sample_time.total_seconds())}</P>"
                "<PU>3</PU>"  # Period Unit: 0=day, 1=hour, 2=min, 3=sec
            )
        if read_type not in [
            ReaderType.RAW,
            ReaderType.SHAPEPRESERVING,
            ReaderType.INT,
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

    def verify_connection(self, server):
        """Connects to the URL and verifies that the provided server exists.

        :param server: Data server to look for
        :type server: String
        :raises ConnectionError: If connection fails
        :return: True if server exists, False if not.
        :rtype: Bool
        """
        url = urljoin(self.base_url, "Datasources")
        params = {"service": "ProcessData", "allQuotes": 1}
        if not self.session.verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        res = self.session.get(url, params=params)
        if res.status_code != 200:
            raise ConnectionError
        j = res.json()
        for item in j["data"]:
            if item["n"] == server:
                return True
        return False

    def connect(self):
        self.verify_connection(self.dataserver)

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
            f"<D><![CDATA[{self.dataserver}]]></D>",
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
            f"<D><![CDATA[{self.dataserver}]]></D>",
            "</Tag>",
            "</Q>",
        ]
        return "".join(parts)

    def _get_maps(self, tagname):
        params = self.generate_get_map_query(tagname)
        url = urljoin(self.base_url, "TagInfo")
        res = self.session.get(url, params=params)
        if res.status_code != 200:
            raise ConnectionError
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

    def search_tag(self, tag=None, desc=None):
        if tag is None:
            raise ValueError("Tag is a required argument")

        tag = tag.replace("%", "*") if isinstance(tag, str) else None
        # Prepare for regex
        desc = desc.replace("%", "*") if isinstance(desc, str) else None
        desc = desc.replace("*", ".*") if isinstance(desc, str) else None

        params = self.generate_search_query(tag, desc, self.dataserver)
        url = urljoin(self.base_url, "Browse")
        res = self.session.get(url, params=params)

        if res.status_code != 200:
            raise ConnectionError
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
        if res.status_code != 200:
            raise ConnectionError
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
            f"<D><![CDATA[{self.dataserver}]]></D>",
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
        if res.status_code != 200:
            raise ConnectionError
        j = res.json()
        try:
            desc = j["data"]["tags"][0]["attrData"][0]["samples"][0]["v"]
        except Exception:
            desc = ""
        return desc

    def read_tag(
        self, tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
        if read_type not in [
            ReaderType.INT,
            ReaderType.MIN,
            ReaderType.MAX,
            ReaderType.RNG,
            ReaderType.AVG,
            ReaderType.VAR,
            ReaderType.STD,
        ]:
            raise (NotImplementedError)

        url = urljoin(self.base_url, "History")

        tagname, mapname = self.split_tagmap(tag)

        params = self.generate_read_query(
            tagname, mapname, start_time, stop_time, sample_time, read_type
        )

        res = self.session.get(url, params=params)
        if res.status_code != 200:
            raise ConnectionError

        j = res.json()

        df = (
            pd.DataFrame.from_dict(j["data"][0]["samples"])
            .drop(labels=["l", "s", "V"], axis="columns")
            .rename(columns={"t": "Timestamp", "v": "Value"})
        )
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms", origin="unix")
        df = (
            df.set_index("Timestamp", drop=True)
            .tz_localize("UTC")
            .tz_convert(start_time.tzinfo)
        )
        df.index.name = "time"
        return df.rename(columns={"Value": tag})


class PIHandlerWeb:
    def __init__(
        self, url=None, server=None, auth=None, verifySSL=None, options={},
    ):
        self._max_rows = options.get("max_rows", 100000)
        if url is None:
            url = r"https://piwebapi.equinor.com/piwebapi"
        self.base_url = url
        self.dataserver = server
        self.session = requests.Session()
        self.session.verify = verifySSL if verifySSL is not None else True
        self.session.auth = auth if auth else get_auth_pi()
        self.webidcache = {}

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
    def generate_search_query(tag=None, desc=None, server=None):
        q = []
        if tag is not None:
            q.extend([f"name:{PIHandlerWeb.escape(tag)}"])
        if desc is not None:
            q.extend([f"description:{PIHandlerWeb.escape(desc)}"])
        query = " AND ".join(q)
        params = {"q": f"{query}"}

        if server is not None:
            params["scope"] = f"pi:{server}"

        return params

    @staticmethod
    def generate_read_query(
        tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
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

        webid = tag

        timecast_format_query = "%d-%b-%y %H:%M:%S"
        starttime = start_time.strftime(timecast_format_query)
        stoptime = stop_time.strftime(timecast_format_query)

        sample_time = sample_time.seconds

        get_action = {ReaderType.INT: "interpolated", ReaderType.RAW: "recorded"}.get(
            read_type, "summary"
        )

        url = f"streams/{webid}/{get_action}"
        params = {}

        params["startTime"] = starttime
        params["endTime"] = stoptime

        summary_type = {
            ReaderType.MIN: "Minimum",
            ReaderType.MAX: "Maximum",
            ReaderType.AVG: "Average",
            ReaderType.VAR: "StdDev",
            ReaderType.STD: "StdDev",
            ReaderType.RNG: "Range",
        }.get(read_type, None)

        if ReaderType.INT == read_type:
            params["interval"] = f"{sample_time}s"
        elif summary_type:
            params["summaryType"] = summary_type
            params["summaryDuration"] = f"{sample_time}s"

        return (url, params)

    def verify_connection(self, server):
        """Connects to the URL and verifies that the provided server exists.

        :param server: Data server to look for
        :type server: String
        :raises ConnectionError: If connection fails
        :return: True if server exists, False if not.
        :rtype: Bool
        """
        url = urljoin(self.base_url, "dataservers")
        if not self.session.verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        res = self.session.get(url)
        if res.status_code != 200:
            raise ConnectionError
        j = res.json()
        for item in j["Items"]:
            if item["Name"] == server:
                return True
        return False

    def connect(self):
        self.verify_connection(self.dataserver)

    def search_tag(self, tag=None, desc=None):
        params = self.generate_search_query(tag, desc, self.dataserver)
        url = urljoin(self.base_url, "search", "query")
        done = False
        ret = []
        while not done:
            res = self.session.get(url, params=params)

            if res.status_code != 200:
                raise ConnectionError
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
        url = urljoin(self.base_url, "points", webid)
        res = self.session.get(url)
        if res.status_code != 200:
            raise ConnectionError
        j = res.json()
        unit = j["EngineeringUnits"]
        return unit

    def _get_tag_description(self, tag):
        webid = self.tag_to_webid(tag)
        url = urljoin(self.base_url, "points", webid)
        res = self.session.get(url)
        if res.status_code != 200:
            raise ConnectionError
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
            params = self.generate_search_query(tag=tag, server=self.dataserver)
            params["fields"] = "name;webid"
            url = urljoin(self.base_url, "search", "query")
            res = self.session.get(url, params=params)
            if res.status_code != 200:
                raise ConnectionError
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
                raise AssertionError(f"No WebId found for {tag}.")

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
        self, tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
        webid = self.tag_to_webid(tag)
        (url, params) = self.generate_read_query(
            webid, start_time, stop_time, sample_time, read_type
        )
        if self._is_summary(read_type):
            params["selectedFields"] = "Links;Items.Value.Timestamp;Items.Value.Value"
        else:
            params["selectedFields"] = "Links;Items.Timestamp;Items.Value"
        url = urljoin(self.base_url, url)
        res = self.session.get(url, params=params)
        if res.status_code != 200:
            raise ConnectionError

        j = res.json()
        # Summary (aggregated) data and DigitalSets return Value as dict
        df = pd.json_normalize(data=j, record_path="Items")
        if self._is_summary(read_type):
            df = df.rename(
                columns={"Value.Timestamp": "Timestamp", "Value.Value": "Value"}
            )
            # TODO: Square Value if read_type is VAR

        # Missing data or digitalset:
        if "Value.Name" in df.columns:
            # Missing data in all rows or digitalset.
            # Assume digitalset and replace any missing data with None.
            # TODO: What happens for digital values with missing data?
            # TODO: And what about summaries with digital values (with missing data)...?
            if "Value" not in df.columns:
                df["Value"] = df["Value.Value"]
                df.loc[df["Value.Name"].str.contains("No Data"), "Value"] = None
            df = df.drop(columns=["Value.Name", "Value.Value", "Value.IsSystem"])
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%Y-%m-%dT%H:%M:%SZ")

        df = df.set_index("Timestamp", drop=True)
        df.index.name = "time"
        df = df.tz_localize("UTC").tz_convert(start_time.tzinfo)

        return df.rename(columns={"Value": tag})
