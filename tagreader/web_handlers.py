import requests
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


def get_auth():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def list_pi_servers(url=r"https://pivision.equinor.com/piwebapi"):
    url_ = urljoin(url, "/dataservers")
    response = requests.get(url_, auth=get_auth())
    if response.status_code == 200:
        server_list = []
        for r in response.json()["Items"]:
            server_list.append(r["Name"])
        return server_list
    elif response.status_code == 404:
        print("Not found")


class AspenHandlerWeb:
    def __init__(
        self,
        server=None,
        url=r"https://aspenone.equinor.com/ProcessData/AtProcessDataREST.dll",
        options={},
    ):
        raise NotImplementedError


class PIHandlerWeb:
    def __init__(
        self, url=None, server=None, options={},
    ):
        self._max_rows = options.get("max_rows", 100000)
        if url is None:
            url = r"https://pivision.equinor.com/piwebapi"
        self.base_url = url
        self.dataserver = server
        self.session = requests.Session()
        self.session.auth = get_auth()
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

        if ReaderType.INT == read_type:
            params["interval"] = f"{sample_time}s"
        else:
            # https://techsupport.osisoft.com/Documentation/PI-Web-API/help/topics/sample-type.html
            params["sampleType"] = "Interval"
            params["sampleInterval"] = f"{sample_time}s"

        summary_type = {
            ReaderType.MIN: "Minimum",
            ReaderType.MAX: "Maximum",
            ReaderType.AVG: "Average",
            ReaderType.VAR: "StdDev",
            ReaderType.STD: "StdDev",
            ReaderType.RNG: "Range",
        }.get(read_type, None)

        if summary_type:
            params["summaryType"] = summary_type

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

    def _get_tag_description(self, tag):
        raise NotImplementedError

    def _get_tag_unit(self, tag):
        raise NotImplementedError

    def tag_to_webid(self, tag):
        """Given a tag, returns the WebId.

        :param tag: The tag
        :type tag: str
        :raises ConnectionError: If connection or query fails
        :return: WebId
        :rtype: str
        """
        if not tag in self.webidcache:
            params = self.generate_search_query(tag=tag, server=self.dataserver)
            params["fields"] = "name;webid"
            url = urljoin(self.base_url, "search", "query")
            res = self.session.get(url, params=params)
            if res.status_code != 200:
                raise ConnectionError
            j = res.json()
            if len(j["Items"]) > 1:
                raise AssertionError(
                    f"Received {len(j['Items'])} results when trying to find unique WebId for {tag}."
                )
            elif len(j["Items"]) == 0:
                raise AssertionError(f"No WebId found for {tag}.")
            webid = j["Items"][0]["WebId"]
            self.webidcache[tag] = webid
        return self.webidcache[tag]

    def read_tag(
        self, tag, start_time, stop_time, sample_time, read_type, metadata=None
    ):
        webid = self.tag_to_webid(tag)
        (url, params) = self.generate_read_query(
            webid, start_time, stop_time, sample_time, read_type
        )
        params["selectedFields"] = "Links;Items.TimeStamp;Items.Value"
        url = urljoin(self.base_url, url)
        res = self.session.get(url, params=params)
        if res.status_code != 200:
            raise ConnectionError
        j = res.json()

        df = pd.DataFrame.from_dict(j["Items"])
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%Y-%m-%dT%H:%M:%SZ")
        df = df.set_index("Timestamp")
        df.index.name = "time"
        df = df.tz_localize("UTC").tz_convert(start_time.tzinfo)
        # TODO: Handle digitalset
        return df.rename(columns={"Value": tag})

