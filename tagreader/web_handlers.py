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


def get_auth_pi():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def get_auth_aspen():
    return HTTPKerberosAuth(service="HTTPS")


def list_aspen_servers(
    url=r"https://aspenone-qa.equinor.com/ProcessData/AtProcessDataREST.dll",
    auth=get_auth_aspen(),
    verify=False,
):
    url_ = urljoin(url, "DataSources")
    params = {"service": "ProcessData", "allQuotes": 1}

    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    res = requests.get(url_, params=params, auth=auth, verify=False)
    if res.status_code == 200:
        server_list = [r["n"] for r in res.json()["data"] if r["t"] == "IP21"]
        return server_list
    elif res.status_code == 404:
        print("Not found")


def list_pi_servers(url=r"https://piwebapi.equinor.com/piwebapi", auth=get_auth_pi()):
    url_ = urljoin(url, "/dataservers")
    res = requests.get(url_, auth=auth)
    if res.status_code == 200:
        server_list = [r["Name"] for r in res.json()["Items"]]
        return server_list
    elif res.status_code == 404:
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
            url = r"https://piwebapi.equinor.com/piwebapi"
        self.base_url = url
        self.dataserver = server
        self.session = requests.Session()
        self.session.auth = get_auth_pi()
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
