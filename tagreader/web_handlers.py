import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from .utils import logging, urljoin


logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def get_auth():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def list_pi_servers(host=r"https://pivision.equinor.com/piwebapi"):
    url = urljoin(host, "/assetservers")
    response = requests.get(url, auth=get_auth())
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
        host=r"https://aspenone.equinor.com/ProcessData",
        port=443,
        options={},
    ):
        raise NotImplementedError


class PIHandlerWeb:
    def __init__(
        self, host=None, port=None, options={},
    ):
        self._max_rows = options.get("max_rows", 100000)
        if host is None:
            host = r"https://pivision.equinor.com/piwebapi"
        self.host = host
        self.port = port
        self.base_url = host
        self.session = requests.Session()
        self.session.auth = get_auth()

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
                    " ": r"\ "
                }
            )
        )

    @staticmethod
    def generate_search_query(tag=None, desc=None):
        q = []
        if tag is not None:
            q.extend([f"name:{PIHandlerWeb.escape(tag)}"])
        if desc is not None:
            q.extend([f"description:{PIHandlerWeb.escape(desc)}"])
        query = " AND ".join(q)

        params = {"q": f"{query}", "scope": "pi:PINO"}
        return params

    @staticmethod
    def generate_read_query(tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError

    def connect(self):
        self.session.auth = get_auth()

    def search_tag(self, tag=None, desc=None):
        params = self.generate_search_query(tag, desc)
        url = urljoin(self.base_url, "search", "query")
        print("******************************************")
        print(url)
        print(self.base_url)
        done = False
        ret = []
        while not done:
            # res = self.session.get(
            #     "https://pivision.equinor.com/piwebapi/search/query", params=params
            # )
            res = self.session.get(
                url, params=params
            )

            if res.status_code != 200:
                raise Exception
            j = res.json()
            for item in j["Items"]:
                description = item["Description"] if "Description" in item else ""
                ret.append((item["Name"], description))
            next_start = int(j["Links"]["Next"].split("=")[-1])
            if int(j["Links"]["Last"].split("=")[-1]) >= next_start:
                params['start'] = next_start
            else:
                done = True
        return ret

    def read_tag(self, conn, tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError
