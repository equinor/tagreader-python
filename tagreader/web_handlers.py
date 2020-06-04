import sys
import warnings
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from .utils import logging, urljoin


# try:
#     from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
# except ImportError:
#     pass


logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def get_auth():
    return HTTPKerberosAuth(mutual_authentication=OPTIONAL)


def list_pi_servers(host=r"https://pivision.equinor.com/piwebapi"):
    # Suppress warning concerning use of verifySsl = False
    url = urljoin(host, "/assetservers")
    response = requests.get(url, auth=get_auth())
    if response.status_code == 200:
        server_list = []
        for r in response.json()['Items']:
            server_list.append(r['Name'])
        return server_list
    elif response.status_code == 404:
        print("Not found")


class AspenHandlerWeb:
    def __init__(
        self,
        host=r"https://aspenone.equinor.com/ProcessData",
        port=443,
        max_rows=100000,
        options={},
    ):
        raise NotImplementedError


class PIHandlerWeb:
    def __init__(
        self,
        host=r"https://pivision.equinor.com/piwebapi",
        port=443,
        max_rows=100000,
        options={},
    ):
        self._max_rows = max_rows
        self.host = host
        self.port = port
        # Suppress warning concerning use of verifySsl = False
        import warnings
        from urllib3.exceptions import InsecureRequestWarning
        warnings.simplefilter("ignore", InsecureRequestWarning)
        self.session = requests.Session()

    @staticmethod
    def generate_connection_string(host, *_, **__):
        raise NotImplementedError

    @staticmethod
    def generate_search_query(tag=None, desc=None):
        raise NotImplementedError

    @staticmethod
    def generate_read_query(tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError

    def connect(self):
        self.session.auth = get_auth()

    def search_tag(self, tag):
        raise NotImplementedError

    def read_tag(self, conn, tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError


class PIHandlerWebOld:
    def __init__(
        self,
        host=r"https://pivision.statoil.no/piwebapi",
        port=443,
        max_rows=100000,
        options={},
    ):
        if "osisoft.pidevclub.piwebapi.pi_web_api_client" not in sys.modules:
            raise ModuleNotFoundError(
                "You need to install the PI Web API from OSISoft to use the PI web handler."  # noqa: E501
            )
        warnings.warn("Unable to import PIWebApiClient")
        self._max_rows = max_rows
        self.host = host
        self.port = port
        # Suppress warning concerning use of verifySsl = False
        # from urllib3.exceptions import InsecureRequestWarning
        # warnings.simplefilter("ignore", InsecureRequestWarning)
        # self.client = PIWebApiClient(host, useKerberos=True, verifySsl=False)

    @staticmethod
    def generate_connection_string(host, *_, **__):
        raise NotImplementedError

    @staticmethod
    def generate_search_query(tag=None, desc=None):
        raise NotImplementedError

    @staticmethod
    def generate_read_query(tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError

    def connect(self):
        """Not really needed, but nice to follow handler interface and have
        a quick check whether we can actually connect to the home page.
        """
        try:
            self.client.home.get()
        except:
            raise ConnectionError

    def search_tag(self, tag):
        raise NotImplementedError

    def read_tag(self, conn, tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError
