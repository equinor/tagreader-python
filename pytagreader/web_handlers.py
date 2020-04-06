import sys
import warnings
from .utils import *

try:
    from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
except:
    pass

logging.basicConfig(
    format=" %(asctime)s %(levelname)s: %(message)s", level=logging.INFO
)


def list_pi_servers(host=r"https://pivision.statoil.no/piwebapi"):
    ## Suppress warning concerning use of verifySsl = False
    import warnings
    from urllib3.exceptions import InsecureRequestWarning

    warnings.simplefilter("ignore", InsecureRequestWarning)
    c = PIWebApiClient(host, useKerberos=True, verifySsl=False)
    server_list = []
    dataServers = c.dataServer.list()
    for ds in dataServers.items:
        server_list.append(ds.name)
    return server_list


class AspenHandlerWeb:
    def __init__(self, max_rows=100000):
        raise NotImplementedError


class PIHandlerWeb:
    def __init__(
        self,
        host=r"https://pivision.statoil.no/piwebapi",
        port=443,
        max_rows=100000,
        options={},
    ):
        if "osisoft.pidevclub.piwebapi.pi_web_api_client" not in sys.modules:
            raise ModuleNotFoundError(
                "You need to install the PI Web API from OSISoft to use the PI web handler."
            )
        warnings.warn("Unable to import PIWebApiClient")
        self._max_rows = max_rows
        self.host = host
        self.port = port
        ## Suppress warning concerning use of verifySsl = False
        # from urllib3.exceptions import InsecureRequestWarning
        # warnings.simplefilter("ignore", InsecureRequestWarning)
        self.client = PIWebApiClient(host, useKerberos=True, verifySsl=False)

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
        """Not really needed, but nice to follow handler interface and have a quick check whether we can actually
        connect to the home page.
        """
        try:
            self.client.home.get()
        except:
            raise ConnectionError

    def search_tag(self, tag):
        raise NotImplementedError

    def read_tag(self, conn, tag, start_time, stop_time, sample_time, read_type):
        raise NotImplementedError
