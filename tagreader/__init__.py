from .clients import IMSClient, list_sources  # noqa: F401
from .odbc_handlers import (  # noqa: F401
    list_aspen_servers,
    list_pi_servers
)
from .utils import ReaderType  # noqa: F401

try:
    from .version import version as __version__
except ImportError:
    # Just in case it wasn't installed properly, for some reason
    from datetime import datetime

    __version__ = "unknown-" + datetime.today().strftime("%Y%m%d")
