from .clients import IMSClient, list_sources  # noqa: F401
from .utils import ReaderType  # noqa: F401
from .utils import add_statoil_root_certificate, is_equinor, is_windows

if is_windows():
    from .odbc_handlers import list_aspen_servers, list_pi_servers  # noqa: F401

try:
    from .version import version as __version__
except ImportError:
    # Just in case it wasn't installed properly, for some reason
    from datetime import datetime

    __version__ = "unknown-" + datetime.today().strftime("%Y%m%d")

if is_equinor() and is_windows():
    add_statoil_root_certificate(noisy=False)
