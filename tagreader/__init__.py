from tagreader.clients import IMSClient, list_sources
from tagreader.utils import (
    IMSType,
    ReaderType,
    add_statoil_root_certificate,
    is_equinor,
    is_mac,
    is_windows,
)

if is_windows():
    from tagreader.odbc_handlers import list_aspen_servers, list_pi_servers

if is_equinor():
    add_statoil_root_certificate()

from tagreader.__version__ import version as __version__
