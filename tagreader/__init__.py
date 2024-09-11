from tagreader.clients import IMSClient, list_sources
from tagreader.utils import (
    IMSType,
    ReaderType,
    add_equinor_root_certificate,
    is_equinor,
    is_mac,
    is_windows,
)

if is_equinor():
    add_equinor_root_certificate()

from tagreader.__version__ import version as __version__
