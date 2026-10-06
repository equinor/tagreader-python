from tagreader.clients import IMSClient, list_sources
from tagreader.utils import (
    IMSType,
    ReaderType,
    add_equinor_root_certificate,
    is_equinor,
)

if is_equinor():
    add_equinor_root_certificate()

from tagreader.__version__ import version as __version__

__all__ = [
    "IMSClient",
    "list_sources",
    "IMSType",
    "ReaderType",
    "add_equinor_root_certificate",
    "__version__",
]
