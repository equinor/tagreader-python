from .clients import IMSClient      # noqa: F401
from .odbc_handlers import (        # noqa: F401
    list_aspen_sources,
    list_pi_sources,
)
from .utils import ReaderType
try:
    from .version import version as __version__
except ImportError:
    # Just in case it wasn't installed properly, for some reason
    from datetime import datetime
    __version__ = 'unknown-' + datetime.today().strftime('%Y%m%d')
