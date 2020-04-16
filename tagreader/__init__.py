from .clients import IMSClient
from .odbc_handlers import list_aspen_servers, list_pi_servers
try:
    from .version import version as __version__
except ImportError:
    # Just in case it wasn't installed properly, for some reason
    from datetime import datetime
    __version__ = 'unknown-'+datetime.today().strftime('%Y%m%d')
