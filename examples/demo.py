import tagreader
from tagreader.utils import IMSType
from tagreader.web_handlers import get_auth_aspen, get_auth_pi, get_url_aspen

use_internal = False


print(
    tagreader.list_sources(
        imstype=IMSType.ASPENONE,
        url=get_url_aspen(use_internal=use_internal),
        auth=get_auth_aspen(use_internal=use_internal),
    )
)

c = tagreader.IMSClient(
    "GRA",
    handler_options={"max_rows": 1e7},
    auth=get_auth_aspen(use_internal=use_internal),
    url=get_url_aspen(use_internal=use_internal),
)
t = c.search("*PDIC*")
d = c.read(tags=t[0][0], start_time="2024-Jan-01", end_time="2024-Jan-02")


print(
    tagreader.list_sources(
        imstype=IMSType.PIWEBAPI, auth=get_auth_pi(use_internal=use_internal)
    )
)
c = tagreader.IMSClient("JSV")
t4 = c.search("*PDIC*")
d = c.read(tags=t4[0][0], start_time="2024-Jan-02", end_time="2024-Jan-02")


pass
