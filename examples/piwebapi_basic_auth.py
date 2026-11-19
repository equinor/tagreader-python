import getpass

from tagreader.clients import IMSClient
from tagreader.utils import ReaderType
from tagreader.web_handlers import set_basic_auth


def main() -> None:
    datasource = "JSV"  # input("PI Web API data source: ").strip()
    username = input("PI Web API username: ").strip()
    if "\\" not in username and "@" not in username:
        username = f"statoil.net\\{username}"
    password = getpass.getpass("PI Web API password: ")
    tag = "1901-A-20LIC1473A.CA_Y"

    set_basic_auth(username, password)
    client = IMSClient(datasource=datasource, imstype="piwebapi", cache=None)
    try:
        # client.connect()
        result = client.read(tags=tag, read_type=ReaderType.SNAPSHOT)
    finally:
        client.handler.session.close()

    print(result)


if __name__ == "__main__":
    main()
