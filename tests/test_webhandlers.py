import pytest
#import pandas as pd
#from pyims import utils
#from pyims.readertype import ReaderType

from pyims.web_handlers import list_pi_servers

@pytest.fixture(scope="module")
def handler():
    from pyims.web_handlers import PIHandlerWeb
    yield PIHandlerWeb()

def test_list_all_pi_servers():
    res = list_pi_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 11

def test_connect(handler):
    handler.connect()