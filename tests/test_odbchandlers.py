import pytest
#import pandas as pd
#from pyims import utils
#from pyims.readertype import ReaderType
from pyims import IMSClient

from pyims.odbc_handlers import list_aspen_servers,\
    list_pi_servers,\
    PIHandlerODBC,\
    AspenHandlerODBC

from pyims.clients import get_server_address_aspen,\
    get_server_address_pi

def test_list_all_aspen_servers():
    res = list_aspen_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 10

def test_list_all_pi_servers():
    res = list_pi_servers()
    print(res)
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 11

def test_get_aspen_server_address():
    host, port = get_server_address_aspen('SNA')
    assert 'SNA-IMS.statoil.net' == host
    assert 10014 == port

def test_get_pi_server_address():
    host, port = get_server_address_pi('ONO-IMS')
    assert 'stj-w22.statoil.net' == host
    assert 5450 == port

def test_init():
    with pytest.raises(ValueError):
        c = IMSClient('xyz')
    with pytest.raises(ValueError):
        c = IMSClient('sNa', 'pi')
    with pytest.raises(ValueError):
        c = IMSClient('Ono-imS', 'aspen')
    with pytest.raises(ValueError):
        c = IMSClient('ono-ims', 'aspen')
    with pytest.raises(ValueError):
        c = IMSClient('sna', 'pi')
    c = IMSClient('onO-iMs', 'pi')
    assert isinstance(c.handler, PIHandlerODBC)
    c = IMSClient('snA', 'aspen')
    assert isinstance(c.handler, AspenHandlerODBC)
