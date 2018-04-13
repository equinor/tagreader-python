import pytest
#import pandas as pd
#from imsclient import utils
#from imsclient.readertype import ReaderType
from imsclient import IMSClient, \
    get_aspen_servers, \
    get_pi_servers

from imsclient.clients import get_server_address_aspen, \
    get_server_address_pi, \
    AspenHandler, \
    PIHandler

def test_list_all_aspen_servers():
    res = get_aspen_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 10

def test_list_all_pi_servers():
    res = get_pi_servers()
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
    c = IMSClient('onO-iMs')
    assert isinstance(c.handler, PIHandler)
    c = IMSClient('snA')
    assert isinstance(c.handler, AspenHandler)
