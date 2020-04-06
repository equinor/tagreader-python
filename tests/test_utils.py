import datetime
from pytagreader import utils
from pytz import timezone

def test_datestr_to_datetime():
    assert utils.datestr_to_datetime('10. jan. 2018 13:45:15') == timezone('Europe/Oslo').localize(datetime.datetime(2018,1,10,13,45,15))
    assert utils.datestr_to_datetime('02.01.03 00:00:00') == timezone('Europe/Oslo').localize(datetime.datetime(2003,1,2,0,0,0))
    assert utils.datestr_to_datetime('02.01.03 00:00:00') == utils.datestr_to_datetime('2003-02-01 0:00:00am')
    assert utils.datestr_to_datetime('02.01.03 00:00:00', 'America/Sao_Paulo') == timezone('America/Sao_Paulo').localize(datetime.datetime(2003,1,2,0,0,0))