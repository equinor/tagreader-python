import datetime
from pyims import utils

def test_datestr_to_datetime():
    assert utils.datestr_to_datetime('10. jan. 2018 13:45:15') == datetime.datetime(2018,1,10,13,45,15)
    assert utils.datestr_to_datetime('01.02.03 00:00:00') == datetime.datetime(2003, 2, 1, 0, 0)
    assert utils.datestr_to_datetime(datetime.datetime(2003, 2, 1, 0, 0)) == datetime.datetime(2003, 2, 1, 0, 0)