import pytest
import pandas as pd
from readertype import ReaderType

from pyims.cache import SmartCache, \
    safe_tagname

@pytest.fixture()
def data():
    length = 10
    df_total = pd.DataFrame(
        {'tag1': range(0, length)},
         index = pd.date_range(start="2018-01-18 05:00:00", freq="60s", periods=length, name="time"))
    yield df_total

@pytest.fixture()
def cache(request):
    cache = SmartCache('testcache.h5')
    yield cache
    cache.remove()

def test_safe_tagname():
    assert safe_tagname('ASGB.tt-___56_ _%_') == 'ASGB.tt___56___'

def test_key_path(cache):
    pass

def test_cache_single_store_and_fetch(cache, data):
    cache.store(data, readtype = ReaderType.INT)
    df_read = cache.fetch('tag1', ReaderType.INT, 60)
    pd.testing.assert_frame_equal(data, df_read)
    cache.store(data, readtype = ReaderType.RAW)
    df_read = cache.fetch('tag1', ReaderType.RAW)
    pd.testing.assert_frame_equal(data, df_read)

def test_cache_multiple_store_single_fetch(cache, data):
    df1 = data[0:3]
    df2 = data[2:10]
    cache.store(df1, readtype=ReaderType.INT)
    cache.store(df2, readtype=ReaderType.INT)
    df_read = cache.fetch('tag1', ReaderType.INT, 60)
    pd.testing.assert_frame_equal(df_read, data)

def test_interval_reads(cache, data):
    cache.store(data, readtype=ReaderType.INT)
    start_time_oob = pd.to_datetime("2018-01-18 04:55:00")
    start_time = pd.to_datetime("2018-01-18 05:05:00")
    stop_time = pd.to_datetime("2018-01-18 05:08:00")
    stop_time_oob = pd.to_datetime("2018-01-18 06:00:00")

    df_read = cache.fetch('tag1', ReaderType.INT, ts=60, start_time=start_time)
    pd.testing.assert_frame_equal(data[start_time:], df_read)
    df_read = cache.fetch('tag1', ReaderType.INT, ts=60, stop_time=stop_time)
    pd.testing.assert_frame_equal(data[:stop_time].iloc[:-1], df_read)
    df_read = cache.fetch('tag1', ReaderType.INT, ts=60, start_time=start_time_oob)
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch('tag1', ReaderType.INT, ts=60, stop_time=stop_time_oob)
    pd.testing.assert_frame_equal(data, df_read)
    df_read = cache.fetch('tag1', ReaderType.INT, ts=60, start_time=start_time, stop_time=stop_time)
    pd.testing.assert_frame_equal(data[start_time:stop_time].iloc[:-1], df_read)

def test_match_tag(cache):
    assert True == cache._match_tag('INT/s60/tag1', readtype=ReaderType.INT, ts=60, tagname='tag1')
    assert False == cache._match_tag('INT/s60/tag1', readtype=ReaderType.RAW, ts=60, tagname='tag1')
    assert False == cache._match_tag('INT/s60/tag1', readtype=ReaderType.INT, ts=10, tagname='tag1')
    assert False == cache._match_tag('INT/s60/tag1', readtype=ReaderType.INT, ts=60, tagname='tag2')
    assert True == cache._match_tag('INT/s60/tag1', ts=60, tagname='tag1')
    assert True == cache._match_tag('INT/s60/tag1', readtype=ReaderType.INTERPOLATE, tagname='tag1')
    assert True == cache._match_tag('INT/s60/tag1', readtype=ReaderType.INT, ts=60)
    assert True == cache._match_tag('INT/s60/tag1', readtype=[ReaderType.INT, ReaderType.RAW],
                                    ts=[60, 10], tagname=['tag1', 'tag2'])
    assert False == cache._match_tag('INT/s60/tag1', readtype=[ReaderType.AVERAGE, ReaderType.RAW],
                                     ts=[60, 10], tagname=['tag1', 'tag2'])
    assert False == cache._match_tag('INT/s60/tag1', readtype=[ReaderType.INT, ReaderType.RAW],
                                     ts=[120, 10], tagname=['tag1', 'tag2'])
    assert False == cache._match_tag('INT/s60/tag1', readtype=[ReaderType.INT, ReaderType.RAW],
                                     ts=[60, 10], tagname=['tag3', 'tag2'])

def test_delete_tag(cache, data):
    cache.store(data, readtype=ReaderType.INT)
    cache.store(data, readtype=ReaderType.RAW)
    with cache._get_hdfstore() as f:
        assert 'INT/s60/tag1' in f
        assert 'RAW/tag1' in f
    cache.delete_key('tag1', ReaderType.INT, 60)
    cache.delete_key('tag1')
    with cache._get_hdfstore() as f:
        assert 'INT/s60/tag1' not in f
        assert 'RAW/tag1' not in f
