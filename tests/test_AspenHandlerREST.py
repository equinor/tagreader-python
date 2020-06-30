import pytest
import pandas as pd
from tagreader import utils
from tagreader.utils import ReaderType
from tagreader.web_handlers import AspenHandlerWeb

START_TIME = "2018-05-01 10:00:00"
STOP_TIME = "2018-05-01 11:00:00"
SAMPLE_TIME = 60


@pytest.fixture()
def AspenHandler():
    h = AspenHandlerWeb(datasource="sourcename")
    yield h


def test_stringify():
    assert AspenHandlerWeb.stringify({"a": "b*", "c": "d!"}) == "a=b*&c=d!"


def test_generate_search_query():
    with pytest.raises(ValueError):
        AspenHandlerWeb.generate_search_query("ATCAI")
    assert AspenHandlerWeb.generate_search_query("ATCAI", datasource="sourcename") == {
        "datasource": "sourcename",
        "tag": "ATCAI",
        "max": 100,
        "getTrendable": 0,
    }
    assert AspenHandlerWeb.generate_search_query("ATC*", datasource="sourcename") == {
        "datasource": "sourcename",
        "tag": "ATC*",
        "max": 100,
        "getTrendable": 0,
    }
    assert AspenHandlerWeb.generate_search_query(
        tag="ATCAI", datasource="sourcename"
    ) == {"datasource": "sourcename", "tag": "ATCAI", "max": 100, "getTrendable": 0}


def test_split_tagmap():
    assert AspenHandlerWeb.split_tagmap("ATCAI") == ("ATCAI", None)
    assert AspenHandlerWeb.split_tagmap("ATCAI;IP_ANALOGMAP") == (
        "ATCAI",
        "IP_ANALOGMAP",
    )


def test_generate_description_query(AspenHandler):
    assert AspenHandler.generate_get_description_query("ATCAI") == (
        '<Q allQuotes="1" attributeData="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D><AL><A>DSCR</A>"
        "<VS>0</VS></AL></Tag></Q>"
    )


def test_generate_unit_query(AspenHandler):
    assert AspenHandler.generate_get_unit_query("ATCAI") == (
        '<Q allQuotes="1" attributeData="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D><AL><A>Units</A>"
        "<A>MAP_Units</A><VS>0</VS></AL></Tag></Q>"
    )


def test_generate_map_query(AspenHandler):
    assert AspenHandler.generate_get_map_query("ATCAI") == (
        '<Q allQuotes="1" categoryInfo="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D></Tag></Q>"
    )


@pytest.mark.parametrize(
    ("read_type"),
    [
        ("RAW"),
        ("SHAPEPRESERVING"),
        ("INT"),
        ("MIN"),
        ("MAX"),
        ("RNG"),
        ("AVG"),
        ("VAR"),
        ("STD"),
        # pytest.param("COUNT", 0, marks=pytest.mark.skip),
        # pytest.param("GOOD", 0, marks=pytest.mark.skip),
        # pytest.param("BAD", 0, marks=pytest.mark.skip),
        # pytest.param("TOTAL", 0, marks=pytest.mark.skip),
        # pytest.param("SUM", 0, marks=pytest.mark.skip),
        # pytest.param("SNAPSHOT", 0, marks=pytest.mark.skip),
    ],
)
def test_generate_tag_read_query(AspenHandler, read_type):
    start_time = utils.datestr_to_datetime("2020-06-24 17:00:00")
    stop_time = utils.datestr_to_datetime("2020-06-24 18:00:00")
    ts = pd.Timedelta(1, unit="m")
    res = AspenHandler.generate_read_query(
        "ATCAI", None, start_time, stop_time, ts, getattr(ReaderType, read_type)
    )
    expected = {
        "RAW": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>0</RT><X>100000</X><O>0</O></Tag></Q>"
        ),
        "SHAPEPRESERVING": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>2</RT><X>100000</X><O>0</O><S>0</S></Tag></Q>"
        ),
        "INT": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>1</RT><S>0</S><P>60</P><PU>3</PU></Tag></Q>"
        ),
        "MIN": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>14</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "MAX": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>13</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "RNG": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>15</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "AVG": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>12</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "VAR": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>18</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "STD": (
            '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
            "<RT>17</RT><O>0</O><S>0</S><P>60</P><PU>3</PU><AM>0</AM>"
            "<AS>0</AS><AA>0</AA><DSA>0</DSA></Tag></Q>"
        ),
        "COUNT": ("whatever"),
        "GOOD": ("whatever"),
        "BAD": ("whatever"),
        "TOTAL": ("whatever"),
        "SUM": ("whatever"),
        "SNAPSHOT": ("whatever"),
    }
    assert expected[read_type] == res
