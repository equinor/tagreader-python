from datetime import timedelta

import pytest

from tagreader import utils
from tagreader.utils import ReaderType
from tagreader.web_handlers import AspenHandlerWeb

SAMPLE_TIME = timedelta(seconds=60)


@pytest.fixture  # type: ignore[misc]
def aspen_handler() -> AspenHandlerWeb:  # type: ignore[misc]
    h = AspenHandlerWeb(
        datasource="sourcename", auth=None, options={}, url=None, verifySSL=None
    )
    yield h


def test_generate_search_query() -> None:
    with pytest.raises(ValueError):
        AspenHandlerWeb.generate_search_query(tag="ATCAI", desc=None, datasource=None)
    assert AspenHandlerWeb.generate_search_query(
        tag="ATCAI", datasource="sourcename", desc=None
    ) == {
        "datasource": "sourcename",
        "tag": "ATCAI",
        "max": 100,
        "getTrendable": 0,
    }
    assert AspenHandlerWeb.generate_search_query(
        tag="ATC*", datasource="sourcename", desc=None
    ) == {
        "datasource": "sourcename",
        "tag": "ATC*",
        "max": 100,
        "getTrendable": 0,
    }
    assert AspenHandlerWeb.generate_search_query(
        tag="ATCAI", datasource="sourcename", desc=None
    ) == {"datasource": "sourcename", "tag": "ATCAI", "max": 100, "getTrendable": 0}


def test_split_tagmap() -> None:
    assert AspenHandlerWeb.split_tagmap("ATCAI") == ("ATCAI", None)
    assert AspenHandlerWeb.split_tagmap("ATCAI;IP_ANALOGMAP") == (
        "ATCAI",
        "IP_ANALOGMAP",
    )


def test_generate_description_query(aspen_handler: AspenHandlerWeb) -> None:
    assert aspen_handler.generate_get_description_query("ATCAI") == (
        '<Q allQuotes="1" attributeData="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D><AL><A>DSCR</A>"
        "<VS>0</VS></AL></Tag></Q>"
    )


def test_generate_unit_query(aspen_handler: AspenHandlerWeb) -> None:
    assert aspen_handler.generate_get_unit_query("ATCAI") == (
        '<Q allQuotes="1" attributeData="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D><AL><A>Units</A>"
        "<A>MAP_Units</A><VS>0</VS></AL></Tag></Q>"
    )


def test_generate_map_query(aspen_handler: AspenHandlerWeb) -> None:
    assert aspen_handler.generate_get_map_query("ATCAI") == (
        '<Q allQuotes="1" categoryInfo="1"><Tag><N><![CDATA[ATCAI]]></N><T>0</T>'
        "<G><![CDATA[ATCAI]]></G><D><![CDATA[sourcename]]></D></Tag></Q>"
    )


@pytest.mark.parametrize(  # type: ignore[misc]
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
        "SNAPSHOT",
    ],
)
def test_generate_tag_read_query(
    aspen_handler: AspenHandlerWeb, read_type: str
) -> None:
    starttime = utils.ensure_datetime_with_tz("2020-06-24 17:00:00")
    endtime = utils.ensure_datetime_with_tz("2020-06-24 18:00:00")
    ts = SAMPLE_TIME
    res = aspen_handler.generate_read_query(
        tagname="ATCAI",
        mapname=None,
        start_time=starttime,
        stop_time=endtime,
        sample_time=ts,
        read_type=getattr(ReaderType, read_type),
        metadata={},
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
        "SNAPSHOT": (
            '<Q f="d" allQuotes="1" rt="1593014400000" uc="0">'
            "<Tag><N><![CDATA[ATCAI]]></N>"
            "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
            "<VS>1</VS><S>0</S></Tag></Q>"
        ),
    }
    assert expected[read_type] == res


def test_genreadquery_long_sampletime(aspen_handler: AspenHandlerWeb) -> None:
    starttime = utils.ensure_datetime_with_tz("2020-06-24 17:00:00")
    endtime = utils.ensure_datetime_with_tz("2020-06-24 18:00:00")
    ts = timedelta(seconds=86401)

    res = aspen_handler.generate_read_query(
        tagname="ATCAI",
        mapname=None,
        start_time=starttime,
        stop_time=endtime,
        sample_time=ts,
        read_type=ReaderType.INT,
        metadata={},
    )
    expected = (
        '<Q f="d" allQuotes="1"><Tag><N><![CDATA[ATCAI]]></N>'
        "<D><![CDATA[sourcename]]></D><F><![CDATA[VAL]]></F>"
        "<HF>0</HF><St>1593010800000</St><Et>1593014400000</Et>"
        "<RT>1</RT><S>0</S><P>86401</P><PU>3</PU></Tag></Q>"
    )

    assert expected == res


def test_generate_sql_query(aspen_handler: AspenHandlerWeb) -> None:
    res = aspen_handler.generate_sql_query(
        datasource=None,
        connection_string="myconnstring",
        query="myquery",
        max_rows=9999,
    )
    expected = (
        '<SQL c="myconnstring" m="9999" to="30" s="1">' "<![CDATA[myquery]]></SQL>"
    )
    assert res == expected
    res = aspen_handler.generate_sql_query(
        datasource="mydatasource",
        query="myquery",
        max_rows=9999,
        connection_string=None,
    )
    expected = (
        '<SQL t="SQLplus" ds="mydatasource" '
        'dso="CHARINT=N;CHARFLOAT=N;CHARTIME=N;CONVERTERRORS=N" '
        'm="9999" to="30" s="1">'
        "<![CDATA[myquery]]></SQL>"
    )
    assert res == expected


def test_initialize_connectionstring(aspen_handler: AspenHandlerWeb) -> None:
    aspen_handler.initialize_connectionstring(
        host="myhost", port=999, connection_string="myconnstr"
    )
    assert aspen_handler._connection_string == "myconnstr"
    aspen_handler.initialize_connectionstring(
        host="myhost",
        port=999,
    )
    assert aspen_handler._connection_string == (
        "DRIVER=AspenTech SQLPlus;HOST=myhost;PORT=999;"
        "CHARINT=N;CHARFLOAT=N;CHARTIME=N;CONVERTERRORS=N"
    )
