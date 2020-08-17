import pytest
import os
import pandas as pd
from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.odbc_handlers import list_pi_sources
from tagreader.clients import IMSClient, list_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

SOURCE = "PINO"

TAGS = {
    "Float32": "BA:CONC.1",
    "Digital": "BA:ACTIVE.1",
    "Int32": "CDEP158",
}

START_TIME = "2020-04-01 11:05:00"
STOP_TIME = "2020-04-01 12:05:00"
SAMPLE_TIME = 60


@pytest.fixture()
def Client():
    c = IMSClient(SOURCE, "pi")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


def test_list_all_pi_sources():
    res = list_pi_sources()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r)


def test_list_sources_pi():
    res = list_sources("pi")
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r)


def test_search(Client):
    res = Client.search("BA:*.1")
    assert 5 == len(res)
    [taglist, desclist] = zip(*res)
    assert "BA:CONC.1" in taglist
    assert desclist[taglist.index("BA:CONC.1")] == "Concentration Reactor 1"

    res = Client.search(tag="BA:*.1")
    assert 5 == len(res)
    res = Client.search(desc="Batch Active Reactor 1")
    assert 1 == len(res)
    res = Client.search("BA*.1", "*Active*")
    assert 1 == len(res)


@pytest.mark.parametrize(
    ("read_type", "size"),
    [
        ("RAW", 5),
        # pytest.param(
        #     "SHAPEPRESERVING", 0, marks=pytest.mark.skip(reason="Not implemented")
        # ),
        ("INT", 61),
        ("MIN", 60),
        ("MAX", 60),
        ("RNG", 60),
        ("AVG", 60),
        ("VAR", 60),
        ("STD", 60),
        # pytest.param("COUNT", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("GOOD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("BAD", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("TOTAL", 0, marks=pytest.mark.skip(reason="Not implemented")),
        # pytest.param("SUM", 0, marks=pytest.mark.skip(reason="Not implemented")),
        ("SNAPSHOT", 1),
    ],
)
def test_read(Client, read_type, size):
    if read_type == "SNAPSHOT":
        df = Client.read(TAGS["Float32"], read_type=getattr(ReaderType, read_type))
    else:
        df = Client.read(
            TAGS["Float32"],
            start_time=START_TIME,
            end_time=STOP_TIME,
            ts=SAMPLE_TIME,
            read_type=getattr(ReaderType, read_type),
        )
    assert df.shape == (size, 1)
    if read_type not in ["SNAPSHOT", "RAW"]:
        assert df.index[0] == ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] == df.index[0] + (size - 1) * pd.Timedelta(
            SAMPLE_TIME, unit="s"
        )
    elif read_type in "RAW":
        assert df.index[0] >= ensure_datetime_with_tz(START_TIME)
        assert df.index[-1] <= ensure_datetime_with_tz(STOP_TIME)


def test_digitalread_is_one_or_zero(Client):
    tag = TAGS["Digital"]
    df = Client.read(
        tag, start_time=START_TIME, end_time=STOP_TIME, ts=600, read_type=ReaderType.INT
    )
    assert df[tag].max() == 1
    assert df[tag].min() == 0
    assert df[tag].isin([0, 1]).all()


def test_get_unit(Client):
    res = Client.get_units(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "DEG. C"
    assert res[TAGS["Digital"]] == "STATE"
    assert res[TAGS["Int32"]] == ""


def test_get_description(Client):
    res = Client.get_descriptions(list(TAGS.values()))
    assert res[TAGS["Float32"]] == "Concentration Reactor 1"
    assert res[TAGS["Digital"]] == "Batch Active Reactor 1"
    assert res[TAGS["Int32"]] == "Light Naphtha End Point"


def test_from_DST_folds_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = Client.read([tag], interval[0], interval[1], 600)
    assert len(df) == (4 + 1) * 6 + 1
    # Time exists inside fold:
    assert (
        df[tag].loc["2017-10-29 01:10:00+02:00":"2017-10-29 01:50:00+02:00"].size == 5
    )
    # Time inside fold is always included:
    assert (
        df.loc["2017-10-29 01:50:00":"2017-10-29 03:10:00"].size == 2 + (1 + 1) * 6 + 1
    )


def test_to_DST_skips_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = TAGS["Float32"]
    interval = ["2018-03-25 00:30:00", "2018-03-25 03:30:00"]
    df = Client.read([tag], interval[0], interval[1], 600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6
    )


def test_read_unknown_tag(Client):
    with pytest.warns(UserWarning):
        df = Client.read(["sorandomitcantexist"], START_TIME, STOP_TIME)
    assert len(df.index) == 0
    with pytest.warns(UserWarning):
        df = Client.read([TAGS['Float32'], "sorandomitcantexist"], START_TIME, STOP_TIME)
    assert len(df.index) > 0
    assert len(df.columns == 1)
