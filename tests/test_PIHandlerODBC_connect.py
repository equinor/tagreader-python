import pytest
import os
import pandas as pd
from tagreader.utils import ReaderType, ensure_datetime_with_tz
from tagreader.odbc_handlers import list_pi_sources
from tagreader.clients import IMSClient

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to PI server", allow_module_level=True
    )

SOURCE = "PINO"

tags = {
    "Float32": "BA:CONC.1",
    "Digital": "BA:ACTIVE.1",
    "Int32": "CDEP158",
}

interval = ["2020-04-01 11:05:00", "2020-04-01 12:05:00"]


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


def test_search(Client):
    res = Client.search_tag("BA:*.1")
    assert 5 == len(res)
    [taglist, desclist] = zip(*res)
    assert "BA:CONC.1" in taglist
    assert desclist[taglist.index("BA:CONC.1")] == "Concentration Reactor 1"

    res = Client.search_tag(tag="BA:*.1")
    assert 5 == len(res)
    res = Client.search_tag(desc="Batch Active Reactor 1")
    assert 1 == len(res)
    res = Client.search_tag("BA*.1", "*Active*")
    assert 1 == len(res)


@pytest.mark.parametrize(
    ("read_type", "size"),
    [
        # pytest.param("RAW", 0, marks=pytest.mark.skip(reason="Not implemented")),
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
        # pytest.param("SNAPSHOT", 0, marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_read(Client, read_type, size):
    df = Client.read_tags(
        tags["Float32"], interval[0], interval[1], 60, getattr(ReaderType, read_type)
    )
    assert df.shape == (size, 1)
    assert df.index[0] == ensure_datetime_with_tz(interval[0])
    assert df.index[size - 1] == df.index[0] + (size - 1) * pd.Timedelta(60, unit="s")


def test_digitalread_is_one_or_zero(Client):
    tag = tags["Digital"]
    df = Client.read_tags(tag, interval[0], interval[1], 600, ReaderType.INT)
    assert df[tag].max() == 1
    assert df[tag].min() == 0
    assert df[tag].isin([0, 1]).all()


def test_get_unit(Client):
    res = Client.get_units(list(tags.values()))
    assert res[tags["Float32"]] == "DEG. C"
    assert res[tags["Digital"]] == "STATE"
    assert res[tags["Int32"]] == ""


def test_get_description(Client):
    res = Client.get_descriptions(list(tags.values()))
    assert res[tags["Float32"]] == "Concentration Reactor 1"
    assert res[tags["Digital"]] == "Batch Active Reactor 1"
    assert res[tags["Int32"]] == "Light Naphtha End Point"


def test_from_DST_folds_time(Client):
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")
    tag = tags["Float32"]
    interval = ["2017-10-29 00:30:00", "2017-10-29 04:30:00"]
    df = Client.read_tags([tag], interval[0], interval[1], 600)
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
    tag = tags["Float32"]
    interval = ["2018-03-25 00:30:00", "2018-03-25 03:30:00"]
    df = Client.read_tags([tag], interval[0], interval[1], 600)
    # Lose one hour:
    assert (
        df.loc["2018-03-25 01:50:00":"2018-03-25 03:10:00"].size == (2 + 1 * 6 + 1) - 6
    )
