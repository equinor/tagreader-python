import pytest
import os

from tagreader.clients import IMSClient, list_sources
from tagreader.odbc_handlers import list_aspen_sources

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True,
    )

SOURCE = "SNA"
TAGS = ["ATCAI", "ATCMIXTIME1"]


@pytest.fixture()
def Client():
    c = IMSClient(SOURCE, "ip21")
    c.cache = None
    c.connect()
    yield c
    if os.path.exists(SOURCE + ".h5"):
        os.remove(SOURCE + ".h5")


def test_list_all_aspen_sources():
    res = list_aspen_sources()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20


def test_list_sources_aspen():
    res = list_sources("aspen")
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20
