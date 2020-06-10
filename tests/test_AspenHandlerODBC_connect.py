import pytest
import os

from tagreader.odbc_handlers import list_aspen_servers

is_GITHUBACTION = "GITHUB_ACTION" in os.environ

if is_GITHUBACTION:
    pytest.skip(
        "All tests in module require connection to Aspen server",
        allow_module_level=True
    )


def test_list_all_aspen_servers():
    res = list_aspen_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], str)
    for r in res:
        assert 3 <= len(r) <= 20
