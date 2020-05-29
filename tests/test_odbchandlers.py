import os
import pytest

from tagreader import IMSClient
from tagreader.odbc_handlers import PIHandlerODBC, AspenHandlerODBC

is_GITHUBACTION = "GITHUB_ACTION" in os.environ
is_AZUREPIPELINE = "TF_BUILD" in os.environ
is_CI = is_GITHUBACTION or is_AZUREPIPELINE


@pytest.mark.skipif(
    is_GITHUBACTION, reason="ODBC drivers unavailable in GitHub Actions"
)
def test_init_drivers():
    with pytest.raises(ValueError):
        c = IMSClient("xyz")
    with pytest.raises(ValueError):
        c = IMSClient("sNa", "pi")
    with pytest.raises(ValueError):
        c = IMSClient("Ono-imS", "aspen")
    with pytest.raises(ValueError):
        c = IMSClient("ono-ims", "aspen")
    with pytest.raises(ValueError):
        c = IMSClient("sna", "pi")
    c = IMSClient("onO-iMs", "pi")
    assert isinstance(c.handler, PIHandlerODBC)
    c = IMSClient("snA", "aspen")
    assert isinstance(c.handler, AspenHandlerODBC)
