from tagreader.web_handlers import list_pi_servers


def test_list_all_pi_servers():
    res = list_pi_servers()
    assert isinstance(res, list)
    assert len(res) >= 1
    for r in res:
        assert isinstance(r, str)
        assert 3 <= len(r)
