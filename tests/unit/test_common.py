from redgraph import common


def test_handle():
    assert common.handle(b"a", b"b", b"c") == b"a:b:c"
