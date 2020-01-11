from redgraph import types


def test_handle():
    assert types.handle(b"a", b"b", b"c") == b"a:b:c"
