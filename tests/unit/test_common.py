from redgraph import common


def test_handle():
    assert common.handle("a", "b", 5) == b"a:b:5"
