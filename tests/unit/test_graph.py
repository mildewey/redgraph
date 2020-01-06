from redgraph import graph


def test__key():
    assert graph._key(b"a", b"b", b"c") == b"a:b:c"
