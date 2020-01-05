from redgraph import relationship


def test__key():
    assert relationship._key(b"a", b"b", b"c") == b"a:b:c"
