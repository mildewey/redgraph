from redgraph import redis

def test__key():
    assert redis._key(b"a", b"b", b"c") == b"a:b:c"
