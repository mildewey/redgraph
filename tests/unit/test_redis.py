import pytest

from redgraph import redis


def test__key():
    assert redis._key(b"a", b"b", b"c") == b"a:b:c"


def test__entity():
    expected = [
        b"i",
        b"5",
        b"f",
        b"3.14",
        b"b",
        b"false",
        b"n",
        b"null",
        b"s",
        b'"four"',
        b"d",
        b'{"a": "a", "b": "b"}',
        b"li",
        b"[1, 2, 3]",
    ]
    assert expected == redis._entity(
        dict(
            i=5, f=3.14, b=False, n=None, s="four", d=dict(a="a", b="b"), li=[1, 2, 3],
        )
    )

    class NotAllowed:
        pass

    with pytest.raises(TypeError):
        redis._entity(dict(a=NotAllowed()))
    # with pytest.raises(TypeError):
    #     redis._entity({NotAllowed(): 1})
