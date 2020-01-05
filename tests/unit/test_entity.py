import pytest

from redgraph import entity


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
    assert expected == entity._entity_list(
        dict(
            i=5, f=3.14, b=False, n=None, s="four", d=dict(a="a", b="b"), li=[1, 2, 3],
        )
    )

    class NotAllowed:
        pass

    with pytest.raises(TypeError):
        entity._entity_list(dict(a=NotAllowed()))
    # with pytest.raises(TypeError):
    #     entity._entity({NotAllowed(): 1})
