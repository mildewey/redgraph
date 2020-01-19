import json
import pytest

from redgraph import entity
from redgraph.common import handle, MissingFieldError


@pytest.mark.asyncio
async def test_crud(redis_conn):
    key = await entity.create(redis_conn, dict(a=5, b=6))
    vals = await entity.read(redis_conn, key)
    assert vals["a"] == 5
    assert vals["b"] == 6

    await entity.replace(redis_conn, key, dict(a=3, c=55))
    vals = await entity.read(redis_conn, key)
    assert vals["a"] == 3
    assert vals["c"] == 55
    assert "b" not in vals

    await entity.delete(redis_conn, key)
    vals = await entity.read(redis_conn, key)
    assert vals is None


@pytest.mark.asyncio
async def test_field(redis_conn):
    expected = dict(
        a=1,
        b="b",
        c=False,
        d=None,
        e=1.4,
        index=dict(f="hello", g=dict(h="goodbye"), i=[1, 2, 3, 4]),
    )
    key = await entity.create(
        redis_conn,
        expected,
        ["index"],
        ["index", "f"],
        ["index", "g"],
        ["index", "i"],
        ["index", "i", 2],
    )
    assert expected["index"] == await entity.field(redis_conn, key, "index")
    assert expected["a"] == await entity.field(redis_conn, key, "a")
    assert expected["b"] == await entity.field(redis_conn, key, "b")
    assert expected["c"] == await entity.field(redis_conn, key, "c")
    assert expected["d"] == await entity.field(redis_conn, key, "d")
    assert expected["e"] == await entity.field(redis_conn, key, "e")
    assert expected["index"]["f"] == await entity.field(redis_conn, key, "index", "f")
    assert expected["index"]["g"] == await entity.field(redis_conn, key, "index", "g")
    assert expected["index"]["g"]["h"] == await entity.field(
        redis_conn, key, "index", "g", "h"
    )
    assert expected["index"]["i"] == await entity.field(redis_conn, key, "index", "i")
    assert expected["index"]["i"][0] == await entity.field(
        redis_conn, key, "index", "i", 0
    )
    assert expected["index"]["i"][2] == await entity.field(
        redis_conn, key, "index", "i", 2
    )

    redis_conn.hdel(key.bytes, handle("entity"))
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "a")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "b")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "c")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "d")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "e")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "index", "g", "h")
    with pytest.raises(MissingFieldError):
        await entity.field(redis_conn, key, "index", "i", 0)
    assert expected["index"] == await entity.field(redis_conn, key, "index")
    assert expected["index"]["f"] == await entity.field(redis_conn, key, "index", "f")
    assert expected["index"]["g"] == await entity.field(redis_conn, key, "index", "g")
    assert expected["index"]["i"] == await entity.field(redis_conn, key, "index", "i")
    assert expected["index"]["i"][2] == await entity.field(
        redis_conn, key, "index", "i", 2
    )
