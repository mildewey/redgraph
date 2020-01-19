import json
import pytest

from redgraph import entity


@pytest.mark.asyncio
async def test_crud(redis_conn):
    key = await entity.create(redis_conn, dict(a=5, b=6))
    vals = await entity.read(redis_conn, key)
    assert vals["a"] == 5
    assert vals["b"] == 6

    await entity.update(redis_conn, key, dict(a=3, c=55))
    vals = await entity.read(redis_conn, key)
    assert vals["a"] == 3
    assert vals["c"] == 55
    assert "b" not in vals

    await entity.delete(redis_conn, key)
    vals = await entity.read(redis_conn, key)
    assert vals is None
