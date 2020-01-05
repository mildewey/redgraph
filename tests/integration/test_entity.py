import pytest

from redgraph import entity


@pytest.mark.asyncio
async def test_create(redis_conn):
    key = await entity.create(redis_conn, dict(a=5, b=6))
    vals = await redis_conn.hmget(key.bytes, b"a", b"b", encoding="utf-8")
    assert int(vals[0]) == 5
    assert int(vals[1]) == 6
