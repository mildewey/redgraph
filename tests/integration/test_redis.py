import pytest


@pytest.mark.asyncio
async def test_redis(redis_conn):
    assert b"PONG" == await redis_conn.ping()
