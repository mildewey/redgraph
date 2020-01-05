import uuid

import pytest

from redgraph import redis


@pytest.mark.asyncio
async def test_redis(redis_conn):
    assert b"PONG" == await redis_conn.ping()


@pytest.mark.asyncio
async def test_create(redis_conn):
    key = await redis.create(redis_conn, dict(a=5, b=6))
    vals = await redis_conn.hmget(key.bytes, b"a", b"b", encoding="utf-8")
    assert int(vals[0]) == 5
    assert int(vals[1]) == 6


@pytest.mark.asyncio
async def test_relate(redis_conn):
    subject = uuid.uuid1()
    predicate = uuid.uuid1()
    object = uuid.uuid1()
    await redis.relate(redis_conn, subject, predicate, object)
    assert await redis_conn.sismember(
        redis._key(b"spo", subject.bytes, predicate.bytes), object.bytes
    )
