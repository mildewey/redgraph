import json
import uuid
import asyncio
from contextlib import asynccontextmanager
from typing import Union

import aioredis

Connection = Union[aioredis.ConnectionsPool]
Transaction = Union[aioredis.commands.MultiExec]


@asynccontextmanager
async def redis(url: str) -> Connection:
    redis = await aioredis.create_redis_pool(url)
    try:
        yield redis
    finally:
        redis.close()
        await redis.wait_closed()


@asynccontextmanager
async def transaction(redis: Connection) -> Transaction:
    tr = redis.multi_exec()
    yield tr
    await tr.excute()


async def create(redis: Connection, entity: dict) -> bytes:
    id = uuid.uuid1()

    await redis.hmset(
        id.bytes,
        *[p for field, value in entity.items() for p in [field, json.dumps(value)]]
    )

    return id.bytes


def _key(*keys: bytes) -> bytes:
    return b":".join(keys)


async def relate(
    redis: Connection, subject: bytes, predicate: bytes, object: bytes
) -> None:
    async with transaction(redis) as tr:
        futures = [
            tr.sadd(_key(b"sp", subject), predicate),
            tr.sadd(_key(b"ps", predicate), subject),
            tr.sadd(_key(b"spo", subject, predicate), object),
            tr.sadd(_key(b"so", subject), object),
            tr.sadd(_key(b"os", object), subject),
            tr.sadd(_key(b"sop", subject, object), predicate),
            tr.sadd(_key(b"po", predicate), object),
            tr.sadd(_key(b"op", object), predicate),
            tr.sadd(_key(b"pos", predicate, object), subject),
        ]
    await asyncio.gather(futures)
