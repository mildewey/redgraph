from contextlib import asynccontextmanager
from typing import Union

import aioredis

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]


@asynccontextmanager
async def redis(url: str, port: int = 6379) -> Connection:
    redis = await aioredis.create_redis_pool((url, port))
    try:
        yield redis
    finally:
        redis.close()
        await redis.wait_closed()


@asynccontextmanager
async def transaction(redis: Connection) -> Transaction:
    tr = redis.multi_exec()
    yield tr
    await tr.execute()
