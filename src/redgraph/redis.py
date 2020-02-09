import asyncio
from functools import singledispatch
from contextlib import asynccontextmanager
from typing import Callable

import aioredis

from redgraph.types import Connection, Transaction, Pipeline, Future, List


@asynccontextmanager
async def redis(url: str, port: int = 6379) -> Connection:
    for x in range(5):
        try:
            redis = await aioredis.create_redis_pool((url, port))
        except aioredis.errors.ConnectionClosedError:
            await asyncio.sleep(0.1)
            continue
        else:
            break
    else:
        raise Exception("Unable to make connection with redis.")
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


@asynccontextmanager
async def pipeline(redis: Connection) -> Pipeline:
    pipe = redis.pipeline()
    yield pipe
    await pipe.execute()


def awaitable(func: Callable):
    @singledispatch
    async def wrapper(conn: Connection, *args, **kwargs) -> None:
        async with transaction(conn) as tr:
            futures = func(tr, *args, **kwargs)
        asyncio.gather(*futures)

    @wrapper.register
    def _(conn: Transaction, *args, **kwargs) -> List[Future]:
        return func(conn, *args, **kwargs)

    return wrapper
