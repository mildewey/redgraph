import asyncio

import pytest
from redgraph import redis


@pytest.yield_fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def redis_conn():
    async with redis.redis("localhost", 7357) as conn:
        yield conn
