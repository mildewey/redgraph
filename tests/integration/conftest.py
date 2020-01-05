import pytest
from redgraph import redis


@pytest.fixture
async def redis_conn():
    async with redis.redis("localhost", 7357) as conn:
        yield conn
