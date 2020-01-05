import uuid

import pytest

from redgraph import relationship


@pytest.mark.asyncio
async def test_relate(redis_conn):
    subject = uuid.uuid1()
    predicate = uuid.uuid1()
    object = uuid.uuid1()
    await relationship.relate(redis_conn, subject, predicate, object)
    assert await redis_conn.sismember(
        relationship._key(b"spo", subject.bytes, predicate.bytes), object.bytes
    )
