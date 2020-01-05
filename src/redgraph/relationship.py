import uuid
import asyncio

from redgraph import redis


def _key(*keys: bytes) -> bytes:
    return b":".join(keys)


async def relate(
    conn: redis.Connection, subject: uuid.UUID, predicate: uuid.UUID, object: uuid.UUID
) -> None:
    async with redis.transaction(conn) as tr:
        futures = [
            tr.sadd(_key(b"sp", subject.bytes), predicate.bytes),
            tr.sadd(_key(b"ps", predicate.bytes), subject.bytes),
            tr.sadd(_key(b"spo", subject.bytes, predicate.bytes), object.bytes),
            tr.sadd(_key(b"so", subject.bytes), object.bytes),
            tr.sadd(_key(b"os", object.bytes), subject.bytes),
            tr.sadd(_key(b"sop", subject.bytes, object.bytes), predicate.bytes),
            tr.sadd(_key(b"po", predicate.bytes), object.bytes),
            tr.sadd(_key(b"op", object.bytes), predicate.bytes),
            tr.sadd(_key(b"pos", predicate.bytes, object.bytes), subject.bytes),
        ]
    await asyncio.gather(*futures)
