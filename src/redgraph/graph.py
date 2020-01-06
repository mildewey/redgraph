import uuid
import asyncio
from typing import Optional, List, Union

from redgraph import redis, anchors

ID = Union[uuid.UUID]


def _key(*keys: bytes) -> bytes:
    return b":".join(keys)


async def relate(
    conn: redis.Connection, subject: ID, predicate: ID, object: ID
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


async def unrelate(
    conn: redis.Connection, subject: ID, predicate: ID, object: ID,
) -> None:
    async with redis.transaction(conn) as tr:
        futures = [
            tr.srem(_key(b"sp", subject.bytes), predicate.bytes),
            tr.srem(_key(b"ps", predicate.bytes), subject.bytes),
            tr.srem(_key(b"spo", subject.bytes, predicate.bytes), object.bytes),
            tr.srem(_key(b"so", subject.bytes), object.bytes),
            tr.srem(_key(b"os", object.bytes), subject.bytes),
            tr.srem(_key(b"sop", subject.bytes, object.bytes), predicate.bytes),
            tr.srem(_key(b"po", predicate.bytes), object.bytes),
            tr.srem(_key(b"op", object.bytes), predicate.bytes),
            tr.srem(_key(b"pos", predicate.bytes, object.bytes), subject.bytes),
        ]
    await asyncio.gather(*futures)


async def sp(
    conn: redis.Connection, subject: ID, anchors: List[bytes] = [],
) -> List[ID]:
    predicates = await conn.sinter(_key(b"sp", subject.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in predicates]


async def so(
    conn: redis.Connection, subject: ID, anchors: List[bytes] = [],
) -> List[ID]:
    objects = await conn.sinter(_key(b"so", subject.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in objects]


async def ps(
    conn: redis.Connection, predicate: ID, anchors: List[bytes] = [],
) -> List[ID]:
    subjects = await conn.sinter(_key(b"ps", predicate.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in subjects]


async def po(
    conn: redis.Connection, predicate: ID, anchors: List[bytes] = [],
) -> List[ID]:
    objects = await conn.sinter(_key(b"po", predicate.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in objects]


async def os(
    conn: redis.Connection, object: ID, anchors: List[bytes] = [],
) -> List[ID]:
    subjects = await conn.sinter(_key(b"os", object.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in subjects]


async def op(
    conn: redis.Connection, object: ID, anchors: List[bytes] = [],
) -> List[ID]:
    predicates = await conn.sinter(_key(b"op", object.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in predicates]


async def spo(
    conn: redis.Connection, subject: ID, predicate: ID, anchors: List[bytes] = []
) -> List[ID]:
    objects = await conn.sinter(_key(b"spo", subject.bytes, predicate.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in objects]


async def pso(
    conn: redis.Connection, predicate: ID, subject: ID, anchors: List[bytes] = []
) -> List[ID]:
    objects = await spo(conn, subject, predicate, anchors)
    return objects


async def sop(
    conn: redis.Connection, subject: ID, object: ID, anchors: List[bytes] = []
) -> List[ID]:
    predicates = await conn.sinter(_key(b"sop", subject.bytes, object.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in predicates]


async def osp(
    conn: redis.Connection, object: ID, subject: ID, anchors: List[bytes] = []
) -> List[ID]:
    predicates = await sop(conn, subject, object, anchors)
    return predicates


async def pos(
    conn: redis.Connection, predicate: ID, object: ID, anchors: List[bytes] = []
) -> List[ID]:
    subjects = await conn.sinter(_key(b"pos", predicate.bytes, object.bytes), *anchors)
    return [uuid.UUID(bytes=id) for id in subjects]


async def ops(
    conn: redis.Connection, object: ID, predicate: ID, anchors: List[bytes] = []
) -> List[ID]:
    subjects = await pos(conn, predicate, object, anchors)
    return subjects
