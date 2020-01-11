import uuid
import asyncio
from typing import List

from redgraph import redis
from redgraph.types import ID, Handle, handle


async def relate(
    conn: redis.Connection, subject: ID, predicate: ID, object: ID
) -> None:
    async with redis.transaction(conn) as tr:
        futures = [
            tr.sadd(handle(b"sp", subject.bytes), predicate.bytes),
            tr.sadd(handle(b"ps", predicate.bytes), subject.bytes),
            tr.sadd(handle(b"spo", subject.bytes, predicate.bytes), object.bytes),
            tr.sadd(handle(b"so", subject.bytes), object.bytes),
            tr.sadd(handle(b"os", object.bytes), subject.bytes),
            tr.sadd(handle(b"sop", subject.bytes, object.bytes), predicate.bytes),
            tr.sadd(handle(b"po", predicate.bytes), object.bytes),
            tr.sadd(handle(b"op", object.bytes), predicate.bytes),
            tr.sadd(handle(b"pos", predicate.bytes, object.bytes), subject.bytes),
        ]
    await asyncio.gather(*futures)


async def unrelate(
    conn: redis.Connection, subject: ID, predicate: ID, object: ID,
) -> None:
    async with redis.transaction(conn) as tr:
        futures = [
            tr.srem(handle(b"sp", subject.bytes), predicate.bytes),
            tr.srem(handle(b"ps", predicate.bytes), subject.bytes),
            tr.srem(handle(b"spo", subject.bytes, predicate.bytes), object.bytes),
            tr.srem(handle(b"so", subject.bytes), object.bytes),
            tr.srem(handle(b"os", object.bytes), subject.bytes),
            tr.srem(handle(b"sop", subject.bytes, object.bytes), predicate.bytes),
            tr.srem(handle(b"po", predicate.bytes), object.bytes),
            tr.srem(handle(b"op", object.bytes), predicate.bytes),
            tr.srem(handle(b"pos", predicate.bytes, object.bytes), subject.bytes),
        ]
    await asyncio.gather(*futures)


async def sp(conn: redis.Connection, subject: ID, anchors: List[str] = [],) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"sp", subject.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def so(conn: redis.Connection, subject: ID, anchors: List[str] = [],) -> List[ID]:
    objects = await conn.sinter(
        handle(b"so", subject.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def ps(
    conn: redis.Connection, predicate: ID, anchors: List[str] = [],
) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"ps", predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def po(
    conn: redis.Connection, predicate: ID, anchors: List[str] = [],
) -> List[ID]:
    objects = await conn.sinter(
        handle(b"po", predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def os(conn: redis.Connection, object: ID, anchors: List[str] = [],) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"os", object.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def op(conn: redis.Connection, object: ID, anchors: List[str] = [],) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"op", object.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def spo(
    conn: redis.Connection, subject: ID, predicate: ID, anchors: List[str] = []
) -> List[ID]:
    objects = await conn.sinter(
        handle(b"spo", subject.bytes, predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def pso(
    conn: redis.Connection, predicate: ID, subject: ID, anchors: List[str] = []
) -> List[ID]:
    objects = await spo(conn, subject, predicate, anchors)
    return objects


async def sop(
    conn: redis.Connection, subject: ID, object: ID, anchors: List[str] = []
) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"sop", subject.bytes, object.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def osp(
    conn: redis.Connection, object: ID, subject: ID, anchors: List[str] = []
) -> List[ID]:
    predicates = await sop(conn, subject, object, anchors)
    return predicates


async def pos(
    conn: redis.Connection, predicate: ID, object: ID, anchors: List[str] = []
) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"pos", predicate.bytes, object.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def ops(
    conn: redis.Connection, object: ID, predicate: ID, anchors: List[str] = []
) -> List[ID]:
    subjects = await pos(conn, predicate, object, anchors)
    return subjects
