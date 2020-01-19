import uuid
import asyncio
from typing import List

from redgraph.redis import awaitable
from redgraph.types import Transaction, Connection
from redgraph.common import handle
from redgraph.types import ID


@awaitable
def relate(
    conn: Transaction, subject: ID, predicate: ID, object: ID
) -> List[asyncio.Future]:
    return [
        conn.sadd(handle(b"sp", subject.bytes), predicate.bytes),
        conn.sadd(handle(b"ps", predicate.bytes), subject.bytes),
        conn.sadd(handle(b"spo", subject.bytes, predicate.bytes), object.bytes),
        conn.sadd(handle(b"so", subject.bytes), object.bytes),
        conn.sadd(handle(b"os", object.bytes), subject.bytes),
        conn.sadd(handle(b"sop", subject.bytes, object.bytes), predicate.bytes),
        conn.sadd(handle(b"po", predicate.bytes), object.bytes),
        conn.sadd(handle(b"op", object.bytes), predicate.bytes),
        conn.sadd(handle(b"pos", predicate.bytes, object.bytes), subject.bytes),
    ]


@awaitable
def unrelate(
    conn: Transaction, subject: ID, predicate: ID, object: ID,
) -> List[asyncio.Future]:
    return [
        conn.srem(handle(b"sp", subject.bytes), predicate.bytes),
        conn.srem(handle(b"ps", predicate.bytes), subject.bytes),
        conn.srem(handle(b"spo", subject.bytes, predicate.bytes), object.bytes),
        conn.srem(handle(b"so", subject.bytes), object.bytes),
        conn.srem(handle(b"os", object.bytes), subject.bytes),
        conn.srem(handle(b"sop", subject.bytes, object.bytes), predicate.bytes),
        conn.srem(handle(b"po", predicate.bytes), object.bytes),
        conn.srem(handle(b"op", object.bytes), predicate.bytes),
        conn.srem(handle(b"pos", predicate.bytes, object.bytes), subject.bytes),
    ]


async def sp(conn: Connection, subject: ID, anchors: List[str] = [],) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"sp", subject.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def so(conn: Connection, subject: ID, anchors: List[str] = [],) -> List[ID]:
    objects = await conn.sinter(
        handle(b"so", subject.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def ps(conn: Connection, predicate: ID, anchors: List[str] = [],) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"ps", predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def po(conn: Connection, predicate: ID, anchors: List[str] = [],) -> List[ID]:
    objects = await conn.sinter(
        handle(b"po", predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def os(conn: Connection, object: ID, anchors: List[str] = [],) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"os", object.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def op(conn: Connection, object: ID, anchors: List[str] = [],) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"op", object.bytes), *[handle("anchor", anchor) for anchor in anchors]
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def spo(
    conn: Connection, subject: ID, predicate: ID, anchors: List[str] = []
) -> List[ID]:
    objects = await conn.sinter(
        handle(b"spo", subject.bytes, predicate.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in objects]


async def pso(
    conn: Connection, predicate: ID, subject: ID, anchors: List[str] = []
) -> List[ID]:
    objects = await spo(conn, subject, predicate, anchors)
    return objects


async def sop(
    conn: Connection, subject: ID, object: ID, anchors: List[str] = []
) -> List[ID]:
    predicates = await conn.sinter(
        handle(b"sop", subject.bytes, object.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in predicates]


async def osp(
    conn: Connection, object: ID, subject: ID, anchors: List[str] = []
) -> List[ID]:
    predicates = await sop(conn, subject, object, anchors)
    return predicates


async def pos(
    conn: Connection, predicate: ID, object: ID, anchors: List[str] = []
) -> List[ID]:
    subjects = await conn.sinter(
        handle(b"pos", predicate.bytes, object.bytes),
        *[handle("anchor", anchor) for anchor in anchors],
    )
    return [uuid.UUID(bytes=id) for id in subjects]


async def ops(
    conn: Connection, object: ID, predicate: ID, anchors: List[str] = []
) -> List[ID]:
    subjects = await pos(conn, predicate, object, anchors)
    return subjects
