import uuid
from typing import List

from redgraph import redis, graph
from redgraph.common import handle
from redgraph.types import ID
from redgraph.redis import awaitable


@awaitable
def add(conn: redis.Transaction, anchor: str, *entities: ID) -> int:
    key = handle("anchor", anchor)
    if len(entities) != 0:
        return [conn.sadd(key, *[ent.bytes for ent in entities])]
    return []


@awaitable
def remove(conn: redis.Transaction, anchor: str, *entities: ID) -> int:
    key = handle("anchor", anchor)
    if len(entities) != 0:
        return [conn.srem(key, *[ent.bytes for ent in entities])]
    return []


@awaitable
def unionstore(
    conn: redis.Transaction, store: str, *anchors: str, overwrite: bool = True
):
    unite_anchors = [handle("anchor", anchor) for anchor in anchors]
    new_anchor = handle("anchor", store)
    if not overwrite:
        unite_anchors.append(new_anchor)
    return [conn.sunionstore(new_anchor, *unite_anchors)]


@awaitable
def interstore(
    conn: redis.Transaction, store: str, *anchors: str, overwrite: bool = True
):
    intersect_anchors = [handle("anchor", anchor) for anchor in anchors]
    new_anchor = handle("anchor", store)
    if not overwrite:
        intersect_anchors.append(new_anchor)
    return [conn.sinterstore(new_anchor, *intersect_anchors)]


@awaitable
def diffstore(
    conn: redis.Transaction, store: str, *anchors: str, overwrite: bool = True
):
    diff_anchors = [handle("anchor", anchor) for anchor in anchors]
    new_anchor = handle("anchor", store)
    if not overwrite:
        diff_anchors.append(new_anchor)
    return [conn.sdiffstore(new_anchor, *diff_anchors)]


async def union(conn: redis.Connection, *anchors: str) -> List[ID]:
    if len(anchors) == 0:
        return []
    members = await conn.sunion(*[handle("anchor", anchor) for anchor in anchors])
    return [uuid.UUID(bytes=member) for member in members]


async def intersection(conn: redis.Connection, *anchors: str) -> List[ID]:
    if len(anchors) == 0:
        return []
    members = await conn.sinter(*[handle("anchor", anchor) for anchor in anchors])
    return [uuid.UUID(bytes=member) for member in members]


async def difference(conn: redis.Connection, base: str, *remove: str) -> List[ID]:
    members = await conn.sdiff(
        handle("anchor", base), *[handle("anchor", key) for key in remove]
    )
    return [uuid.UUID(bytes=member) for member in members]


async def members(conn: redis.Connection, anchor: str) -> List[ID]:
    members = await conn.smembers(handle("anchor", anchor))
    return [uuid.UUID(bytes=member) for member in members]


async def is_member(conn: redis.Connection, anchor: str, entity: ID) -> bool:
    mem = await conn.sismember(handle("anchor", anchor), entity.bytes)
    return bool(mem)


async def count(conn: redis.Connection, anchor: str) -> int:
    key = handle("anchor", anchor)
    return await conn.scard(key)
