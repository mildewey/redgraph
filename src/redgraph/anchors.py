import uuid
from typing import List, Optional, AnyStr

from redgraph import redis, graph
from redgraph.types import ID, handle


async def add(conn: redis.Connection, anchor: str, *entities: ID) -> int:
    key = handle("anchor", anchor)
    if len(entities) != 0:
        return await conn.sadd(key, *[ent.bytes for ent in entities])
    return 0


async def remove(conn: redis.Connection, anchor: str, *entities: ID) -> int:
    key = handle("anchor", anchor)
    if len(entities) != 0:
        return await conn.srem(key, *[ent.bytes for ent in entities])
    return 0


async def count(conn: redis.Connection, anchor: str) -> int:
    key = handle("anchor", anchor)
    return await conn.scard(key)


async def union(
    conn: redis.Connection, *anchors: str, store: Optional[str] = None
) -> List[ID]:
    if len(anchors) == 0:
        return []
    if store is None:
        members = await conn.sunion(*[handle("anchor", anchor) for anchor in anchors])
    else:
        await conn.sunionstore(
            handle("anchor", store), *[handle("anchor", anchor) for anchor in anchors]
        )
        members = await conn.smembers(handle("anchor", store))
    return [uuid.UUID(bytes=member) for member in members]


async def intersection(
    conn: redis.Connection, *anchors: str, store: Optional[str] = None
) -> List[ID]:
    if len(anchors) == 0:
        return []
    if store is None:
        members = await conn.sinter(*[handle("anchor", anchor) for anchor in anchors])
    else:
        await conn.sinterstore(
            handle("anchor", store), *[handle("anchor", anchor) for anchor in anchors]
        )
        members = await conn.smembers(handle("anchor", store))
    return [uuid.UUID(bytes=member) for member in members]


async def difference(
    conn: redis.Connection, base: str, *remove: str, store: Optional[str] = None
) -> List[ID]:
    if store is None:
        members = await conn.sdiff(
            handle("anchor", base), *[handle("anchor", key) for key in remove]
        )
    else:
        await conn.sdiffstore(
            handle("anchor", store),
            handle("anchor", base),
            *[handle("anchor", key) for key in remove],
        )
        members = await conn.smembers(handle("anchor", store))
    return [uuid.UUID(bytes=member) for member in members]


async def members(conn: redis.Connection, anchor: str) -> List[ID]:
    members = await conn.smembers(handle("anchor", anchor))
    return [uuid.UUID(bytes=member) for member in members]


async def is_member(conn: redis.Connection, anchor: str, entity: ID) -> List[ID]:
    mem = await conn.sismember(handle("anchor", anchor), entity.bytes)
    return bool(mem)
