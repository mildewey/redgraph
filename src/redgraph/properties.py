import asyncio
from typing import List, Mapping

from redgraph import redis, graph
from redgraph.common import handle, serialize, deserialize
from redgraph.types import ID, Value, Transaction, Connection
from redgraph.redis import awaitable, pipeline


@awaitable
def set(conn: Transaction, *entities: ID, **properties: Value) -> None:
    if len(entities) == 0:
        return []
    futures = []
    handles = [ent.bytes for ent in entities]
    for prop, value in properties.items():
        key = handle("property", prop)
        val = serialize(value)
        futures.append(conn.hmset(key, *[p for ent in handles for p in [ent, val]]))
    return futures


@awaitable
def remove(conn: Transaction, prop: str, *entities: ID) -> None:
    key = handle("property", prop)
    if len(entities) != 0:
        return [conn.hdel(key, *[ent.bytes for ent in entities])]
    return []


async def property(conn: Connection, prop: str, *entities: ID) -> List[Value]:
    key = handle("property", prop)
    if len(entities) != 0:
        raw = await conn.hmget(key, *[ent.bytes for ent in entities], encoding="utf-8")
        return [deserialize(val) for val in raw]
    return []


async def entity(conn: Connection, entity: ID, *properties: str) -> Mapping[str, Value]:
    ent = {}
    futures = []
    async with pipeline(conn) as pipe:
        for prop in properties:
            futures.append(pipe.hget(handle("property", prop), entity.bytes))
    values = await asyncio.gather(*futures)
    return {
        prop: value
        for prop, value in zip(properties, [deserialize(val) for val in values])
    }
