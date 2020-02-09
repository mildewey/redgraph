from typing import List

from redgraph import redis, graph
from redgraph.common import handle, serialize, deserialize
from redgraph.types import ID, Value, Transaction, Connection
from redgraph.redis import awaitable


@awaitable
def set(conn: Transaction, prop: str, value: Value, *entities: ID) -> None:
    key = handle("property", prop)
    val = serialize(value)
    if len(entities) != 0:
        return [conn.hmset(key, *[p for ent in entities for p in [ent.bytes, val]])]
    return []


@awaitable
def remove(conn: Transaction, prop: str, *entities: ID) -> None:
    key = handle("property", prop)
    if len(entities) != 0:
        return [conn.hdel(key, *[ent.bytes for ent in entities])]
    return []


async def get(conn: Connection, prop: str, *entities: ID) -> List[Value]:
    key = handle("property", prop)
    if len(entities) != 0:
        raw = await conn.hmget(key, *[ent.bytes for ent in entities], encoding="utf-8")
        return [deserialize(val) for val in raw]
    return []
