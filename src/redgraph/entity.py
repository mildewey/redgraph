import json
import uuid
from functools import singledispatch

from redgraph import redis
from redgraph.types import Document, ID, Connection, Key, Union, List


async def create(conn: Connection, entity: str) -> ID:
    id = uuid.uuid1()
    await update(conn, id, entity)
    return id


async def read(conn: Connection, id: ID) -> str:
    entity = await conn.get(id.bytes, encoding="utf-8")
    return entity


async def update(conn: Connection, id: ID, updates: str) -> None:
    await conn.set(id.bytes, bytes(updates, "utf-8"))


async def delete(conn: Connection, id: ID) -> None:
    await conn.delete(id.bytes)
