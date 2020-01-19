import json
import uuid
import logging
from functools import singledispatch
from typing import Optional
from asyncio import Future

from redgraph import redis, anchors
from redgraph.redis import awaitable
from redgraph.common import handle, serialize, deserialize, extract, MissingFieldError
from redgraph.types import (
    Document,
    ID,
    Connection,
    Transaction,
    Future,
    List,
    Key,
    Field,
    Value,
)

logger = logging.getLogger("redgraph")


def _extract_all(entity: Document, *indices: Key) -> List[bytes]:
    indexed = []
    for index in indices:
        try:
            val = extract(entity, *index)
        except MissingFieldError as e:
            continue
        indexed.append(handle("index", *index))
        indexed.append(serialize(val))
    return indexed


@awaitable
def set(conn: Transaction, id: ID, entity: Document, *indices: Key) -> List[Future]:
    indexed = _extract_all(entity, *indices)

    return [conn.hmset(id.bytes, handle("entity"), serialize(entity), *indexed)]


async def read(conn: Connection, id: ID) -> Value:
    raw = await conn.hget(id.bytes, handle("entity"), encoding="utf-8")
    if raw is not None:
        return deserialize(raw)
    return None


@awaitable
def replace(conn: Transaction, id: ID, entity: Document, *indices: Key) -> List[Future]:
    return [*delete(conn, id), *set(conn, id, entity, *indices)]


@awaitable
def delete(conn: Connection, id: ID) -> None:
    return [conn.delete(id.bytes)]


async def field(conn: Connection, id: ID, *index: Key) -> Value:
    val = await conn.hget(id.bytes, handle("index", *index))
    if val is not None:
        logger.info(f"Missing field: {'.'.join([str(i) for i in index])}")
        return deserialize(val)
    else:
        ent = await read(conn, id)
        if ent is None:
            raise MissingFieldError("No such entity")
        return extract(ent, *index)
