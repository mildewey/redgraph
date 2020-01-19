import json
import uuid
import logging
from functools import singledispatch
from typing import Optional

from redgraph import redis, anchors
from redgraph.common import handle, serialize, deserialize, extract, MissingFieldError
from redgraph.types import (
    Document,
    ID,
    Connection,
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
            val = extract(entity, index)
        except MissingFieldError as e:
            continue
        indexed.append(handle("index", *index))
        indexed.append(serialize(val))
    logger.info(indexed)
    return indexed


async def _set(conn: Connection, id: ID, entity: Document, *indices: Key) -> None:
    if type is not None:
        indexed = _extract_all(entity, *indices)
    else:
        indexed = []

    await conn.hmset(
        id.bytes, handle("entity"), serialize(entity), *indexed,
    )


async def create(conn: Connection, entity: Document, *indices: Key) -> ID:
    id = uuid.uuid1()
    await _set(conn, id, entity, *indices)
    return id


async def read(conn: Connection, id: ID) -> Value:
    raw = await conn.hget(id.bytes, handle("entity"), encoding="utf-8")
    if raw is not None:
        return deserialize(raw)
    return None


async def replace(conn: Connection, id: ID, entity: Document, *indices: Key) -> None:
    await delete(conn, id)
    await _set(conn, id, entity, type)


async def update(conn: Connection, id: ID, entity: Document, *indices: Key) -> None:
    await _set(conn, id, entity, type)


async def delete(conn: Connection, id: ID) -> None:
    await conn.delete(id.bytes)
