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
            val = extract(entity, *index)
        except MissingFieldError as e:
            continue
        indexed.append(handle("index", *index))
        indexed.append(serialize(val))
    return indexed


async def update(conn: Connection, id: ID, entity: Document, *indices: Key) -> None:
    indexed = _extract_all(entity, *indices)

    await conn.hmset(
        id.bytes, handle("entity"), serialize(entity), *indexed,
    )


async def create(conn: Connection, entity: Document, *indices: Key) -> ID:
    id = uuid.uuid1()
    await update(conn, id, entity, *indices)
    return id


async def read(conn: Connection, id: ID) -> Value:
    raw = await conn.hget(id.bytes, handle("entity"), encoding="utf-8")
    if raw is not None:
        return deserialize(raw)
    return None


async def replace(conn: Connection, id: ID, entity: Document, *indices: Key) -> None:
    await delete(conn, id)
    await update(conn, id, entity, *indices)


async def delete(conn: Connection, id: ID) -> None:
    await conn.delete(id.bytes)


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
