import json
import uuid
import logging
from functools import singledispatch
from typing import Optional

from redgraph import redis, anchors
from redgraph.common import handle, serialize, deserialize
from redgraph.types import (
    Document,
    Type,
    ID,
    Connection,
    List,
    Field,
    Value,
)

logger = logging.getLogger("redgraph")


def _extract_all(entity: Document, type: Type, type_check: bool) -> List[bytes]:
    indexed = []
    for index in type.indices:
        try:
            val = _extract(entity, index)
        except BadTypeError as e:
            if type_check:
                raise e
        else:
            indexed.append(handle("index", *index))
            indexed.append(serialize(val))
    return indexed


async def _set(
    conn: Connection,
    id: ID,
    entity: Document,
    type: Optional[Type] = None,
    type_check: bool = True,
) -> None:
    if type is not None:
        indexed = _extract_all(entity, type, type_check)
    else:
        indexed = []

    await conn.hmset(
        id.bytes,
        handle("entity"),
        serialize(entity),
        handle("type", "name"),
        serialize(type.name if type is not None else None),
        *indexed,
    )


async def create(conn: Connection, entity: Document, type: Optional[Type] = None) -> ID:
    id = uuid.uuid1()
    await _set(conn, id, entity, type)
    return id


async def read(conn: Connection, id: ID, index: Optional[Field] = None) -> Value:
    if index is None:
        raw = await conn.hget(id.bytes, handle("entity"), encoding="utf-8")
    else:
        raw = await conn.hget(id.bytes, handle("index", *index), encoding="utf-8")
    if raw is not None:
        return deserialize(raw)
    return None


async def replace(
    conn: Connection, id: ID, entity: Document, type: Optional[Type] = None
) -> None:
    await delete(conn, id)
    await _set(conn, id, entity, type)


async def update(
    conn: Connection, id: ID, entity: Document, type: Optional[Type] = None
) -> None:
    await _set(conn, id, entity, type)


async def delete(conn: Connection, id: ID) -> None:
    await conn.delete(id.bytes)
