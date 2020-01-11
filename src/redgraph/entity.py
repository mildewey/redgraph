import json
import uuid
from typing import List

from redgraph import redis
from redgraph.types import Document, ID


def _entity_list(doc: Document) -> List[bytes]:
    return [
        bytes(str(p), "utf-8")
        for field, value in doc.items()
        for p in [field, json.dumps(value)]
    ]


async def create(conn: redis.Connection, entity: Document) -> ID:
    id = uuid.uuid1()

    await conn.hmset(id.bytes, *_entity_list(entity))

    return id
