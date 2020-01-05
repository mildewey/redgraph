import json
import uuid
from typing import Union, List, Mapping

from redgraph import redis

Key = Union[str, bytes]
Value = Union[List["Value"], "Document", str, bytes, int, float, bool, None]
Document = Mapping[Key, Value]


def _entity_list(doc: Document) -> List[bytes]:
    return [
        bytes(str(p), "utf-8")
        for field, value in doc.items()
        for p in [field, json.dumps(value)]
    ]


async def create(redis: redis.Connection, entity: Document) -> bytes:
    id = uuid.uuid1()

    await redis.hmset(id.bytes, *_entity_list(entity))

    return id
