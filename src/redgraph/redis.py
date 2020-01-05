import json
import uuid
import asyncio
from contextlib import asynccontextmanager
from typing import Union, List, Mapping

import aioredis

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]
Key = Union[str, bytes]
Value = Union[List["Value"], "Document", str, bytes, int, float, bool, None]
Document = Mapping[Key, Value]


@asynccontextmanager
async def redis(url: str, port: int = 6379) -> Connection:
    redis = await aioredis.create_redis_pool((url, port))
    try:
        yield redis
    finally:
        redis.close()
        await redis.wait_closed()


@asynccontextmanager
async def transaction(redis: Connection) -> Transaction:
    tr = redis.multi_exec()
    yield tr
    await tr.execute()


def _entity(doc: Document) -> List[bytes]:
    return [
        bytes(str(p), "utf-8")
        for field, value in doc.items()
        for p in [field, json.dumps(value)]
    ]


async def create(redis: Connection, entity: Document) -> bytes:
    id = uuid.uuid1()

    await redis.hmset(id.bytes, *_entity(entity))

    return id


def _key(*keys: bytes) -> bytes:
    return b":".join(keys)


async def relate(
    redis: Connection, subject: uuid.UUID, predicate: uuid.UUID, object: uuid.UUID
) -> None:
    async with transaction(redis) as tr:
        futures = [
            tr.sadd(_key(b"sp", subject.bytes), predicate.bytes),
            tr.sadd(_key(b"ps", predicate.bytes), subject.bytes),
            tr.sadd(_key(b"spo", subject.bytes, predicate.bytes), object.bytes),
            tr.sadd(_key(b"so", subject.bytes), object.bytes),
            tr.sadd(_key(b"os", object.bytes), subject.bytes),
            tr.sadd(_key(b"sop", subject.bytes, object.bytes), predicate.bytes),
            tr.sadd(_key(b"po", predicate.bytes), object.bytes),
            tr.sadd(_key(b"op", object.bytes), predicate.bytes),
            tr.sadd(_key(b"pos", predicate.bytes, object.bytes), subject.bytes),
        ]
    await asyncio.gather(*futures)
