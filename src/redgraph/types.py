import uuid
from typing import Union, List, Mapping
from functools import singledispatch

import aioredis

ID = Union[uuid.UUID]
Handle = bytes

Key = Union[str, bytes]
Value = Union[List["Value"], "Document", str, bytes, int, float, bool, None]
Document = Mapping[Key, Value]

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]


@singledispatch
def handle(*parts) -> Handle:
    return b":".join([bytes(str(part), "utf-8") for part in parts])


@handle.register
def _(*parts: bytes) -> Handle:
    return b":".join(parts)


@handle.register
def _(*parts: str) -> Handle:
    return b":".join([bytes(part, "utf-8") for part in parts])
