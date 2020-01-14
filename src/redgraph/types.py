import uuid
from typing import Union, List, Mapping, Tuple
from dataclasses import dataclass

import aioredis

Handle = bytes
ID = Union[uuid.UUID]

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]
Redis = Union[Connection, Transaction]

Key = Union[str, bytes, int]
Primitive = Union[str, bytes, int, float, bool, None]
Document = Mapping[Key, "Value"]
Value = Union[Document, List[Union[Document, Primitive]], Primitive]
Index = List[Key]


@dataclass
class Type:
    name: str
    indices: List[Index]
