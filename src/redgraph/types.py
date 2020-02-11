import uuid
import asyncio
from typing import Union, List, Mapping, Tuple
from dataclasses import dataclass

import aioredis

Handle = bytes
ID = Union[uuid.UUID]

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]
Pipeline = Union[aioredis.commands.Pipeline]
Future = asyncio.Future

Redis = Union[Connection, Transaction]

Key = Union[str, int]
Primitive = Union[str, int, float, bool, None]
Document = Mapping[Key, "Value"]
Value = Union[Document, List[Union[Document, Primitive]], Primitive]
