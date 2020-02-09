import uuid
import asyncio
from typing import Union, List, Mapping, Tuple
from dataclasses import dataclass

import aioredis

Handle = bytes
ID = Union[uuid.UUID]

Connection = Union[aioredis.ConnectionsPool, aioredis.RedisConnection]
Transaction = Union[aioredis.commands.MultiExec]
Future = asyncio.Future

Redis = Union[Connection, Transaction]
