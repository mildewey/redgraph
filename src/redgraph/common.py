import uuid
import json
import logging
from functools import singledispatch, reduce
from typing import Union, List, Mapping, Optional

import aioredis

from redgraph.types import Handle, Value


logger = logging.getLogger("redgraph")


def handle(*parts) -> Handle:
    return b":".join([bytes(str(part), "utf-8") for part in parts])


def serialize(val: Value) -> bytes:
    return bytes(json.dumps(val), "utf-8")


def deserialize(val: Optional[str]) -> Value:
    if val is not None:
        return json.loads(val)
    return val
